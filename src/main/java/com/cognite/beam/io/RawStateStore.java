/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.beam.io;

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.read.AddPartitionsFn;
import com.cognite.beam.io.fn.read.ListEventsFn;
import com.cognite.beam.io.fn.read.ReadAggregatesFn;
import com.cognite.beam.io.fn.read.RetrieveEventsFn;
import com.cognite.beam.io.fn.request.GenerateReadRequestsUnboundFn;
import com.cognite.beam.io.fn.statestore.RawStateStoreDeleteStateFn;
import com.cognite.beam.io.fn.statestore.RawStateStoreExpandHighFn;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.beam.io.CogniteIO.invalidProjectConfigFile;

public abstract class RawStateStore {
    private static final int MAX_WRITE_BATCH_SIZE = 1000;

    /**
     * Transform that will read a collection of {@link Event} objects from Cognite Data Fusion.
     *
     * You specify which {@link Event} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class Read extends ConnectorBase<PBegin, PCollection<Event>> {

        public static Builder builder() {
            return new AutoValue_RawStateStore_Read.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();

        public abstract Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public Read withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public Read withRequestParameters(RequestParameters params) {
            return toBuilder().setRequestParameters(params).build();
        }

        @Override
        public PCollection<Event> expand(PBegin input) {

            PCollection<Event> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllEvents()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                    );

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setRequestParameters(RequestParameters value);
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);

            public abstract Read build();
        }
    }

    /**
     * Transform that will read a collection of {@link Event} objects from Cognite Data Fusion.
     *
     * You specify which {@link Event} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link Event} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAll
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<Event>> {

        public static Builder builder() {
            return new AutoValue_RawStateStore_ReadAll.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public ReadAll withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        @Override
        public PCollection<Event> expand(PCollection<RequestParameters> input) {
            PCollection<Event> outputCollection = input
                    .apply("Read direct", CogniteIO.readAllDirectEvents()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig())
                            .withHints(getHints()))
                    .apply("Unwrap events", ParDo.of(new DoFn<List<Event>, Event>() {
                        @ProcessElement
                        public void processElement(@Element List<Event> element,
                                                   OutputReceiver<Event> out) {
                            if (getReaderConfig().isStreamingEnabled()) {
                                // output with timestamp
                                element.forEach(row -> out.outputWithTimestamp(row,
                                        new org.joda.time.Instant(row.getLastUpdatedTime())));
                            } else {
                                // output without timestamp
                                element.forEach(row -> out.output(row));
                            }
                        }
                    }))
                    ;

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);
            public abstract ReadAll build();
        }
    }

    /**
     * Transform that will read a collection of {@link Event} objects from Cognite Data Fusion. The
     * {@link Event} result objects are returned in batches ({@code List<Event>}) of size <10k.
     *
     * You specify which {@link Event} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link Event} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllDirect
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<List<Event>>> {

        public static Builder builder() {
            return new AutoValue_RawStateStore_ReadAllDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public ReadAllDirect withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        @Override
        public PCollection<List<Event>> expand(PCollection<RequestParameters> input) {
            Preconditions.checkState(!(getReaderConfig().isStreamingEnabled() && getReaderConfig().isDeltaEnabled()),
                    "Using delta read in combination with streaming is not supported.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // conditional streaming
            PCollection<RequestParameters> requestParametersPCollection;

            if (getReaderConfig().isStreamingEnabled()) {
                // streaming mode
                LOG.info("Setting up streaming mode");
                requestParametersPCollection = input
                        .apply("Watch for new items", ParDo.of(
                                new GenerateReadRequestsUnboundFn(getReaderConfig(), ResourceType.EVENT)));
            } else {
                // batch mode
                LOG.info("Setting up batch mode");
                requestParametersPCollection = input;
            }

            PCollection<List<Event>> outputCollection = requestParametersPCollection
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("Apply delta timestamp", ApplyDeltaTimestamp.to(ResourceType.EVENT)
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Add partitions", ParDo.of(new AddPartitionsFn(getHints(),
                                    getReaderConfig().enableMetrics(false), ResourceType.EVENT,
                                    projectConfigView))
                            .withSideInputs(projectConfigView))
                    .apply("Break fusion", BreakFusion.<RequestParameters>create())
                    .apply("Read results", ParDo.of(new ListEventsFn(getHints(), getReaderConfig(),projectConfigView))
                            .withSideInputs(projectConfigView));

            // Record delta timestamp
            outputCollection
                    .apply("Extract last change timestamp", MapElements.into(TypeDescriptors.longs())
                            .via((List<Event> batch) -> batch.stream()
                                    .mapToLong(Event::getLastUpdatedTime)
                                    .max()
                                    .orElse(1L))
                    )
                    .apply("Record delta timestamp", RecordDeltaTimestamp.create()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);
            public abstract ReadAllDirect build();
        }
    }

    /**
     * Transform that will read a collection of {@link Event} objects from Cognite Data Fusion.
     *
     * You specify which {@link Event} objects to read via a set of ids enclosed in
     * {@link Item} objects. This transform takes a collection of {@link Item}
     * as input and returns all {@link Event} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllById
            extends ConnectorBase<PCollection<Item>, PCollection<Event>> {

        public static RawStateStore.ReadAllById.Builder builder() {
            return new AutoValue_RawStateStore_ReadAllById.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract RawStateStore.ReadAllById.Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public RawStateStore.ReadAllById withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        @Override
        public PCollection<Event> expand(PCollection<Item> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<Event> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(4000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Read results", ParDo.of(
                            new RetrieveEventsFn(getHints(), getReaderConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<RawStateStore.ReadAllById.Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);
            public abstract ReadAllById build();
        }
    }

    /**
     * Transform that will read aggregate/summary statistics related to {@link Event} objects in
     * Cognite Data Fusion.
     *
     * You specify the parameters of the aggregate(s) via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class ReadAggregate extends ConnectorBase<PBegin, PCollection<Aggregate>> {

        public static Builder builder() {
            return new AutoValue_RawStateStore_ReadAggregate.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();

        public abstract Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public ReadAggregate withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAggregate withRequestParameters(RequestParameters params) {
            return toBuilder().setRequestParameters(params).build();
        }

        @Override
        public PCollection<Aggregate> expand(PBegin input) {
            LOG.debug("Building read events composite transform.");

            PCollection<Aggregate> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllAggregatesEvents()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                    );

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setRequestParameters(RequestParameters value);
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);

            public abstract ReadAggregate build();
        }
    }

    /**
     * Transform that will read aggregate/summary statistics related to {@link Event} objects in
     * Cognite Data Fusion.
     *
     * You specify the parameters of the aggregate(s) via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link Aggregate} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllAggregate
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<Aggregate>> {

        public static Builder builder() {
            return new AutoValue_RawStateStore_ReadAllAggregate.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public ReadAllAggregate withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        @Override
        public PCollection<Aggregate> expand(PCollection<RequestParameters> input) {
            LOG.debug("Building read all events aggregates composite transform.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<Aggregate> outputCollection = input
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("Break fusion", BreakFusion.<RequestParameters>create())
                    .apply("Read results", ParDo.of(
                            new ReadAggregatesFn(getHints(), getReaderConfig(),
                                    projectConfigView, ResourceType.EVENT))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);
            public abstract ReadAllAggregate build();
        }
    }

    /**
     * Transform that will write {@link Event} objects to Cognite Data Fusion.
     * <p>
     * The input objects will be batched and upserted. If the {@link Event} object
     * does not exist, it will be created as a new object. In case the {@link Event} already
     * exists, it will be updated with the new input.
     */
    @AutoValue
    public abstract static class ExpandHigh
            extends ConnectorBase<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {
        private static final int MAX_WRITE_BATCH_SIZE = 1000;

        public static Builder builder() {
            return new AutoValue_RawStateStore_Write.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public ExpandHigh withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<KV<String, Long>> input) {
            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<Long> longCoder = VarLongCoder.of();
            KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, longCoder);

            // main input
            PCollection<KV<String, Long>> outputCollection = input
                    .apply("Batch items", GroupIntoBatches.<String, Long>of(keyValueCoder)
                            .withMaxBatchSize(MAX_WRITE_BATCH_SIZE)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Expand high", CogniteIO.writeDirectEvents()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(getWriterConfig())
                            .withHints(getHints()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);

            public abstract ExpandHigh build();
        }
    }

    /**
     * Expands the high watermark state to a RAW state store. It takes a {@code KV<String, Long>} representing
     * the key and high watermark value as input and outputs the same element after it has been committed to the
     * state store.
     *
     * Expand high will only set a new value it the supplied value is higher than the current state for the key.
     */
    @AutoValue
    public abstract static class ExpandHighDirect
            extends ConnectorBase<PCollection<Iterable<KV<String, Long>>>, PCollection<KV<String, Long>>> {

        public static Builder builder() {
            return new AutoValue_RawStateStore_WriteDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public ExpandHighDirect withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public ExpandHighDirect withWriterConfig(WriterConfig config) {
            return toBuilder().setWriterConfig(config).build();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<Iterable<KV<String, Long>>> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<KV<String, Long>> outputCollection = input
                    .apply("Expand high direct", ParDo.of(
                                    new RawStateStoreExpandHighFn(getHints(),
                                            getWriterConfig(),
                                            getDbName(),
                                            getTableName(),
                                            projectConfigView) {
                                    })
                            .withSideInputs(projectConfigView))
                    .apply("Unwrap", ParDo.of(new DoFn<List<KV<String, Long>>, KV<String, Long>>() {
                        @ProcessElement
                        public void processElement(@Element List<KV<String, Long>> element,
                                                   OutputReceiver<KV<String, Long>> out) {
                            // output without timestamp
                            element.forEach(row -> out.output(row));
                        }
                    }));


            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);

            public abstract ExpandHighDirect build();
        }
    }

    /**
     * Transform that will delete a state from the Raw state store. It takes the state key to delete as input and
     * outputs the same key after the delete has been committed.
     */
    @AutoValue
    public abstract static class DeleteState
            extends ConnectorBase<PCollection<Item>, PCollection<Item>> {
        public static Builder builder() {
            return new AutoValue_RawStateStore_DeleteState.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();
        public abstract String getDbName();
        public abstract String getTableName();

        public DeleteState withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public DeleteState withWriterConfig(WriterConfig config) {
            return toBuilder().setWriterConfig(config).build();
        }

        public DeleteState withDbName(String dbName) {
            return toBuilder().setDbName(dbName).build();
        }

        public DeleteState withTableName(String tableName) {
            return toBuilder().setTableName(tableName).build();
        }

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            Coder<String> utf8Coder = StringUtf8Coder.of();
            KvCoder<String, String> keyValueCoder = KvCoder.of(utf8Coder, utf8Coder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<String> outputCollection = input
                    .apply("Shard items", WithKeys.of((String inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, String>of(keyValueCoder)
                            .withMaxBatchSize(MAX_WRITE_BATCH_SIZE)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<String>>create())
                    .apply("Delete items", ParDo.of(
                                    new RawStateStoreDeleteStateFn(getHints(),
                                                                    getWriterConfig(),
                                                                    getDbName(),
                                                                    getTableName(),
                                                                    projectConfigView) {
                                    })
                            .withSideInputs(projectConfigView))
                    .apply("Unwrap", ParDo.of(new DoFn<List<String>, String>() {
                        @ProcessElement
                        public void processElement(@Element List<String> element,
                                                   OutputReceiver<String> out) {
                                // output without timestamp
                                element.forEach(row -> out.output(row));
                        }
                    }));


            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);
            public abstract DeleteState build();
        }
    }
}
