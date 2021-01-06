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

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Item;
import com.cognite.client.config.ResourceType;
import com.cognite.beam.io.fn.delete.DeleteItemsFn;
import com.cognite.beam.io.fn.parse.ParseAggregateFn;
import com.cognite.beam.io.fn.read.AddPartitionsFn;
import com.cognite.beam.io.fn.read.ReadItemsByIdFn;
import com.cognite.beam.io.fn.read.ReadItemsFn;
import com.cognite.beam.io.fn.read.ReadItemsIteratorFn;
import com.cognite.beam.io.fn.request.GenerateReadRequestsUnboundFn;
import com.cognite.beam.io.fn.write.UpsertTsHeaderFn;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import com.google.common.base.Preconditions;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import com.cognite.client.dto.TimeseriesMetadata;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.fn.parse.ParseTimeseriesMetaFn;
import com.google.auto.value.AutoValue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.beam.io.CogniteIO.*;

public abstract class TSMetadata {

    /**
     * Transform that will read a collection of {@link TimeseriesMetadata} objects from Cognite Data Fusion.
     *
     * You specify which {@link TimeseriesMetadata} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class Read extends ConnectorBase<PBegin, PCollection<TimeseriesMetadata>> {

        public static Read.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSMetadata_Read.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        abstract RequestParameters getRequestParameters();

        abstract Read.Builder toBuilder();

        public Read withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Read withRequestParameters(RequestParameters params) {
            Preconditions.checkNotNull(params, "Parameters cannot be null.");
            return toBuilder().setRequestParameters(params).build();
        }

        public Read withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Read withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public Read withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public Read withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        @Override
        public PCollection<TimeseriesMetadata> expand(PBegin input) {

            PCollection<TimeseriesMetadata> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllTimeseriesMetadata()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Read.Builder setRequestParameters(RequestParameters value);

            public abstract Read build();
        }
    }

    /**
     * Transform that will read a collection of {@link TimeseriesMetadata} objects from Cognite Data Fusion.
     *
     * You specify which {@link TimeseriesMetadata} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link TimeseriesMetadata} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAll
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<TimeseriesMetadata>> {

        public static ReadAll.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSMetadata_ReadAll.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        abstract ReadAll.Builder toBuilder();

        public ReadAll withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAll withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public ReadAll withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadAll withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public ReadAll withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        @Override
        public PCollection<TimeseriesMetadata> expand(PCollection<RequestParameters> input) {
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building read time series metadata composite transform.");

            Preconditions.checkState(!(getReaderConfig().isStreamingEnabled() && getReaderConfig().isDeltaEnabled()),
                    "Using delta read in combination with streaming is not supported.");

            // main input
            PCollection<RequestParameters> requestParametersPCollection;

            if (getReaderConfig().isStreamingEnabled()) {
                // streaming mode
                LOG.info("Setting up streaming mode");
                requestParametersPCollection = input
                        .apply("Watch for new items", ParDo.of(
                                new GenerateReadRequestsUnboundFn(getReaderConfig(), ResourceType.TIMESERIES_HEADER)));
            } else {
                // batch mode
                LOG.info("Setting up batch mode");
                requestParametersPCollection = input;
            }

            PCollection<TimeseriesMetadata> outputCollection = requestParametersPCollection
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Apply delta timestamp", ApplyDeltaTimestamp.to(ResourceType.TIMESERIES_HEADER)
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Add partitions", ParDo.of(new AddPartitionsFn(getHints(), ResourceType.TIMESERIES_HEADER,
                            getReaderConfig().enableMetrics(false))))
                    .apply("Break fusion", BreakFusion.<RequestParameters>create())
                    .apply("Read results", ParDo.of(new ReadItemsIteratorFn(getHints(), ResourceType.TIMESERIES_HEADER,
                            getReaderConfig())))
                    .apply("Parse results", ParDo.of(new ParseTimeseriesMetaFn()));

            // Record delta timestamp
            outputCollection
                    .apply("Extract last change timestamp", MapElements.into(TypeDescriptors.longs())
                            .via((TimeseriesMetadata tsMeta) -> tsMeta.getLastUpdatedTime().getValue()))
                    .apply("Record delta timestamp", RecordDeltaTimestamp.create()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAll build();
        }
    }

    /**
     * Transform that will read a collection of {@link TimeseriesMetadata} objects from Cognite Data Fusion.
     *
     * You specify which {@link TimeseriesMetadata} objects to read via a set of ids enclosed in
     * {@link Item} objects. This transform takes a collection of {@link Item}
     * as input and returns all {@link TimeseriesMetadata} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllById
            extends ConnectorBase<PCollection<Item>, PCollection<TimeseriesMetadata>> {

        public static TSMetadata.ReadAllById.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSMetadata_ReadAllById.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract TSMetadata.ReadAllById.Builder toBuilder();

        public TSMetadata.ReadAllById withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public TSMetadata.ReadAllById withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public TSMetadata.ReadAllById withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public TSMetadata.ReadAllById withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public TSMetadata.ReadAllById withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<TimeseriesMetadata> expand(PCollection<Item> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getReaderConfig().getAppIdentifier())
                            .withSessionIdentifier(getReaderConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<TimeseriesMetadata> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(1000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Read results", ParDo.of(
                            new ReadItemsByIdFn(getHints(), ResourceType.TIMESERIES_BY_ID, getReaderConfig(),
                                    projectConfigView)).withSideInputs(projectConfigView))
                    .apply("Parse results", ParDo.of(new ParseTimeseriesMetaFn()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<TSMetadata.ReadAllById.Builder> {
            public abstract TSMetadata.ReadAllById.Builder setReaderConfig(ReaderConfig value);
            public abstract TSMetadata.ReadAllById build();
        }
    }

    /**
     * Transform that will read aggregate/summary statistics related to {@link TimeseriesMetadata}
     * objects in Cognite Data Fusion.
     *
     * You specify the parameters of the aggregate(s) via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class ReadAggregate extends ConnectorBase<PBegin, PCollection<Aggregate>> {

        public static TSMetadata.ReadAggregate.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSMetadata_ReadAggregate.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();

        public abstract TSMetadata.ReadAggregate.Builder toBuilder();

        public TSMetadata.ReadAggregate withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public TSMetadata.ReadAggregate withRequestParameters(RequestParameters params) {
            Preconditions.checkNotNull(params, "Parameters cannot be null.");
            return toBuilder().setRequestParameters(params).build();
        }

        public TSMetadata.ReadAggregate withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public TSMetadata.ReadAggregate withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public TSMetadata.ReadAggregate withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public TSMetadata.ReadAggregate withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Aggregate> expand(PBegin input) {

            PCollection<Aggregate> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllAggregatesTimeseriesMetadata()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                    );

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<TSMetadata.ReadAggregate.Builder> {
            public abstract TSMetadata.ReadAggregate.Builder setRequestParameters(RequestParameters value);
            public abstract TSMetadata.ReadAggregate.Builder setReaderConfig(ReaderConfig value);

            public abstract TSMetadata.ReadAggregate build();
        }
    }

    /**
     * Transform that will read aggregate/summary statistics related to {@link TimeseriesMetadata} objects in
     * Cognite Data Fusion.
     *
     * You specify the parameters of the aggregate(s) via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link Aggregate} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllAggregate
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<Aggregate>> {

        public static TSMetadata.ReadAllAggregate.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSMetadata_ReadAllAggregate.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract TSMetadata.ReadAllAggregate.Builder toBuilder();

        public TSMetadata.ReadAllAggregate withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public TSMetadata.ReadAllAggregate withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public TSMetadata.ReadAllAggregate withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public TSMetadata.ReadAllAggregate withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public TSMetadata.ReadAllAggregate withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Aggregate> expand(PCollection<RequestParameters> input) {
            LOG.info("Starting Cognite reader.");

            PCollection<Aggregate> outputCollection = input
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Break fusion", BreakFusion.<RequestParameters>create())
                    .apply("Read results", ParDo.of(
                            new ReadItemsFn(getHints(), ResourceType.EVENT_AGGREGATES, getReaderConfig())))
                    .apply("Parse results", ParDo.of(new ParseAggregateFn()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<TSMetadata.ReadAllAggregate.Builder> {
            public abstract TSMetadata.ReadAllAggregate.Builder setReaderConfig(ReaderConfig value);
            public abstract TSMetadata.ReadAllAggregate build();
        }
    }

    /**
     * Transform that will write {@link TimeseriesMetadata} objects to Cognite Data Fusion.
     * <p>
     * The input objects will be batched and upserted. If the {@link TimeseriesMetadata} object
     * does not exist, it will be created as a new object. In case the {@link TimeseriesMetadata} already
     * exists, it will be updated with the new input.
     */
    @AutoValue
    public abstract static class Write
            extends ConnectorBase<PCollection<TimeseriesMetadata>, PCollection<TimeseriesMetadata>> {
        private static final int MAX_WRITE_BATCH_SIZE = 1000;

        public static TSMetadata.Write.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSMetadata_Write.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract TSMetadata.Write.Builder toBuilder();

        public TSMetadata.Write withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public TSMetadata.Write withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public TSMetadata.Write withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public TSMetadata.Write withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public TSMetadata.Write withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        @Override
        public PCollection<TimeseriesMetadata> expand(PCollection<TimeseriesMetadata> input) {
            LOG.info("Starting Cognite writer.");

            LOG.debug("Building upsert time series metadata composite transform.");
            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<TimeseriesMetadata> tsMetadataCoder = ProtoCoder.of(TimeseriesMetadata.class);
            KvCoder<String, TimeseriesMetadata> keyValueCoder = KvCoder.of(utf8Coder, tsMetadataCoder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getWriterConfig().getAppIdentifier())
                            .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<TimeseriesMetadata> outputCollection = input
                    .apply("Check id", MapElements.into(TypeDescriptor.of(TimeseriesMetadata.class))
                            .via((TimeseriesMetadata inputItem) -> {
                                if (inputItem.hasExternalId() || inputItem.hasId()) {
                                    return inputItem;
                                } else {
                                    return TimeseriesMetadata.newBuilder(inputItem)
                                            .setExternalId(StringValue.of(UUID.randomUUID().toString()))
                                            .build();
                                }
                            }))
                    .apply("Shard items", WithKeys.of((TimeseriesMetadata inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, TimeseriesMetadata>of(keyValueCoder)
                            .withMaxBatchSize(MAX_WRITE_BATCH_SIZE)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<TimeseriesMetadata>>create())
                    .apply("Upsert items", ParDo.of(
                            new UpsertTsHeaderFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView))
                    .apply("Parse results items", ParDo.of(new ParseTimeseriesMetaFn()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract TSMetadata.Write build();
        }
    }

    /**
     * Transform that will delete {@link TimeseriesMetadata} objects (and also the corresponding
     * {@link com.cognite.client.dto.TimeseriesPoint}) from Cognite Data Fusion.
     * <p>
     * The input to this transform is a collection of {@link Item} objects that identifies (via
     * id or externalId) which {@link TimeseriesMetadata} objects to delete.
     */
    @AutoValue
    public abstract static class Delete
            extends ConnectorBase<PCollection<Item>, PCollection<Item>> {

        public static TSMetadata.Delete.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSMetadata_Delete.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract TSMetadata.Delete.Builder toBuilder();

        public TSMetadata.Delete withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public TSMetadata.Delete withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public TSMetadata.Delete withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public TSMetadata.Delete withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public TSMetadata.Delete withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        @Override
        public PCollection<Item> expand(PCollection<Item> input) {
            LOG.info("Starting Cognite writer.");
            LOG.debug("Building delete events composite transform.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getWriterConfig().getAppIdentifier())
                            .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<Item> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(1000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Delete time series", ParDo.of(
                            new DeleteItemsFn(getHints(), ResourceType.TIMESERIES_HEADER,
                                    getWriterConfig().getAppIdentifier(), getWriterConfig().getSessionIdentifier(),
                                    projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<TSMetadata.Delete.Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract TSMetadata.Delete build();
        }
    }
}
