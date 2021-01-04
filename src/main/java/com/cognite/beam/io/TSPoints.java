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

import com.cognite.beam.io.config.*;
import com.cognite.client.dto.TimeseriesPoint;
import com.cognite.client.dto.TimeseriesPointPost;
import com.cognite.beam.io.fn.ResourceType;
import com.cognite.beam.io.fn.parse.ParseTimeseriesPointFn;
import com.cognite.beam.io.fn.read.ReadItemsIteratorFn;
import com.cognite.beam.io.fn.read.ReadTsPointProto;
import com.cognite.beam.io.fn.read.ReadTsPointProtoSdf;
import com.cognite.beam.io.fn.request.GenerateReadRequestsUnboundFn;
import com.cognite.beam.io.fn.write.UpsertTsPointsProtoFn;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.internal.*;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import com.google.auto.value.AutoValue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.beam.io.CogniteIO.*;

public abstract class TSPoints {

    @AutoValue
    public abstract static class Read extends ConnectorBase<PBegin, PCollection<TimeseriesPoint>> {

        public static Read.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSPoints_Read.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setNewCodePath(false);
        }

        public abstract ReaderConfig getReaderConfig();
        public abstract RequestParameters getRequestParameters();
        public abstract boolean isNewCodePath();

        public abstract Read.Builder toBuilder();

        public Read withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public Read withRequestParameters(RequestParameters params) {
            return toBuilder().setRequestParameters(params).build();
        }

        public Read withHints(Hints hints) {
            return toBuilder().setHints(hints).build();
        }

        public Read withReaderConfig(ReaderConfig config) {
            return toBuilder().setReaderConfig(config).build();
        }

        public Read withProjectConfigFile(ValueProvider<String> filePath) {
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public Read withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public Read enableNewCodePath() {
            return toBuilder().setNewCodePath(true).build();
        }

        @Override
        public PCollection<TimeseriesPoint> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");

            PCollection<TimeseriesPoint> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllTimeseriesPoints()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withNewCodePath(isNewCodePath())
                    );


            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setRequestParameters(RequestParameters value);
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setNewCodePath(boolean value);

            public abstract Read build();
        }
    }

    @AutoValue
    public abstract static class ReadAll extends ConnectorBase<PCollection<RequestParameters>, PCollection<TimeseriesPoint>> {

        public static ReadAll.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSPoints_ReadAll.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setNewCodePath(false);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract ReadAll.Builder toBuilder();
        public abstract boolean isNewCodePath();

        public ReadAll withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAll withHints(Hints hints) {
            return toBuilder().setHints(hints).build();
        }

        public ReadAll withReaderConfig(ReaderConfig config) {
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadAll withProjectConfigFile(ValueProvider<String> filePath) {
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public ReadAll withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public ReadAll enableNewCodePath() {
            return toBuilder().setNewCodePath(true).build();
        }
        public ReadAll withNewCodePath(boolean enable) {
            return toBuilder().setNewCodePath(enable).build();
        }

        @Override
        public PCollection<TimeseriesPoint> expand(PCollection<RequestParameters> input) {
            LOG.info("Starting Cognite reader.");

            PCollection<TimeseriesPoint> outputCollection = input
                    .apply("Read TS points direct", CogniteIO.readAllDirectTimeseriesPoints()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig())
                            .withHints(getHints())
                            .withNewCodePath(isNewCodePath()))
                    .apply("Unwrap points", ParDo.of(new DoFn<Iterable<TimeseriesPoint>, TimeseriesPoint>() {
                        @ProcessElement
                        public void processElement(@Element Iterable<TimeseriesPoint> element,
                                                   OutputReceiver<TimeseriesPoint> out) {
                            if (getReaderConfig().isStreamingEnabled()) {
                                // output with timestamp
                                element.forEach(point -> out.outputWithTimestamp(point,
                                        org.joda.time.Instant.ofEpochMilli(point.getTimestamp())));
                            } else {
                                // output without timestamp
                                element.forEach(point -> out.output(point));
                            }
                        }
                    }));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setNewCodePath(boolean value);
            public abstract ReadAll build();
        }
    }

    @AutoValue
    public abstract static class ReadAllDirect
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<Iterable<TimeseriesPoint>>> {

        public static ReadAllDirect.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSPoints_ReadAllDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setNewCodePath(false);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract ReadAllDirect.Builder toBuilder();
        public abstract boolean isNewCodePath();

        public ReadAllDirect withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAllDirect withHints(Hints hints) {
            return toBuilder().setHints(hints).build();
        }

        public ReadAllDirect withReaderConfig(ReaderConfig config) {
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadAllDirect withProjectConfigFile(ValueProvider<String> filePath) {
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public ReadAllDirect withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public ReadAllDirect enableNewCodePath() {
            return toBuilder().setNewCodePath(true).build();
        }
        public ReadAllDirect withNewCodePath(boolean enable) {
            return toBuilder().setNewCodePath(enable).build();
        }

        @Override
        public PCollection<Iterable<TimeseriesPoint>> expand(PCollection<RequestParameters> input) {
            LOG.info("Starting Cognite datapoints reader.");

            Preconditions.checkState(!(getReaderConfig().isStreamingEnabled() && getReaderConfig().isDeltaEnabled()),
                    "Using delta read in combination with streaming is not supported.");

            // main input
            PCollection<RequestParameters> requestParametersPCollection;

            if (getReaderConfig().isStreamingEnabled()) {
                // streaming mode
                LOG.info("Setting up streaming mode");
                requestParametersPCollection = input
                        .apply("Watch for new items", ParDo.of(
                                new GenerateReadRequestsUnboundFn(getReaderConfig(), ResourceType.TIMESERIES_DATAPOINTS)));
            } else {
                // batch mode
                LOG.info("Setting up batch mode");
                requestParametersPCollection = input;
            }

            PCollection<RequestParameters> requestParametersWithConfig = requestParametersPCollection
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withReaderConfig(getReaderConfig()));

            PCollection<Iterable<TimeseriesPoint>> outputCollection;

            if (isNewCodePath()) {
                LOG.info("Using new codepath with sdf reader");
                outputCollection = requestParametersWithConfig
                        .apply("Read results", ParDo.of(new ReadTsPointProtoSdf(getHints(), getReaderConfig())));
            } else {
                LOG.info("Using old codepath for reader");
                outputCollection = requestParametersWithConfig
                        .apply("Read results", ParDo.of(new ReadTsPointProto(getHints(), getReaderConfig())));
            }

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setNewCodePath(boolean value);
            public abstract ReadAllDirect build();
        }
    }

    @AutoValue
    public abstract static class ReadAllJson
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<TimeseriesPoint>> {

        public static ReadAllJson.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSPoints_ReadAllJson.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract ReadAllJson.Builder toBuilder();

        public ReadAllJson withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAllJson withHints(Hints hints) {
            return toBuilder().setHints(hints).build();
        }

        public ReadAllJson withReaderConfig(ReaderConfig config) {
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadAllJson withProjectConfigFile(ValueProvider<String> filePath) {
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public ReadAllJson withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        @Override
        public PCollection<TimeseriesPoint> expand(PCollection<RequestParameters> input) {
            LOG.info("Starting Cognite reader.");

            PCollection<TimeseriesPoint> outputCollection = input
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Read results", ParDo.of(new ReadItemsIteratorFn(getHints(), ResourceType.TIMESERIES_DATAPOINTS,
                                    getReaderConfig())))
                    .apply("Parse results", ParDo.of(new ParseTimeseriesPointFn()))
                    .apply("Break fusion", BreakFusion.<TimeseriesPoint>create());

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAllJson build();
        }
    }

    /**
     * Writes time series points to the Cognite API.
     *
     * The writer will sort and batch the data points for optimized writes. In batch jobs, all data points are
     * sorted and batched before the writes start, while in streaming mode the data is optimized in 10 second
     * batches (configurable).
     */
    @AutoValue
    public abstract static class Write
            extends ConnectorBase<PCollection<TimeseriesPointPost>, PCollection<TimeseriesPointPost>> {

        public static TSPoints.Write.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSPoints_Write.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract TSPoints.Write.Builder toBuilder();

        public TSPoints.Write withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public TSPoints.Write withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public TSPoints.Write withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public TSPoints.Write withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public TSPoints.Write withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        @Override
        public PCollection<TimeseriesPointPost> expand(PCollection<TimeseriesPointPost> input) {
            LOG.info("Starting Cognite writer.");

            LOG.debug("Building upsert time series points composite transform.");
            final Coder<String> utf8Coder = StringUtf8Coder.of();
            final Coder<TimeseriesPointPost> tsPointsCoder = ProtoCoder.of(TimeseriesPointPost.class);
            final KvCoder<String, TimeseriesPointPost> keyValueCoder = KvCoder.of(utf8Coder, tsPointsCoder);
            final String delimiter = "__§delimiter§__";

            // main input
            PCollectionList<KV<String, Iterable<TimeseriesPointPost>>> tsPointBatches = input
                    .apply("Verify id", MapElements.into(TypeDescriptor.of(TimeseriesPointPost.class))
                            .via((TimeseriesPointPost inputItem) -> {
                                Preconditions.checkArgument(
                                        inputItem.getIdTypeCase() != TimeseriesPointPost.IdTypeCase.IDTYPE_NOT_SET,
                                        "TS point input does not contain externalId nor id. "
                                        + "TS point timestamp: " + inputItem.getTimestamp());
                                return inputItem;
                            }))
                    .apply("Shard by TS", WithKeys.of((TimeseriesPointPost inputItem) -> {
                            // Group on ts id and daily timestamp.
                            String tsId = inputItem.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID ?
                                    inputItem.getExternalId() : Long.toString(inputItem.getId());
                            String key = tsId + delimiter + Instant.ofEpochMilli(inputItem.getTimestamp()).truncatedTo(ChronoUnit.DAYS).toString();
                            return key;
                    }
                    )).setCoder(keyValueCoder)
                    .apply("Batch by TS", GroupIntoBatchesDatapoints.builder()
                            .setMaxLatency(getHints().getWriteMaxBatchLatency().dividedBy(2))
                            .build())
                    .apply("Check batch size", Partition.of(2, new Partition.PartitionFn<KV<String, Iterable<TimeseriesPointPost>>>() {
                        @Override
                        public int partitionFor(KV<String, Iterable<TimeseriesPointPost>> elem, int numPartitions) {
                            int counter = 0;
                            for (TimeseriesPointPost point : elem.getValue()) {
                                counter++;
                            }
                            if (counter >= 50000) {
                                return 0; // no additional batching
                            } else {
                                return 1; // run through additional batching
                            }
                        }
                    }));

            // Sub-path for additional batching
            PCollection<KV<String, Iterable<TimeseriesPointPost>>> tsPointsSecondBatchLevel = tsPointBatches.get(1)
                    .apply("Shard by day", MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                                    TypeDescriptors.iterables(TypeDescriptor.of(TimeseriesPointPost.class))))
                            .via((KV<String, Iterable<TimeseriesPointPost>> inputElements) ->
                                // Group on daily timestamp + no. shards.
                                KV.of(inputElements.getKey().split(delimiter)[1] + delimiter
                                        + String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards())),
                                        inputElements.getValue())))
                    .apply("Batch by day", GroupIntoBatchesDatapointsCollections.builder()
                            .setMaxLatency(getHints().getWriteMaxBatchLatency().dividedBy(2))
                            .build());

            // Merge the two batch levels together and write
            PCollectionList<KV<String, Iterable<TimeseriesPointPost>>> tsBothBatchLevels =
                    PCollectionList.of(tsPointBatches.get(0)).and(tsPointsSecondBatchLevel);
            PCollection<TimeseriesPointPost> outputCollection = tsBothBatchLevels
                    .apply("Flatten", Flatten.pCollections())
                    .apply("Remove key", Values.<Iterable<TimeseriesPointPost>>create())
                    .apply("Write data points", CogniteIO.writeDirectTimeseriesPoints()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(getWriterConfig())
                            .withHints(getHints()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<TSPoints.Write.Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract TSPoints.Write build();
        }
    }

    /**
     * Writes TS datapoints directly to the Cognite API, bypassing the regular validation and optimization steps. This
     * writer is designed for advanced use with extreme data volumes (100+ billion items). Most use cases should
     * use the regular {@link TSPoints.Write} writer which will perform shuffling and batching to optimize
     * the write performance.
     *
     * This writer will push each input {@link Iterable<TimeseriesPointPost>} as a single batch. If the input
     * violates any constraints, the write will fail. Also, the performance of the writer depends heavily on the
     * input being sorted and batched as optimally as possible.
     *
     * If your source system offers data pre-sorted, you may get additional performance from this writer as
     * it bypasses the regular shuffle and batch steps. For example, if you use {@link TSPoints.ReadAllDirect} to
     * read batches of data points directly from Cognite then you can feed these batches to this writer as they are
     * pre-sorted.
     */
    @AutoValue
    public abstract static class WriteDirect
            extends ConnectorBase<PCollection<Iterable<TimeseriesPointPost>>, PCollection<TimeseriesPointPost>> {

        public static TSPoints.WriteDirect.Builder builder() {
            return new com.cognite.beam.io.AutoValue_TSPoints_WriteDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract TSPoints.WriteDirect.Builder toBuilder();

        public TSPoints.WriteDirect withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public TSPoints.WriteDirect withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public TSPoints.WriteDirect withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public TSPoints.WriteDirect withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public TSPoints.WriteDirect withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        @Override
        public PCollection<TimeseriesPointPost> expand(PCollection<Iterable<TimeseriesPointPost>> input) {
            LOG.info("Starting Cognite writer.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getWriterConfig().getAppIdentifier())
                            .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<TimeseriesPointPost> outputCollection = input
                    .apply("Write data points", ParDo.of(
                            new UpsertTsPointsProtoFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<TSPoints.WriteDirect.Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract TSPoints.WriteDirect build();
        }
    }
}
