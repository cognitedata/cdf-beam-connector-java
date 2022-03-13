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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.read.*;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.client.dto.FileMetadata;
import com.cognite.client.dto.Item;
import com.cognite.client.config.ResourceType;
import com.cognite.beam.io.fn.request.GenerateReadRequestsUnboundFn;
import com.cognite.beam.io.fn.write.UpsertFileHeaderFn;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import static com.cognite.beam.io.CogniteIO.invalidProjectConfigFile;

public abstract class FilesMetadata {

    /**
     * Transform that will read a collection of {@link FileMetadata} objects from Cognite Data Fusion.
     *
     * You specify which {@link FileMetadata} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class Read extends ConnectorBase<PBegin, PCollection<FileMetadata>> {

        public static FilesMetadata.Read.Builder builder() {
            return new com.cognite.beam.io.AutoValue_FilesMetadata_Read.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setReaderConfig(ReaderConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();

        public abstract FilesMetadata.Read.Builder toBuilder();

        public FilesMetadata.Read withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public FilesMetadata.Read withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public FilesMetadata.Read withRequestParameters(RequestParameters params) {
            Preconditions.checkNotNull(params, "Parameters cannot be null.");
            return toBuilder().setRequestParameters(params).build();
        }

        public FilesMetadata.Read withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public FilesMetadata.Read withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public FilesMetadata.Read withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<FileMetadata> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building read file metadata composite transform.");

            PCollection<FileMetadata> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllFilesMetadata()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                    );

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Read.Builder setRequestParameters(RequestParameters value);
            public abstract Read.Builder setReaderConfig(ReaderConfig value);

            public abstract Read build();
        }
    }

    /**
     * Transform that will read a collection of {@link FileMetadata} objects from Cognite Data Fusion.
     *
     * You specify which {@link FileMetadata} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link FileMetadata} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAll
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<FileMetadata>> {

        public static FilesMetadata.ReadAll.Builder builder() {
            return new com.cognite.beam.io.AutoValue_FilesMetadata_ReadAll.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setReaderConfig(ReaderConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract FilesMetadata.ReadAll.Builder toBuilder();

        public FilesMetadata.ReadAll withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAll withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public FilesMetadata.ReadAll withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public FilesMetadata.ReadAll withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public FilesMetadata.ReadAll withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }
        @Override
        public PCollection<FileMetadata> expand(PCollection<RequestParameters> input) {

            PCollection<FileMetadata> outputCollection = input
                    .apply("Read direct", CogniteIO.readAllDirectFilesMetadata()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig())
                            .withHints(getHints()))
                    .apply("Unwrap files metadata", ParDo.of(new DoFn<List<FileMetadata>, FileMetadata>() {
                        @ProcessElement
                        public void processElement(@Element List<FileMetadata> element,
                                                   OutputReceiver<FileMetadata> out) {
                            if (getReaderConfig().isStreamingEnabled()) {
                                // output with timestamp
                                element.forEach(row -> out.outputWithTimestamp(row,
                                        org.joda.time.Instant.ofEpochMilli(row.getLastUpdatedTime())));
                            } else {
                                // output without timestamp
                                element.forEach(row -> out.output(row));
                            }
                        }
                    }));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<FilesMetadata.ReadAll.Builder> {
            public abstract ReadAll.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAll build();
        }
    }

    /**
     * Transform that will read a collection of {@link FileMetadata} objects from Cognite Data Fusion. The
     * {@link FileMetadata} result objects are returned in batches ({@code List<FileMetadata>}) of size <10k.
     *
     * You specify which {@link FileMetadata} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link FileMetadata} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllDirect
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<List<FileMetadata>>> {

        public static FilesMetadata.ReadAllDirect.Builder builder() {
            return new com.cognite.beam.io.AutoValue_FilesMetadata_ReadAllDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setReaderConfig(ReaderConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract ReadAllDirect.Builder toBuilder();

        public ReadAllDirect withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAllDirect withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadAllDirect withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public ReadAllDirect withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public ReadAllDirect withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }
        @Override
        public PCollection<List<FileMetadata>> expand(PCollection<RequestParameters> input) {
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
                                new GenerateReadRequestsUnboundFn(getReaderConfig(), ResourceType.FILE_HEADER)));
            } else {
                // batch mode
                LOG.info("Setting up batch mode");
                requestParametersPCollection = input;
            }

            PCollection<List<FileMetadata>> outputCollection = requestParametersPCollection
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("Apply delta timestamp", ApplyDeltaTimestamp.to(ResourceType.FILE_HEADER)
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Add partitions", ParDo.of(new AddPartitionsFn(getHints(),
                                    getReaderConfig().enableMetrics(false), ResourceType.FILE_HEADER,
                                    projectConfigView))
                            .withSideInputs(projectConfigView))
                    .apply("Break fusion", BreakFusion.<RequestParameters>create())
                    .apply(ParDo.of(new ListFilesFn(getHints(), getReaderConfig(),projectConfigView))
                            .withSideInputs(projectConfigView));

            // Record delta timestamp
            outputCollection
                    .apply("Extract last change timestamp", MapElements.into(TypeDescriptors.longs())
                            .via((List<FileMetadata> batch) -> batch.stream()
                                    .mapToLong(FileMetadata::getLastUpdatedTime)
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
        public abstract static class Builder extends ConnectorBase.Builder<FilesMetadata.ReadAllDirect.Builder> {
            public abstract ReadAllDirect.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAllDirect build();
        }
    }

    /**
     * Transform that will read a collection of {@link FileMetadata} objects from Cognite Data Fusion.
     *
     * You specify which {@link FileMetadata} objects to read via a set of ids enclosed in
     * {@link Item} objects. This transform takes a collection of {@link Item}
     * as input and returns all {@link FileMetadata} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllById
            extends ConnectorBase<PCollection<Item>, PCollection<FileMetadata>> {

        public static FilesMetadata.ReadAllById.Builder builder() {
            return new com.cognite.beam.io.AutoValue_FilesMetadata_ReadAllById.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract FilesMetadata.ReadAllById.Builder toBuilder();

        public FilesMetadata.ReadAllById withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public FilesMetadata.ReadAllById withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public FilesMetadata.ReadAllById withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public FilesMetadata.ReadAllById withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public FilesMetadata.ReadAllById withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<FileMetadata> expand(PCollection<Item> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<FileMetadata> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(4000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Read results", ParDo.of(
                            new RetrieveFilesFn(getHints(), getReaderConfig(),
                                    projectConfigView)).withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<FilesMetadata.ReadAllById.Builder> {
            public abstract FilesMetadata.ReadAllById.Builder setReaderConfig(ReaderConfig value);
            public abstract FilesMetadata.ReadAllById build();
        }
    }

    /**
     * Transform that will write {@link FileMetadata} objects to Cognite Data Fusion.
     * <p>
     * The input objects will be batched and upserted. If the {@link FileMetadata} object
     * does not exist, it will be created as a new object. In case the {@link FileMetadata} already
     * exists, it will be updated with the new input.
     */
    @AutoValue
    public abstract static class Write
            extends ConnectorBase<PCollection<FileMetadata>, PCollection<FileMetadata>> {
        private static final int MAX_WRITE_BATCH_SIZE = 400;

        public static Write.Builder builder() {
            return new com.cognite.beam.io.AutoValue_FilesMetadata_Write.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Write.Builder toBuilder();

        public Write withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Write withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Write withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Write withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Write withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        @Override
        public PCollection<FileMetadata> expand(PCollection<FileMetadata> input) {
            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<FileMetadata> eventCoder = ProtoCoder.of(FileMetadata.class);
            KvCoder<String, FileMetadata> keyValueCoder = KvCoder.of(utf8Coder, eventCoder);

            // main input
            PCollection<FileMetadata> outputCollection = input
                    .apply("Check id", MapElements.into(TypeDescriptor.of(FileMetadata.class))
                            .via((FileMetadata inputItem) -> {
                                if (inputItem.hasExternalId() || inputItem.hasId()) {
                                    return inputItem;
                                } else {
                                    return FileMetadata.newBuilder(inputItem)
                                            .setExternalId(UUID.randomUUID().toString())
                                            .build();
                                }
                            }))
                    .apply("Shard items", WithKeys.of((FileMetadata inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, FileMetadata>of(keyValueCoder)
                            .withMaxBatchSize(MAX_WRITE_BATCH_SIZE)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<FileMetadata>>create())
                    .apply("Write file metadata", CogniteIO.writeDirectFilesMetadata()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(getWriterConfig())
                            .withHints(getHints()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);

            public abstract Write build();
        }
    }

    /**
     * Writes {@code file metadata/headers} directly to the Cognite API, bypassing the regular validation and optimization steps. This
     * writer is designed for advanced use with very large data volumes. Most use cases should
     * use the regular {@link FilesMetadata.Write} writer which will perform shuffling and batching to optimize
     * the write performance.
     *
     * This writer will push each input {@link Iterable<FileMetadata>} as a single batch. If the input
     * violates any constraints, the write will fail. Also, the performance of the writer depends heavily on the
     * input being batched as optimally as possible.
     *
     * If your source system offers data pre-batched, you may get additional performance from this writer as
     * it bypasses the regular shuffle and batch steps.
     */
    @AutoValue
    public abstract static class WriteDirect
            extends ConnectorBase<PCollection<Iterable<FileMetadata>>, PCollection<FileMetadata>> {

        public static WriteDirect.Builder builder() {
            return new com.cognite.beam.io.AutoValue_FilesMetadata_WriteDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract WriteDirect.Builder toBuilder();

        public WriteDirect withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public WriteDirect withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public WriteDirect withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public WriteDirect withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public WriteDirect withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        @Override
        public PCollection<FileMetadata> expand(PCollection<Iterable<FileMetadata>> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<FileMetadata> outputCollection = input
                    .apply("Upsert items", ParDo.of(
                                    new UpsertFileHeaderFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);

            public abstract WriteDirect build();
        }
    }
}
