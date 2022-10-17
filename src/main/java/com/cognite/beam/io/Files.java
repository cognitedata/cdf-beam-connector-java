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
import com.cognite.beam.io.fn.read.RetrieveFileContainersFn;
import com.cognite.beam.io.fn.write.RemoveTempFile;
import com.cognite.client.dto.*;
import com.cognite.client.config.ResourceType;
import com.cognite.beam.io.fn.delete.DeleteItemsFn;
import com.cognite.beam.io.fn.read.ReadAggregatesFn;
import com.cognite.beam.io.fn.write.UpsertFileFn;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.beam.io.CogniteIO.defaultHints;
import static com.cognite.beam.io.CogniteIO.invalidProjectConfigFile;

public abstract class Files {

    /**
     * Transform that will read a collection of {@code file} objects from Cognite Data Fusion.
     *
     * You specify which {@code file} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class Read extends ConnectorBase<PBegin, PCollection<FileContainer>> {

        public static Files.Read.Builder builder() {
            return new AutoValue_Files_Read.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setReaderConfig(ReaderConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setForceTempStorage(false);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();
        @Nullable
        public abstract ValueProvider<String> getTempStorageURI();
        public abstract boolean isForceTempStorage();

        public abstract Files.Read.Builder toBuilder();

        public Files.Read withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Files.Read withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public Files.Read withRequestParameters(RequestParameters params) {
            Preconditions.checkNotNull(params, "Parameters cannot be null.");
            return toBuilder().setRequestParameters(params).build();
        }

        public Files.Read withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        @Deprecated
        public Files.Read withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        @Deprecated
        public Files.Read withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Files.Read withTempStorageURI(ValueProvider<String> tempStorageURI) {
            return toBuilder().setTempStorageURI(tempStorageURI).build();
        }

        public Files.Read enableForceTempStorage(boolean forceTempStorage) {
            return toBuilder().setForceTempStorage(forceTempStorage).build();
        }

        @Override
        public PCollection<FileContainer> expand(PBegin input) {

            PCollection<FileContainer> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllFiles()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withTempStorageURI(getTempStorageURI())
                            .enableForceTempStorage(isForceTempStorage())
                    );

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Read.Builder setRequestParameters(RequestParameters value);
            public abstract Read.Builder setReaderConfig(ReaderConfig value);
            public abstract Read.Builder setTempStorageURI(ValueProvider<String> tempStorageURI);
            public abstract Read.Builder setForceTempStorage(boolean value);

            public abstract Read build();
        }
    }

    /**
     * Transform that will read a collection of {@code file} objects from Cognite Data Fusion.
     *
     * You specify which {@code file} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@code file} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAll
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<FileContainer>> {

        public static Files.ReadAll.Builder builder() {
            return new AutoValue_Files_ReadAll.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setReaderConfig(ReaderConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setForceTempStorage(false);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Files.ReadAll.Builder toBuilder();

        @Nullable
        public abstract ValueProvider<String> getTempStorageURI();
        public abstract boolean isForceTempStorage();

        public Files.ReadAll withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAll withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public Files.ReadAll withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        @Deprecated
        public Files.ReadAll withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        @Deprecated
        public Files.ReadAll withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Files.ReadAll withTempStorageURI(ValueProvider<String> tempStorageURI) {
            return toBuilder().setTempStorageURI(tempStorageURI).build();
        }

        public Files.ReadAll enableForceTempStorage(boolean forceTempStorage) {
            return toBuilder().setForceTempStorage(forceTempStorage).build();
        }

        @Override
        public PCollection<FileContainer> expand(PCollection<RequestParameters> input) {

            // Download the file binaries matching the file metadata
            PCollection<FileContainer> outputCollection = input
                    .apply("Read files direct", CogniteIO.readAllDirectFiles()
                            .withHints(getHints())
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig())
                            .withTempStorageURI(getTempStorageURI())
                            .enableForceTempStorage(isForceTempStorage()))
                    .apply("Unwrap batch", ParDo.of(new DoFn<List<FileContainer>, FileContainer>() {
                        @ProcessElement
                        public void processElement(@Element List<FileContainer> element,
                                                   OutputReceiver<FileContainer> out) {
                            // output without timestamp
                            element.forEach(row -> out.output(row));
                        }
                    }));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Files.ReadAll.Builder> {
            public abstract ReadAll.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAll.Builder setTempStorageURI(ValueProvider<String> tempStorageURI);
            public abstract ReadAll.Builder setForceTempStorage(boolean value);
            public abstract ReadAll build();
        }
    }

    /**
     * Transform that will read a collection of {@code file} objects from Cognite Data Fusion.
     *
     * You specify which {@code file} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@code file} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllDirect
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<List<FileContainer>>> {
        // the max parallelization potential of the job
        private static final int NO_SHARDS = 10;

        public static Files.ReadAllDirect.Builder builder() {
            return new AutoValue_Files_ReadAllDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setReaderConfig(ReaderConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setForceTempStorage(false);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Files.ReadAllDirect.Builder toBuilder();

        @Nullable
        public abstract ValueProvider<String> getTempStorageURI();
        public abstract boolean isForceTempStorage();

        public Files.ReadAllDirect withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAllDirect withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public Files.ReadAllDirect withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Files.ReadAllDirect withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Files.ReadAllDirect withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Files.ReadAllDirect withTempStorageURI(ValueProvider<String> tempStorageURI) {
            return toBuilder().setTempStorageURI(tempStorageURI).build();
        }

        public Files.ReadAllDirect enableForceTempStorage(boolean forceTempStorage) {
            return toBuilder().setForceTempStorage(forceTempStorage).build();
        }

        @Override
        public PCollection<List<FileContainer>> expand(PCollection<RequestParameters> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // Identify the files matching the request and read the file metadata
            PCollection<List<FileContainer>> outputCollection = input
                    .apply("List file metadata", CogniteIO.readAllFilesMetadata()
                            .withHints(getHints())
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Convert to items", MapElements.into(TypeDescriptor.of(Item.class))
                            .via(fileMetadata ->
                                    Item.newBuilder()
                                            .setId(fileMetadata.getId())
                                            .build()))
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(getHints().getReadFileBinaryBatchSize())
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(NO_SHARDS)
                            .build())
                    .apply("Retrieve files", ParDo.of(new RetrieveFileContainersFn(getHints(),
                                    getReaderConfig(), getTempStorageURI(), isForceTempStorage(), projectConfigView))
                            .withSideInputs(projectConfigView));

            // Record delta timestamp
            outputCollection
                    .apply("Extract last change timestamp", MapElements.into(TypeDescriptors.longs())
                            .via((List<FileContainer> batch) -> batch.stream()
                                    .mapToLong(fileContainer -> fileContainer.getFileMetadata().getLastUpdatedTime())
                                    .max()
                                    .orElse(1L)))
                    .apply("Record delta timestamp", RecordDeltaTimestamp.create()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Files.ReadAllDirect.Builder> {
            public abstract ReadAllDirect.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAllDirect.Builder setTempStorageURI(ValueProvider<String> tempStorageURI);
            public abstract ReadAllDirect.Builder setForceTempStorage(boolean value);
            public abstract ReadAllDirect build();
        }
    }

    /**
     * Transform that will read aggregate/summary statistics related to {@code file} objects in
     * Cognite Data Fusion.
     *
     * You specify the parameters of the aggregate(s) via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class ReadAggregate extends ConnectorBase<PBegin, PCollection<Aggregate>> {

        public static Files.ReadAggregate.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Files_ReadAggregate.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();

        public abstract Files.ReadAggregate.Builder toBuilder();

        public Files.ReadAggregate withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Files.ReadAggregate withRequestParameters(RequestParameters params) {
            Preconditions.checkNotNull(params, "Parameters cannot be null.");
            return toBuilder().setRequestParameters(params).build();
        }

        public Files.ReadAggregate withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Files.ReadAggregate withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Files.ReadAggregate withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Files.ReadAggregate withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Aggregate> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building read events composite transform.");

            PCollection<Aggregate> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllAggregatesFiles()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                    );

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Files.ReadAggregate.Builder> {
            public abstract Files.ReadAggregate.Builder setRequestParameters(RequestParameters value);
            public abstract Files.ReadAggregate.Builder setReaderConfig(ReaderConfig value);

            public abstract Files.ReadAggregate build();
        }
    }

    /**
     * Transform that will read aggregate/summary statistics related to {@code file} objects in
     * Cognite Data Fusion.
     *
     * You specify the parameters of the aggregate(s) via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link Aggregate} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllAggregate
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<Aggregate>> {

        public static Files.ReadAllAggregate.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Files_ReadAllAggregate.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Files.ReadAllAggregate.Builder toBuilder();

        public Files.ReadAllAggregate withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Files.ReadAllAggregate withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Files.ReadAllAggregate withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Files.ReadAllAggregate withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Files.ReadAllAggregate withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Aggregate> expand(PCollection<RequestParameters> input) {
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
                                    projectConfigView, ResourceType.FILE_HEADER))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Files.ReadAllAggregate.Builder> {
            public abstract Files.ReadAllAggregate.Builder setReaderConfig(ReaderConfig value);
            public abstract Files.ReadAllAggregate build();
        }
    }

    /**
     * Transform that will write {@link FileContainer} objects to Cognite Data Fusion.
     * <p>
     * The input objects will be batched and upserted. If the {@link FileContainer} object
     * does not exist, it will be created as a new object. In case the {@link FileContainer} already
     * exists, it will be updated with the new input.
     */
    @AutoValue
    public abstract static class Write
            extends ConnectorBase<PCollection<FileContainer>, PCollection<FileMetadata>> {
        private static final String loggingPrefix = "File.Write - ";

        public static Write.Builder builder() {
            return new AutoValue_Files_Write.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setDeleteTempFile(true);
        }

        public abstract WriterConfig getWriterConfig();
        public abstract boolean isDeleteTempFile();
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

        /**
         * Configure how to treat a temp blob after an upload. This setting only affects behavior when uploading
         * file binaries to the Cognite API--it has no effect on downloading file binaries.
         *
         * When set to {@code true}, the temp file (if present) will be removed after a successful upload. If the file
         * binary is memory-based (which is the default for small and medium sized files), this setting has no effect.
         *
         * When set to {@code false}, the temp file (if present) will not be deleted.
         *
         * The default setting is {@code true}.
         *
         * @param enable
         * @return
         */
        public Write enableDeleteTempFile(boolean enable) {
            return toBuilder().setDeleteTempFile(enable).build();
        }

        @Override
        public PCollection<FileMetadata> expand(PCollection<FileContainer> input) {
            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<FileContainer> containerCoder = ProtoCoder.of(FileContainer.class);
            KvCoder<String, FileContainer> keyValueCoder = KvCoder.of(utf8Coder, containerCoder);

            // main input
            PCollection<FileMetadata> upsertedFilesCollection = input
                    .apply("Check id", MapElements.into(TypeDescriptor.of(FileContainer.class))
                            .via((FileContainer inputItem) -> {
                                if (inputItem.getFileMetadata().hasExternalId() || inputItem.getFileMetadata().hasId()) {
                                    return inputItem;
                                } else {
                                    LOG.warn(loggingPrefix + "File does not contain id nor externalId. "
                                            + "Will generate a UUID to use as externalID.");
                                    FileMetadata metadata = inputItem.getFileMetadata().toBuilder()
                                            .setExternalId(UUID.randomUUID().toString())
                                            .build();
                                    return inputItem.toBuilder()
                                            .setFileMetadata(metadata)
                                            .build();
                                }
                            }))
                    .apply("Shard items", WithKeys.of((FileContainer inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, FileContainer>of(keyValueCoder)
                            .withMaxBatchSize(getHints().getWriteFileBatchSize())
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<FileContainer>>create())
                    .apply("Write files", CogniteIO.writeDirectFiles()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(getWriterConfig())
                            .withHints(getHints())
                            .enableDeleteTempFile(isDeleteTempFile()));

            return upsertedFilesCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            abstract Builder setWriterConfig(WriterConfig value);
            abstract Builder setDeleteTempFile(boolean value);

            public abstract Write build();
        }
    }

    /**
     * Writes {@code FileContainer} directly to the Cognite API, bypassing the regular validation and optimization steps. This
     * writer is designed for advanced use cases where you have pre-batched the data. Most use cases should
     * use the regular {@link Files.Write} writer which will perform shuffling and batching to optimize
     * the write performance.
     *
     * This writer will push each input {@link Iterable<FileContainer>} as a single batch. If the input
     * violates any constraints, the write will fail.
     *
     * If your source system offers data pre-batched, you may get additional performance from this writer as
     * it bypasses the regular shuffle and batch steps.
     */
    @AutoValue
    public abstract static class WriteDirect
            extends ConnectorBase<PCollection<Iterable<FileContainer>>, PCollection<FileMetadata>> {
        private static final String loggingPrefix = "File.WriteDirect - ";

        public static WriteDirect.Builder builder() {
            return new AutoValue_Files_WriteDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setDeleteTempFile(true);
        }

        public abstract WriterConfig getWriterConfig();
        public abstract boolean isDeleteTempFile();
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

        /**
         * Configure how to treat a temp blob after an upload. This setting only affects behavior when uploading
         * file binaries to the Cognite API--it has no effect on downloading file binaries.
         *
         * When set to {@code true}, the temp file (if present) will be removed after a successful upload. If the file
         * binary is memory-based (which is the default for small and medium sized files), this setting has no effect.
         *
         * When set to {@code false}, the temp file (if present) will not be deleted.
         *
         * The default setting is {@code true}.
         *
         * @param enable
         * @return
         */
        public WriteDirect enableDeleteTempFile(boolean enable) {
            return toBuilder().setDeleteTempFile(enable).build();
        }

        @Override
        public PCollection<FileMetadata> expand(PCollection<Iterable<FileContainer>> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input. Upsert the file containers
            PCollection<KV<FileContainer, FileMetadata>> upsertedFilesCollection = input
                    .apply("Upsert files", ParDo.of(
                                    new UpsertFileFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            // The main output. Just filter out the file containers and keep the file metadata
            PCollection<FileMetadata> outputCollection = upsertedFilesCollection
                    .apply("Filter file metadata", Values.create());

            // Record successful data pipeline run
            if (null != getWriterConfig().getExtractionPipelineExtId()) {
                outputCollection
                        .apply("Report pipeline run", WritePipelineRun.<FileMetadata>create()
                                .withProjectConfig(getProjectConfig())
                                .withProjectConfigFile(getProjectConfigFile())
                                .withWriterConfig(getWriterConfig()));
            }

            // Remove the temporary files, if enabled.
            // Must "wait on" the file upsert to finish because of possible bundle re-tries.
            PCollection<String> tempFilesUriCollection = upsertedFilesCollection
                    .apply("Filter file containers", Keys.create())
                    .apply("Filter temp files", Filter.by(fileContainer ->
                            isDeleteTempFile()
                                    && fileContainer.hasFileBinary()
                                    && fileContainer.getFileBinary().getBinaryTypeCase() == FileBinary.BinaryTypeCase.BINARY_URI
                    ))
                    .apply("Get temp file URI", MapElements.into(TypeDescriptors.strings())
                            .via((FileContainer fileContainer) -> fileContainer.getFileBinary().getBinaryUri())
                    );

            tempFilesUriCollection
                    .apply("Wait on: Upsert files", Wait.on(tempFilesUriCollection))
                    .apply("Remove temp binary", ParDo.of(new RemoveTempFile()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            abstract Builder setWriterConfig(WriterConfig value);
            abstract Builder setDeleteTempFile(boolean value);

            public abstract WriteDirect build();
        }
    }

    /**
     * Transform that will delete {@code file} objects from Cognite Data Fusion.
     * <p>
     * The input to this transform is a collection of {@link Item} objects that identifies (via
     * id or externalId) which {@code file} objects to delete.
     */
    @AutoValue
    public abstract static class Delete
            extends ConnectorBase<PCollection<Item>, PCollection<Item>> {

        public static Files.Delete.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Files_Delete.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract WriterConfig getWriterConfig();
        public abstract Files.Delete.Builder toBuilder();

        public Files.Delete withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Files.Delete withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Files.Delete withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public Files.Delete withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public Files.Delete withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "File path cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        @Override
        public PCollection<Item> expand(PCollection<Item> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<Item> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(1000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Delete items", ParDo.of(
                            new DeleteItemsFn(getHints(), getWriterConfig(), ResourceType.FILE, projectConfigView))
                            .withSideInputs(projectConfigView));

            // Record successful data pipeline run
            if (null != getWriterConfig().getExtractionPipelineExtId()) {
                outputCollection
                        .apply("Report pipeline run", WritePipelineRun.<Item>create()
                                .withProjectConfig(getProjectConfig())
                                .withProjectConfigFile(getProjectConfigFile())
                                .withWriterConfig(getWriterConfig())
                                .withWriterOperationDescription("deleted"));
            }

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Files.Delete.Builder> {
            public abstract Files.Delete.Builder setWriterConfig(WriterConfig value);
            public abstract Files.Delete build();
        }
    }
}
