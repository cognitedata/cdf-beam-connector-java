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
import com.cognite.client.dto.FileMetadata;
import com.cognite.client.dto.Item;
import com.cognite.beam.io.fn.ResourceType;
import com.cognite.beam.io.fn.parse.ParseFileMetaFn;
import com.cognite.beam.io.fn.read.ReadItemsByIdFn;
import com.cognite.beam.io.fn.request.GenerateReadRequestsUnboundFn;
import com.cognite.beam.io.fn.write.UpsertFileHeaderFn;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import com.cognite.beam.io.fn.read.ReadItemsIteratorFn;
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
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building read all files metadata composite transform.");

            Preconditions.checkState(!(getReaderConfig().isStreamingEnabled() && getReaderConfig().isDeltaEnabled()),
                    "Using delta read in combination with streaming is not supported.");

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

            PCollection<FileMetadata> outputCollection = requestParametersPCollection
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Apply delta timestamp", ApplyDeltaTimestamp.to(ResourceType.FILE_HEADER)
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Read results", ParDo.of(new ReadItemsIteratorFn(getHints(), ResourceType.FILE_HEADER,
                            getReaderConfig())))
                    .apply("Parse results", ParDo.of(new ParseFileMetaFn()));

            // Record delta timestamp
            outputCollection
                    .apply("Extract last change timestamp", MapElements.into(TypeDescriptors.longs())
                            .via((FileMetadata fileHeader) -> fileHeader.getLastUpdatedTime().getValue()))
                    .apply("Record delta timestamp", RecordDeltaTimestamp.create()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<FilesMetadata.ReadAll.Builder> {
            public abstract ReadAll.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAll build();
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
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getReaderConfig().getAppIdentifier())
                            .withSessionIdentifier(getReaderConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<FileMetadata> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(1000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Read results", ParDo.of(
                            new ReadItemsByIdFn(getHints(), ResourceType.FILE_BY_ID, getReaderConfig(),
                                    projectConfigView)).withSideInputs(projectConfigView))
                    .apply("Parse results", ParDo.of(new ParseFileMetaFn()));

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
        private static final int MAX_WRITE_BATCH_SIZE = 100;

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
            LOG.info("Starting Cognite writer.");

            LOG.debug("Building upsert file metadata composite transform.");
            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<FileMetadata> eventCoder = ProtoCoder.of(FileMetadata.class);
            KvCoder<String, FileMetadata> keyValueCoder = KvCoder.of(utf8Coder, eventCoder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getWriterConfig().getAppIdentifier())
                            .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<FileMetadata> outputCollection = input
                    .apply("Check id", MapElements.into(TypeDescriptor.of(FileMetadata.class))
                            .via((FileMetadata inputItem) -> {
                                if (inputItem.hasExternalId() || inputItem.hasId()) {
                                    return inputItem;
                                } else {
                                    return FileMetadata.newBuilder(inputItem)
                                            .setExternalId(StringValue.of(UUID.randomUUID().toString()))
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
                    .apply("Upsert items", ParDo.of(
                            new UpsertFileHeaderFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView))
                    .apply("Parse results items", ParDo.of(new ParseFileMetaFn()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);

            public abstract Write build();
        }
    }
}
