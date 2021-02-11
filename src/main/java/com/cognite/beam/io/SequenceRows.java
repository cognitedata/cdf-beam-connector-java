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
import com.cognite.beam.io.fn.read.ListSequencesRowsFn;
import com.cognite.client.dto.SequenceBody;
import com.cognite.client.config.ResourceType;
import com.cognite.beam.io.fn.delete.DeleteSequenceRowsFn;
import com.cognite.beam.io.fn.parse.ParseSequenceBodyFn;
import com.cognite.beam.io.fn.read.ReadItemsIteratorFn;
import com.cognite.beam.io.fn.write.UpsertSeqBodyFn;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.beam.io.CogniteIO.invalidProjectConfigFile;

public abstract class SequenceRows {

    /**
     * Transform that will read a collection of {@link SequenceBody} objects from Cognite Data Fusion.
     *
     * You specify which {@link SequenceBody} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class Read extends ConnectorBase<PBegin, PCollection<SequenceBody>> {

        public static Builder builder() {
            return new AutoValue_SequenceRows_Read.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract ReaderConfig getReaderConfig();

        public abstract RequestParameters getRequestParameters();

        public abstract Builder toBuilder();

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
        public PCollection<SequenceBody> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building read sequence rows composite transform.");

            PCollection<SequenceBody> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllSequenceRows()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setRequestParameters(RequestParameters value);

            public abstract Read build();
        }
    }

    /**
     * Transform that will read a collection of {@link SequenceBody} objects from Cognite Data Fusion.
     *
     * You specify which {@link SequenceBody} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link SequenceBody} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAll
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<SequenceBody>> {

        public static Builder builder() {
            return new AutoValue_SequenceRows_ReadAll.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract ReaderConfig getReaderConfig();

        public abstract Builder toBuilder();

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
        public PCollection<SequenceBody> expand(PCollection<RequestParameters> input) {
            LOG.debug("Building read all sequence rows composite transform.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getReaderConfig().getAppIdentifier())
                            .withSessionIdentifier(getReaderConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<SequenceBody> outputCollection = input
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Break fusion", BreakFusion.<RequestParameters>create())
                    .apply("Read results", ParDo.of(new ListSequencesRowsFn(getHints(),
                            getReaderConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);

            public abstract ReadAll build();
        }
    }

    /**
     * Transform that will write {@link SequenceBody} objects to Cognite Data Fusion.
     * <p>
     * The input objects will be batched and upserted. If the {@link SequenceBody} object
     * does not exist, it will be created as a new object. In case the {@link SequenceBody} already
     * exists, it will be updated with the new input.
     */
    @AutoValue
    public abstract static class Write
            extends ConnectorBase<PCollection<SequenceBody>, PCollection<SequenceBody>> {
        private static final int DEFAULT_WORKER_PARALLELIZATION = 4;

        public static SequenceRows.Write.Builder builder() {
            return new AutoValue_SequenceRows_Write.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setWorkerParallelizationFactor(DEFAULT_WORKER_PARALLELIZATION);
        }

        public abstract WriterConfig getWriterConfig();
        public abstract int getWorkerParallelizationFactor();

        public abstract SequenceRows.Write.Builder toBuilder();

        public SequenceRows.Write withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public SequenceRows.Write withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public SequenceRows.Write withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public SequenceRows.Write withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public SequenceRows.Write withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public SequenceRows.Write withWorkerParallelizationFactor(int parallelization) {
            return toBuilder().setWorkerParallelizationFactor(parallelization).build();
        }

        @Override
        public PCollection<SequenceBody> expand(PCollection<SequenceBody> input) {
            LOG.debug("Building upsert sequences rows composite transform.");

            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<SequenceBody> seqBodyCoder = ProtoCoder.of(SequenceBody.class);
            KvCoder<String, SequenceBody> keyValueCoder = KvCoder.of(utf8Coder, seqBodyCoder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getWriterConfig().getAppIdentifier())
                            .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<SequenceBody> outputCollection = input
                    .apply("Check id", MapElements.into(TypeDescriptor.of(SequenceBody.class))
                            .via((SequenceBody inputItem) -> {
                                if (inputItem.hasExternalId() || inputItem.hasId()) {
                                    return inputItem;
                                } else {
                                    return SequenceBody.newBuilder(inputItem)
                                            .setExternalId(StringValue.of(UUID.randomUUID().toString()))
                                            .build();
                                }
                            }))
                    .apply("Shard items", WithKeys.of((SequenceBody inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatchesSequencesBodies.create()
                            .withWorkerParallelizationFactor(getWorkerParallelizationFactor())
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<SequenceBody>>create())
                    .apply("Upsert Rows", ParDo.of(
                            new UpsertSeqBodyFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<SequenceRows.Write.Builder> {
            public abstract SequenceRows.Write.Builder setWriterConfig(WriterConfig value);
            public abstract SequenceRows.Write.Builder setWorkerParallelizationFactor(int value);

            public abstract SequenceRows.Write build();
        }
    }

    /**
     * Transform that will delete {@link SequenceBody} rows from Cognite Data Fusion.
     * <p>
     * The input to this transform is a collection of {@link SequenceBody} objects that identifies
     * which rows to delete.
     */
    @AutoValue
    public abstract static class Delete
            extends ConnectorBase<PCollection<SequenceBody>, PCollection<SequenceBody>> {

        public static SequenceRows.Delete.Builder builder() {
            return new AutoValue_SequenceRows_Delete.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract WriterConfig getWriterConfig();

        public abstract SequenceRows.Delete.Builder toBuilder();

        public SequenceRows.Delete withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public SequenceRows.Delete withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public SequenceRows.Delete withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public SequenceRows.Delete withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public SequenceRows.Delete withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        @Override
        public PCollection<SequenceBody> expand(PCollection<SequenceBody> input) {
            LOG.info("Starting Cognite writer.");
            LOG.debug("Building delete sequence rows composite transform.");

            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<SequenceBody> seqBodyCoder = ProtoCoder.of(SequenceBody.class);
            KvCoder<String, SequenceBody> keyValueCoder = KvCoder.of(utf8Coder, seqBodyCoder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getWriterConfig().getAppIdentifier())
                            .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<SequenceBody> outputCollection = input
                    .apply("Shard items", WithKeys.of((SequenceBody inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, SequenceBody>of(keyValueCoder)
                            .withMaxBatchSize(10)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<SequenceBody>>create())
                    .apply("Delete sequence rows", ParDo.of(
                            new DeleteSequenceRowsFn(getHints(),
                                                    getWriterConfig().getAppIdentifier(),
                                                    getWriterConfig().getSessionIdentifier(),
                                                    projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<SequenceRows.Delete.Builder> {
            public abstract SequenceRows.Delete.Builder setWriterConfig(WriterConfig value);

            public abstract SequenceRows.Delete build();
        }
    }
}
