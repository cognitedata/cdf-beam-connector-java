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
import com.cognite.beam.io.fn.read.ListRelationshipsFn;
import com.cognite.beam.io.fn.read.RetrieveRelationshipsFn;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Relationship;
import com.cognite.client.config.ResourceType;
import com.cognite.beam.io.fn.delete.DeleteItemsFn;
import com.cognite.beam.io.fn.write.UpsertRelationshipFn;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.beam.io.CogniteIO.invalidProjectConfigFile;

public abstract class Relationships {

    /**
     * Transform that will read a collection of {@link Relationship} objects from Cognite Data Fusion.
     *
     * You specify which {@link Relationship} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class Read extends ConnectorBase<PBegin, PCollection<Relationship>> {

        public static Builder builder() {
            return new AutoValue_Relationships_Read.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();

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

        public Read withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Read withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Read withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Relationship> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building read relationships composite transform.");

            PCollection<Relationship> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllRelationships()
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

            public abstract Read build();
        }
    }

    /**
     * Transform that will read a collection of {@link Relationship} objects from Cognite Data Fusion.
     *
     * You specify which {@link Relationship} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link Relationship} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAll
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<Relationship>> {

        public static Builder builder() {
            return new AutoValue_Relationships_ReadAll.Builder()
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

        public ReadAll withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public ReadAll withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public ReadAll withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Relationship> expand(PCollection<RequestParameters> input) {
            LOG.debug("Building read all relationships composite transform.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getReaderConfig().getAppIdentifier())
                            .withSessionIdentifier(getReaderConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<Relationship> outputCollection = input
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Apply delta timestamp", ApplyDeltaTimestamp.to(ResourceType.RELATIONSHIP)
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()))
                    //.apply("Add partitions", ParDo.of(new AddPartitionsFn(getHints(), ResourceType.RELATIONSHIP,
                    //        getReaderConfig().enableMetrics(false))))
                    //.apply("Break fusion", BreakFusion.<RequestParameters>create())
                    .apply("Read results", ParDo.of(
                            new ListRelationshipsFn(getHints(), getReaderConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            // Record delta timestamp
            outputCollection
                    .apply("Extract last change timestamp", MapElements.into(TypeDescriptors.longs())
                            .via((Relationship rel) -> rel.getLastUpdatedTime().getValue()))
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
     * Transform that will read a collection of {@link Relationship} objects from Cognite Data Fusion.
     *
     * You specify which {@link Relationship} objects to read via a set of ids enclosed in
     * {@link Item} objects. This transform takes a collection of {@link Item}
     * as input and returns all {@link Relationship} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllById
            extends ConnectorBase<PCollection<Item>, PCollection<Relationship>> {

        public static Relationships.ReadAllById.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Relationships_ReadAllById.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Relationships.ReadAllById.Builder toBuilder();

        public Relationships.ReadAllById withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Relationships.ReadAllById withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Relationships.ReadAllById withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Relationships.ReadAllById withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Relationships.ReadAllById withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Relationship> expand(PCollection<Item> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getReaderConfig().getAppIdentifier())
                            .withSessionIdentifier(getReaderConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<Relationship> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(4000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Read results", ParDo.of(
                            new RetrieveRelationshipsFn(getHints(), getReaderConfig(),
                                    projectConfigView)).withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Relationships.ReadAllById.Builder> {
            public abstract Relationships.ReadAllById.Builder setReaderConfig(ReaderConfig value);
            public abstract Relationships.ReadAllById build();
        }
    }

    /**
     * Transform that will write {@link Relationship} objects to Cognite Data Fusion.
     * <p>
     * The input objects will be batched and upserted. If the {@link Relationship} object
     * does not exist, it will be created as a new object. In case the {@link Relationship} already
     * exists, it will be replaced by the new input.
     */
    @AutoValue
    public abstract static class Write
            extends ConnectorBase<PCollection<Relationship>, PCollection<Relationship>> {
        private static final int MAX_WRITE_BATCH_SIZE = 4000;

        public static Builder builder() {
            return new AutoValue_Relationships_Write.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();

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
        public PCollection<Relationship> expand(PCollection<Relationship> input) {
            LOG.info("Starting Cognite writer.");

            LOG.debug("Building upsert relationships composite transform.");
            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<Relationship> relationshipCoder = ProtoCoder.of(Relationship.class);
            KvCoder<String, Relationship> keyValueCoder = KvCoder.of(utf8Coder, relationshipCoder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getWriterConfig().getAppIdentifier())
                            .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<Relationship> outputCollection = input
                    .apply("Shard items", WithKeys.of((Relationship inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, Relationship>of(keyValueCoder)
                            .withMaxBatchSize(MAX_WRITE_BATCH_SIZE)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<Relationship>>create())
                    .apply("Upsert items", ParDo.of(
                            new UpsertRelationshipFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);

            public abstract Write build();
        }
    }

    /**
     * Transform that will delete {@link Relationship} objects from Cognite Data Fusion.
     * <p>
     * The input to this transform is a collection of {@link Item} objects that identifies (via
     * id or externalId) which {@link Relationship} objects to delete.
     */
    @AutoValue
    public abstract static class Delete
            extends ConnectorBase<PCollection<Item>, PCollection<Item>> {

        public static Builder builder() {
            return new AutoValue_Relationships_Delete.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();

        public Delete withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Delete withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Delete withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Delete withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Delete withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        @Override
        public PCollection<Item> expand(PCollection<Item> input) {
            LOG.info("Starting Cognite writer.");
            LOG.debug("Building delete relationships composite transform.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig())
                            .withAppIdentifier(getWriterConfig().getAppIdentifier())
                            .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<Item> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(1000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Delete items", ParDo.of(
                            new DeleteItemsFn(getHints(), ResourceType.RELATIONSHIP, getWriterConfig().getAppIdentifier(),
                                    getWriterConfig().getSessionIdentifier(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Delete build();
        }
    }
}
