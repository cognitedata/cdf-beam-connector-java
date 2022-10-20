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
import com.cognite.beam.io.fn.read.*;
import com.cognite.beam.io.fn.write.SynchronizeAssetHierarchyFn;
import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.beam.io.fn.*;
import com.cognite.beam.io.fn.delete.DeleteItemsFn;
import com.cognite.beam.io.fn.request.GenerateReadRequestsUnboundFn;
import com.cognite.beam.io.fn.write.UpsertAssetFn;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.internal.*;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import com.cognite.client.dto.Asset;
import com.google.auto.value.AutoValue;

import java.util.List;
import java.util.Map;

import static com.cognite.beam.io.CogniteIO.*;

public abstract class Assets {

    /**
     * Transform that will read a collection of {@link Asset} objects from Cognite Data Fusion.
     *
     * You specify which {@link Asset} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class Read extends ConnectorBase<PBegin, PCollection<Asset>> {

        public static Read.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_Read.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();

        public abstract Read.Builder toBuilder();

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

        @Deprecated
        public Read withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        @Deprecated
        public Read withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public Read withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "File path cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Asset> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");

            LOG.debug("Building read assets composite transform.");

            PCollection<Asset> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllAssets()
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
     * Transform that will read a collection of {@link Asset} objects from Cognite Data Fusion.
     *
     * You specify which {@link Asset} objects to read via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link Asset} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAll
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<Asset>> {

        public static ReadAll.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_ReadAll.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract ReaderConfig getReaderConfig();
        public abstract ReadAll.Builder toBuilder();

        public ReadAll withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAll withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
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

        public ReadAll withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "File path cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Asset> expand(PCollection<RequestParameters> input) {
            LOG.debug("Building read assets composite transform.");

            Preconditions.checkState(!(getReaderConfig().isStreamingEnabled() && getReaderConfig().isDeltaEnabled()),
                    "Using delta read in combination with streaming is not supported.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // Conditional streaming
            PCollection<RequestParameters> requestParametersPCollection;

            if (getReaderConfig().isStreamingEnabled()) {
                // streaming mode
                LOG.info("Setting up streaming mode");
                requestParametersPCollection = input
                        .apply("Watch for new items", ParDo.of(
                                new GenerateReadRequestsUnboundFn(getReaderConfig(), ResourceType.ASSET)));
            } else {
                // batch mode
                LOG.info("Setting up batch mode");
                requestParametersPCollection = input;
            }

            // main pipeline
            PCollection<Asset> outputCollection = requestParametersPCollection
                    .apply("Apply project config", ApplyProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("Apply delta timestamp", ApplyDeltaTimestamp.to(ResourceType.ASSET)
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()))
                    .apply("Add partitions", ParDo.of(new AddPartitionsFn(getHints(),
                            getReaderConfig().enableMetrics(false), ResourceType.ASSET,
                            projectConfigView))
                            .withSideInputs(projectConfigView))
                    .apply("Break fusion", BreakFusion.<RequestParameters>create())
                    .apply("Read results", ParDo.of(new ListAssetsFn(getHints(), getReaderConfig(),projectConfigView))
                            .withSideInputs(projectConfigView));

            // Record delta timestamp
            outputCollection
                    .apply("Extract last change timestamp", MapElements.into(TypeDescriptors.longs())
                            .via((Asset asset) -> asset.getLastUpdatedTime()))
                    .apply("Record delta timestamp", RecordDeltaTimestamp.create()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()));

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract ReadAll.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAll build();
        }
    }

    /**
     * Transform that will read a collection of {@link Asset} objects from Cognite Data Fusion.
     *
     * You specify which {@link Asset} objects to read via a set of ids enclosed in
     * {@link Item} objects. This transform takes a collection of {@link Item}
     * as input and returns all {@link Asset} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllById
            extends ConnectorBase<PCollection<Item>, PCollection<Asset>> {

        public static Assets.ReadAllById.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_ReadAllById.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Assets.ReadAllById.Builder toBuilder();

        public Assets.ReadAllById withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Assets.ReadAllById withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Assets.ReadAllById withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Assets.ReadAllById withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Assets.ReadAllById withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Asset> expand(PCollection<Item> input) {
            LOG.debug("Building read all asset by id composite transform.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<Asset> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(4000)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Read results", ParDo.of(
                            new RetrieveAssetsFn(getHints(), getReaderConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Assets.ReadAllById.Builder> {
            public abstract Assets.ReadAllById.Builder setReaderConfig(ReaderConfig value);
            public abstract Assets.ReadAllById build();
        }
    }

    /**
     * Transform that will read aggregate/summary statistics related to {@link Asset} objects in
     * Cognite Data Fusion.
     *
     * You specify the parameters of the aggregate(s) via a set of filters enclosed in
     * a {@link RequestParameters} object.
     */
    @AutoValue
    public abstract static class ReadAggregate extends ConnectorBase<PBegin, PCollection<Aggregate>> {

        public static Assets.ReadAggregate.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_ReadAggregate.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();

        public abstract Assets.ReadAggregate.Builder toBuilder();

        public Assets.ReadAggregate withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Assets.ReadAggregate withRequestParameters(RequestParameters params) {
            Preconditions.checkNotNull(params, "Parameters cannot be null.");
            return toBuilder().setRequestParameters(params).build();
        }

        public Assets.ReadAggregate withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Assets.ReadAggregate withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Assets.ReadAggregate withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Assets.ReadAggregate withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Aggregate> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building read assets composite transform.");

            PCollection<Aggregate> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Read results", CogniteIO.readAllAggregatesAssets()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                    );

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Assets.ReadAggregate.Builder> {
            public abstract Assets.ReadAggregate.Builder setRequestParameters(RequestParameters value);
            public abstract Assets.ReadAggregate.Builder setReaderConfig(ReaderConfig value);

            public abstract Assets.ReadAggregate build();
        }
    }

    /**
     * Transform that will read aggregate/summary statistics related to {@link Asset} objects in
     * Cognite Data Fusion.
     *
     * You specify the parameters of the aggregate(s) via a set of filters enclosed in
     * a {@link RequestParameters} object. This transform takes a collection of {@link RequestParameters}
     * as input and returns all {@link Aggregate} objects matching them.
     */
    @AutoValue
    public abstract static class ReadAllAggregate
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<Aggregate>> {

        public static Assets.ReadAllAggregate.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_ReadAllAggregate.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract Assets.ReadAllAggregate.Builder toBuilder();

        public Assets.ReadAllAggregate withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Assets.ReadAllAggregate withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Assets.ReadAllAggregate withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public Assets.ReadAllAggregate withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public Assets.ReadAllAggregate withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        @Override
        public PCollection<Aggregate> expand(PCollection<RequestParameters> input) {
            LOG.debug("Building read all asset aggregates composite transform.");

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
                                    projectConfigView, ResourceType.ASSET))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Assets.ReadAllAggregate.Builder> {
            public abstract Assets.ReadAllAggregate.Builder setReaderConfig(ReaderConfig value);
            public abstract Assets.ReadAllAggregate build();
        }
    }

    /**
     * Transform that will write {@link Asset} objects to Cognite Data Fusion.
     * <p>
     * The input objects will be batched and upserted. If the {@link Asset} object
     * does not exist, it will be created as a new object. In case the {@link Asset} already
     * exists, it will be updated with the new input.
     *
     * Writing {@link Asset} has an additional constraint that other resource types do not have.
     * An {@link Asset} must have a parent link / foreign key that either:
     * - Is {@code null}, in which case the asset will be represented as a root asset.
     * - References an existing {@link Asset} in Cognite Data Fusion.
     * - References an {@link Asset} object that is a part of the same input batch.
     *
     * This means that the input must be topologically sorted in order to secure a successful write
     * operation.
     *
     * This writer will treat all input as a single batch and performs sorting of the
     * entire input before writing. If you are writing to different {@code asset hierarchies} then you
     * can enable parallel processing of each hierarchy via the {@code WriteMultipleHierarchies} transform.
     */
    @AutoValue
    public abstract static class Write
            extends ConnectorBase<PCollection<Asset>, PCollection<Asset>> {

        public static Write.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_Write.Builder()
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

        public Write withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public Write withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public Write withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "File path cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        @Override
        public PCollection<Asset> expand(PCollection<Asset> input) {
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building write assets composite transform.");

            // main pipeline
            PCollection<Asset> outputCollection = input
                    .apply("Add static key", WithKeys.of("staticKey"))
                    .apply("Write assets", CogniteIO.writeAssetsMultipleHierarchies()
                            .withHints(getHints())
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(getWriterConfig()));

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Write.Builder setWriterConfig(WriterConfig value);
            public abstract Write build();
        }
    }

    /**
     * Transform that will write {@link Asset} objects to Cognite Data Fusion.
     * <p>
     * The input objects will be batched and upserted. If the {@link Asset} object
     * does not exist, it will be created as a new object. In case the {@link Asset} already
     * exists, it will be updated with the new input.
     *
     * Writing {@link Asset} has an additional constraint that other resource types do not have.
     * An {@link Asset} must have a parent link / foreign key that either:
     * - Is {@code null}, in which case the asset will be represented as a root asset.
     * - References an existing {@link Asset} in Cognite Data Fusion.
     * - References an {@link Asset} object that is a part of the same input batch.
     *
     * This means that the input must be topologically sorted in order to secure a successful write
     * operation.
     *
     * This writer will partition the input per key for parallelization. The input will be sorted
     * and written to Cognite Data Fusion per partition / key.
     */
    @AutoValue
    public abstract static class WriteMultipleHierarchies
            extends ConnectorBase<PCollection<KV<String, Asset>>, PCollection<Asset>> {

        public static WriteMultipleHierarchies.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_WriteMultipleHierarchies.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract WriterConfig getWriterConfig();
        public abstract WriteMultipleHierarchies.Builder toBuilder();

        public WriteMultipleHierarchies withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public WriteMultipleHierarchies withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public WriteMultipleHierarchies withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public WriteMultipleHierarchies withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public WriteMultipleHierarchies withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "File path cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        @Override
        public PCollection<Asset> expand(PCollection<KV<String, Asset>> input) {

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());


            // main pipeline
            PCollection<Asset> outputCollection = input
                    .apply("Group assets by key", GroupByKey.create())
                    .apply("Remove key", Values.create())
                    .apply("Write assets", ParDo.of(new UpsertAssetFn(getHints(), getWriterConfig(),
                            projectConfigView)).withSideInputs(projectConfigView));

            // Record successful data pipeline run
            if (null != getWriterConfig().getExtractionPipelineExtId()) {
                outputCollection
                        .apply("Report pipeline run", WritePipelineRun.<Asset>create()
                                .withProjectConfig(getProjectConfig())
                                .withProjectConfigFile(getProjectConfigFile())
                                .withWriterConfig(getWriterConfig()));
            }

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract WriteMultipleHierarchies.Builder setWriterConfig(WriterConfig value);
            public abstract WriteMultipleHierarchies build();
        }
    }

    /**
     * This writer will synchronize an input collection of {@link Asset} (representing a single hierarchy)
     * with the existing hierarchy in CDF. New asset nodes will be added, changed asset nodes will be
     * updated and deleted asset nodes will be removed.
     *
     * A hierarchy is identified by the key in the collection. The key is the externalId of the root
     * node of the hierarchy. All assets belonging to the same hierarchy must be attached to the same key.
     */
    @AutoValue
    public abstract static class SynchronizeHierarchies
            extends ConnectorBase<PCollection<KV<String, Asset>>, PCollection<Asset>> {
        private final TupleTag<Item> deletedItemsTag = new TupleTag<Item>() {};
        private final TupleTag<Asset> upsertedAssetsTag = new TupleTag<Asset>() {};

        public static SynchronizeHierarchies.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_SynchronizeHierarchies.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract WriterConfig getWriterConfig();
        public abstract SynchronizeHierarchies.Builder toBuilder();

        public SynchronizeHierarchies withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public SynchronizeHierarchies withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public SynchronizeHierarchies withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public SynchronizeHierarchies withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public SynchronizeHierarchies withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "File path cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        /**
         * Returns the tuple tag for the deleted items.
         * @return
         */
        public TupleTag<Item> getDeletedItemsTag() {
            return deletedItemsTag;
        }

        /**
         * Returns the tuple tag for the upserted assets.
         * @return
         */
        public TupleTag<Asset> getUpsertedAssetsTag() {
            return upsertedAssetsTag;
        }

        @Override
        public PCollection<Asset> expand(PCollection<KV<String, Asset>> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());


            // main pipeline
            PCollection<Asset> outputCollection = input
                    .apply("Group by hierarchy", GroupByKey.create())
                    .apply("Remove key", Values.create())
                    .apply("Synchronize hierarchies", ParDo.of(new SynchronizeAssetHierarchyFn(
                            getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView)
                    );

            // Record successful data pipeline run
            if (null != getWriterConfig().getExtractionPipelineExtId()) {
                outputCollection
                        .apply("Report pipeline run", WritePipelineRun.<Asset>create()
                                .withProjectConfig(getProjectConfig())
                                .withProjectConfigFile(getProjectConfigFile())
                                .withWriterConfig(getWriterConfig()));
            }

            return outputCollection;

            /*
            final String delimiter = "__§delimiter§__";
            final TupleTag<Asset> cdfAssetTag = new TupleTag<Asset>(){}; // input assets with composite key
            final TupleTag<Asset> inputAssetTag = new TupleTag<Asset>(){}; // cdf assets with composite key
            final TupleTag<KV<String, Asset>> upsertAssetTag = new TupleTag<KV<String, Asset>>(){}; // assets to be upserted to cdf
            final TupleTag<KV<String, Asset>> deleteAssetTag = new TupleTag<KV<String, Asset>>(){}; // assets to de deleted from cdf

            // Existing CDF root assets side inputs. One keyed to id and another keyed to externalId
            // These are used to map between internal and external ids of root assets.
            PCollection<Asset> cdfRootAssets = input.getPipeline()
                    .apply("Read root assets", CogniteIO.readAssets()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfig(getProjectConfig())
                            .withReaderConfig(ReaderConfig.create()
                                    .withAppIdentifier(getWriterConfig().getAppIdentifier())
                                    .withSessionIdentifier(getWriterConfig().getSessionIdentifier()))
                            .withRequestParameters(RequestParameters.create()
                                    .withFilterParameter("root", true)))
                    .apply("Filter on extId", Filter.by(item -> item.hasExternalId()));

            PCollectionView<Map<String, Asset>> cdfRootAssetsViewExtId = cdfRootAssets
                    .apply("Add externalId as key", WithKeys
                            .of((Asset asset) -> asset.getExternalId()))
                            .setCoder(KvCoder.of(StringUtf8Coder.of(), ProtoCoder.of(Asset.class)))
                    .apply("To map view externalId", View.asMap());

            PCollectionView<Map<Long, Asset>> cdfRootAssetsViewId = cdfRootAssets
                    .apply("Add id as key", WithKeys
                            .of((Asset asset) -> asset.getId()))
                    .setCoder(KvCoder.of(VarLongCoder.of(), ProtoCoder.of(Asset.class)))
                    .apply("To map view id", View.asMap());

            // Collect the target data sets from the input. These are used to filter the existing target
            // assets collection to synchronize with.
            PCollectionView<List<Long>> targetDataSetIds = input
                    .apply("Extract data sets", MapElements.into(TypeDescriptors.longs())
                            .via(item -> item.getValue().getDataSetId()))
                    .apply("Select distinct", Distinct.create())
                    .apply("Log id", MapElements.into(TypeDescriptors.longs())
                            .via(expression -> {
                                LOG.info("Registered dataSetId: {}", expression);
                                return expression;
                            }))
                    .apply("To list view", View.asList());

            // Read existing CDF asset hierarchies based on input data sets
            PCollection<Asset> cdfAssets = input
                    .getPipeline()
                    .apply("Read target assets", CogniteIO.readAssets()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfig(getProjectConfig())
                            .withReaderConfig(ReaderConfig.create()
                                    .withAppIdentifier(getWriterConfig().getAppIdentifier())
                                    .withSessionIdentifier(getWriterConfig().getSessionIdentifier())))
                    .apply("Filter by data set", ParDo.of(new DoFn<Asset, Asset>() {
                        @ProcessElement
                        public void processElement(@Element Asset asset,
                                                   OutputReceiver<Asset> outputReceiver,
                                                   ProcessContext context) {
                            List<Long> dataSetIds = context.sideInput(targetDataSetIds);
                            /*
                            If the data set list contains "0", then we allow assets that don't have any data set
                            assigned. Otherwise we have have to match the asset data set id to the list of
                            target data set ids.
                             */
            /*
                            if ((dataSetIds.contains(0L) && !asset.hasDataSetId())
                                    || (asset.hasDataSetId() && dataSetIds.contains(asset.getDataSetId()))) {
                                outputReceiver.output(asset);
                            }

                        }
                    }).withSideInputs(targetDataSetIds)
                    );

            // Existing CDF hierarchy input keyed to root externalId + asset externalId.
            PCollection<KV<String, Asset>> cdfAssetsWithKey = cdfAssets
                .apply("Key:rootExtId + extId", ParDo.of(new DoFn<Asset, KV<String, Asset>>() {
                    @ProcessElement
                    public void processElement(@Element Asset item,
                                               OutputReceiver<KV<String, Asset>> outputReceiver,
                                               ProcessContext context) throws Exception {
                        Map<Long, Asset> externalIdMap = context.sideInput(cdfRootAssetsViewId);

                        // build key
                        String rootExternalId = "noRootExternalId";
                        if (externalIdMap.containsKey(item.getRootId())) {
                            rootExternalId = externalIdMap.get(item.getRootId()).getExternalId();
                        }

                        outputReceiver.output(KV.of(rootExternalId + delimiter + item.getExternalId(), item));
                        }
                }).withSideInputs(cdfRootAssetsViewId));

            // Asset collection input. Build composite key of root externalId + asset externalId.
            // Check referential integrity.
            PCollection<KV<String, Asset>> inputAssets = input
                    .apply("Group by asset hierarchy", GroupByKey.create())
                    .apply("Check referential integrity", ParDo.of(new CheckAssetReferentialIntegrity()))
                    .apply("Add asset extId to key", ParDo.of(new DoFn<KV<String, Asset>, KV<String, Asset>>() {
                        @ProcessElement
                        public void processElement(@Element KV<String, Asset> item,
                                            OutputReceiver<KV<String, Asset>> outputReceiver) throws Exception {
                            outputReceiver.output(
                                    KV.of(item.getKey() + delimiter + item.getValue().getExternalId(),
                                            item.getValue()));
                        }
                    }));

            // Perform the join between CDF assets and input assets.
            PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
                    KeyedPCollectionTuple.of(cdfAssetTag, cdfAssetsWithKey)
                            .and(inputAssetTag, inputAssets)
                    .apply(CoGroupByKey.<String>create());

            // Compare input assets and CDF assets. Identify upserts and deletes. Key the assets to the root externalId
            PCollectionTuple comparisonResults = coGbkResultCollection
                    .apply("Compare input and CDF", ParDo.of(new CompareAssetCollections(cdfAssetTag,
                            inputAssetTag, upsertAssetTag, deleteAssetTag, delimiter))
                            .withOutputTags(upsertAssetTag, TupleTagList.of(deleteAssetTag)));

            // Delete assets
            PCollection<Item> deletedAssets = comparisonResults.get(deleteAssetTag)
                    .apply("Remove key", Values.create())
                    .apply("Build delete items", MapElements.into(TypeDescriptor.of(Item.class))
                            .via(asset -> Item.newBuilder()
                                    .setExternalId(asset.getExternalId())
                                    .build()))
                    .apply("Delete assets", CogniteIO.deleteAssets()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(getWriterConfig()));

            // Upsert assets
            PCollection<Asset> upsertedAssets = comparisonResults.get(upsertAssetTag)
                    //.apply("Wait for deletes to finish", Wait.on(deletedAssets)) // Triggers a bug in Beam.
                    .apply("Write hierarchies", CogniteIO.writeAssetsMultipleHierarchies()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(getWriterConfig()));

            PCollectionTuple outputCollectionTuple = PCollectionTuple.of(getUpsertedAssetsTag(), upsertedAssets)
                    .and(getDeletedItemsTag(), deletedAssets);

            return outputCollectionTuple;

             */

        }

        @AutoValue.Builder public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract SynchronizeHierarchies.Builder setWriterConfig(WriterConfig value);
            public abstract SynchronizeHierarchies build();
        }
    }

    @AutoValue
    public abstract static class Delete
            extends ConnectorBase<PCollection<Item>, PCollection<Item>> {

        public static Assets.Delete.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Assets_Delete.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }

        public abstract WriterConfig getWriterConfig();
        public abstract Assets.Delete.Builder toBuilder();

        public Assets.Delete withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public Assets.Delete withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public Assets.Delete withProjectConfigFile(ValueProvider<String> filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            return toBuilder().setProjectConfigFile(filePath).build();
        }

        public Assets.Delete withProjectConfigFile(String filePath) {
            Preconditions.checkNotNull(filePath, "File path cannot be null");
            Preconditions.checkArgument(!filePath.isEmpty(), "File path cannot be empty");
            return withProjectConfigFile(ValueProvider.StaticValueProvider.of(filePath));
        }

        public Assets.Delete withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "File path cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        @Override
        public PCollection<Item> expand(PCollection<Item> input) {
            LOG.info("Starting Cognite writer.");
            LOG.debug("Building delete assets composite transform.");

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
                            new DeleteItemsFn(getHints(), getWriterConfig(), ResourceType.ASSET, projectConfigView))
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
        public abstract static class Builder extends ConnectorBase.Builder<Assets.Delete.Builder> {
            public abstract Delete.Builder setWriterConfig(WriterConfig value);
            public abstract Assets.Delete build();
        }
    }
}