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
import com.cognite.client.dto.DiagramResponse;
import com.cognite.client.dto.EntityMatch;
import com.cognite.client.dto.Item;
import com.cognite.beam.io.fn.context.CreateInteractiveDiagramsFn;
import com.cognite.beam.io.fn.context.MatchEntitiesFn;
import com.cognite.beam.io.fn.context.MatchEntitiesWithContextFn;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.RandomStringUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.beam.io.CogniteIO.invalidProjectConfigFile;

public abstract class Context {
    private static final int DEFAULT_MAX_NUM_MATCHES = 1;
    private static final int DEFAULT_WORKER_PARALLELIZATION = 10;
    private static final double DEFAULT_SCORE_THRESHOLD = 0d;

    /**
     * Just gathering common properties for all the entity matcher transforms.
     * @param <InputT>
     * @param <OutputT>
     */
    public static abstract class EntityMatcherBase<InputT extends PInput,OutputT extends POutput>
            extends ConnectorBase<InputT, OutputT> {

        public abstract ReaderConfig getReaderConfig();
        public abstract int getMaxNumMatches();
        public abstract double getScoreThreshold();
        public abstract int getWorkerParallelization();

        @Nullable
        public abstract ValueProvider<Long> getId();
        @Nullable
        public abstract ValueProvider<String> getExternalId();
        @Nullable
        public abstract PCollectionView<List<Struct>> getTargetView();

        public static abstract class Builder<B extends ConnectorBase.Builder<B>>
                extends ConnectorBase.Builder<B> {
            public abstract B setReaderConfig(ReaderConfig value);
            public abstract B setId(ValueProvider<Long> value);
            public abstract B setExternalId(ValueProvider<String> value);
            public abstract B setMaxNumMatches(int value);
            public abstract B setScoreThreshold(double value);
            public abstract B setWorkerParallelization(int value);
            public abstract B setTargetView(PCollectionView<List<Struct>> value);
        }
    }

    /**
     * Matches a set of entities (of type {@code T}) using an entity matcher model.
     *
     * This transform will match the inbound entities with a configured matcher model. You have to configure
     * this transform with 1) a function to translate from {@code T} to {@link Struct}, and 2) a matching model id.
     *
     * Example using {@link com.cognite.client.dto.Event} as the inbound entity type:
     * <pre>{@code
     * PCollection<KV<Event, List<EntityMatch>>> results = myInputPCollectionOfEvents
     *                 .apply("match events", CogniteIO.<Event>matchEntities()
     *                         .via(event ->
     *                                 Struct.newBuilder()
     *                                         .putFields("name", Value.newBuilder()
     *                                                 .setStringValue(event.getMetadataOrDefault("asset", "noAsset"))
     *                                                 .build())
     *                                         .build()
     *                                 )
     *                         .withProjectConfig(myProjectConfig)
     *                         .withExternalId(myModelExternalId)
     *                 );
     * }</pre>
     *
     * If you want to specify the {@code matchTo} collection, you can add this as a {@link PCollectionView}
     * to the transform:
     * <pre>{@code
     * PCollection<KV<Event, List<EntityMatch>>> results = myInputPCollectionOfEvents
     *                 .apply("match events", CogniteIO.<Event>matchEntities()
     *                         .via(event ->
     *                                 Struct.newBuilder()
     *                                         .putFields("name", Value.newBuilder()
     *                                                 .setStringValue(event.getMetadataOrDefault("asset", "noAsset"))
     *                                                 .build())
     *                                         .build()
     *                                 )
     *                         .withProjectConfig(myProjectConfig)
     *                         .withExternalId(myModelExternalId)
     *                         .withTargetView(myPCollectionView)
     *                 );
     * }</pre>
     *
     * @param <T>
     */
    @AutoValue
    public abstract static class MatchEntities<T>
            extends EntityMatcherBase<PCollection<T>, PCollection<KV<T, List<EntityMatch>>>> {

        private static <T> Builder<T> builder() {
            return new AutoValue_Context_MatchEntities.Builder<T>()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setMaxNumMatches(DEFAULT_MAX_NUM_MATCHES)
                    .setWorkerParallelization(DEFAULT_WORKER_PARALLELIZATION)
                    .setScoreThreshold(DEFAULT_SCORE_THRESHOLD)
                    ;
        }

        public static <T> MatchEntities<T> create() {
            return MatchEntities.<T>builder()
                    .build();
        }

        public abstract Builder<T> toBuilder();

        @Nullable
        public abstract SerializableFunction<T, Struct> getSerializableFunction();

        public MatchEntities<T> withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public MatchEntities<T> withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        @Deprecated
        public MatchEntities<T> withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        @Deprecated
        public MatchEntities<T> withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public MatchEntities<T> withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public MatchEntities<T> withId(ValueProvider<Long> modelId) {
            return toBuilder().setId(modelId).build();
        }
        public MatchEntities<T> withId(long modelId) {
            return withId(ValueProvider.StaticValueProvider.of(modelId));
        }

        public MatchEntities<T> withExternalId(ValueProvider<String> modelExternalId) {
            return toBuilder().setExternalId(modelExternalId).build();
        }
        public MatchEntities<T> withExternalId(String modelExternalId) {
            return withExternalId(ValueProvider.StaticValueProvider.of(modelExternalId));
        }

        public MatchEntities<T> withMaxNumMatches(int maxNumMatches) {
            return toBuilder().setMaxNumMatches(maxNumMatches).build();
        }

        public MatchEntities<T> withScoreThreshold(double threshold) {
            return toBuilder().setScoreThreshold(threshold).build();
        }

        public MatchEntities<T> withWorkerParallelization(int parallelization) {
            return toBuilder().setWorkerParallelization(parallelization).build();
        }

        public MatchEntities<T> withTargetView(@Nullable PCollectionView<List<Struct>> targetView) {
            return toBuilder().setTargetView(targetView).build();
        }

        public MatchEntities<T> via(SerializableFunction<T, Struct> function) {
            return toBuilder().setSerializableFunction(function).build();
        }

        /**
         * Performs the matching of entities with a given matching model:
         * 1. Build join key for the input.
         *  - Based on 1) translating input to Struct and 2) hashing the Struct
         * 2. Map input to struct and run it through the entity matcher
         * 3. Join the match result with the original input object.
         *
         * @param input
         * @return
         */
        @Override
        public PCollection<KV<T, List<EntityMatch>>> expand(PCollection<T> input) {
            // validate required config parameters
            Preconditions.checkState(getId() != null || getExternalId() != null,
                    "No matching model specified. You must configure a matching model using [.withId(<id>)] "
                            + " or [.withExternalId(<externalId>)].");
            Preconditions.checkNotNull(getSerializableFunction(),
                    "No process function. You must specify a process function using [.via()].");

            // Tags and attributes for joining the input with the entity match results.
            final TupleTag<T> inputTag = new TupleTag<>();
            final TupleTag<List<EntityMatch>> matchedTag = new TupleTag<>();
            final String idFieldName = "tempJoinUuid_" + RandomStringUtils.randomAlphanumeric(5);
            final Value defaultIntValue = Value.newBuilder().setNumberValue(-1d).build();
            final KvCoder<Integer, T> inputKvCoder = KvCoder.of(VarIntCoder.of(), input.getCoder());
            final KvCoder<T, List<EntityMatch>> outputKvCoder =
                    KvCoder.of(input.getCoder(), ListCoder.of(ProtoCoder.of(EntityMatch.class)));

            /*
            Build a keyed collection of the input so we can map the Struct back to their original type.
            In order to enable caching we cannot apply a UUID to each input, so we hash the matcher Struct.
            I.e. every input that maps to the same (value) Struct will get the same key.
             */
            PCollection<KV<Integer, T>> keyedInput = input
                    .apply("Build join key", WithKeys.<Integer, T>of(element ->
                            getSerializableFunction().apply(element).hashCode()
                    )).setCoder(inputKvCoder);

            // Map the input to Struct and run the entity matcher.
            PCollection<KV<Integer, List<EntityMatch>>> matchedEntities = keyedInput
                    .apply("Map to struct", MapElements
                            .into(TypeDescriptor.of(Struct.class))
                            .via((KV<Integer, T> element) ->
                                    getSerializableFunction().apply(element.getValue()).toBuilder()
                                            .putFields(idFieldName, Value.newBuilder()
                                                    .setNumberValue(element.getKey().doubleValue())
                                                    .build())
                                            .build()))
                    .apply("Match entities", CogniteIO.matchStructEntities()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig())
                            .withHints(getHints())
                            .withId(getId())
                            .withExternalId(getExternalId())
                            .withMaxNumMatches(getMaxNumMatches())
                            .withWorkerParallelization(getWorkerParallelization())
                            .withScoreThreshold(getScoreThreshold())
                            .withTargetView(getTargetView()))
                    .apply("Build joinKey", MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.integers(),
                                    TypeDescriptors.lists(TypeDescriptor.of(EntityMatch.class))))
                            .via((KV<Struct, List<EntityMatch>> element) ->
                                    KV.of((int) element.getKey().getFieldsOrDefault(idFieldName, defaultIntValue).getNumberValue(),
                                            element.getValue())));

            // Join the output from the entity matcher with the keyed input.
            // Will give us back the original input object.
            PCollection<KV<Integer, CoGbkResult>> joinResults = KeyedPCollectionTuple
                    .of(inputTag, keyedInput)
                    .and(matchedTag, matchedEntities)
                    .apply("Join", CoGroupByKey.create());

            PCollection<KV<T, List<EntityMatch>>> outputCollection = joinResults
                    .apply(ParDo.of(
                            new DoFn<KV<Integer, CoGbkResult>, KV<T, List<EntityMatch>>>() {
                                final String instanceId = RandomStringUtils.randomAlphanumeric(5);

                                @ProcessElement
                                public void processElement(@Element KV<Integer, CoGbkResult> element,
                                                           OutputReceiver<KV<T, List<EntityMatch>>> out) {
                                    String loggingPrefix = String.format("Instance [%s] - batch [%s] - ",
                                            instanceId,
                                            RandomStringUtils.randomAlphanumeric(4));
                                    ImmutableList<T> inputItems =
                                            ImmutableList.copyOf(element.getValue().getAll(inputTag));
                                    ImmutableList<List<EntityMatch>> matches =
                                            ImmutableList.copyOf(element.getValue().getAll(matchedTag));

                                    if (inputItems.isEmpty()) {
                                        LOG.warn(loggingPrefix + "No input entity found for join key {}. Should be investigated."
                                                + " Number of match candidates {}.",
                                                element.getKey(),
                                                matches.size());
                                        return;
                                    }

                                    // Check that all match lists are equal. They should be since they are produced
                                    // by the same input.
                                    boolean allMatchesEqual = true;
                                    for (List<EntityMatch> matchResult : matches) {
                                        allMatchesEqual = allMatchesEqual && matchResult.equals(matches.get(0));
                                    }
                                    if (!allMatchesEqual) {
                                        LOG.warn(loggingPrefix + "Identified different matches for the same entity input."
                                                + " Input entity: {}",
                                                inputItems.get(0));
                                    }

                                    if (inputItems.size() != matches.size()) {
                                        // Something went wrong. There should be equal number of object from all inputs.
                                        LOG.warn(loggingPrefix + "Identified inequality from the inputs for join key {}."
                                                + " Number of items: {}, number of match lists: {}."
                                                + " Input entity: {}",
                                                element.getKey(),
                                                inputItems.size(),
                                                matches.size(),
                                                inputItems.get(0));
                                    }

                                    // in case of 0 match candidate lists we build an empty list
                                    ImmutableList<EntityMatch> matchesList = ImmutableList.of();
                                    if (!matches.isEmpty()) {
                                        matchesList = ImmutableList.copyOf(matches.get(0));
                                    }

                                    for (T item : inputItems) {
                                            out.output(KV.of(item, matches.get(0)));
                                    }
                                }
                            })).setCoder(outputKvCoder);

            return outputCollection;
        }

        @AutoValue.Builder public abstract static class Builder<T> extends EntityMatcherBase.Builder<Builder<T>> {
            abstract Builder<T> setSerializableFunction(SerializableFunction<T, Struct> value);

            abstract MatchEntities<T> build();
        }
    }

    /**
     * Matches a set of entities (of type {@code Struct}) using an entity matcher model.
     *
     * This transform will match the inbound entities with a configured matcher model. You have to configure
     * this transform with 1) a matching model id.
     *
     * Example:
     * <pre>{@code
     * PCollection<KV<Struct, List<EntityMatch>>> results = myInputPCollectionOfStructs
     *                 .apply("match events", CogniteIO.matchStructEntities()
     *                         .withProjectConfig(myProjectConfig)
     *                         .withExternalId(myModelExternalId)
     *                 );
     * }</pre>
     *
     * If you want to specify the {@code matchTo} collection, you can add this as a {@link PCollectionView}
     * to the transform:
     * <pre>{@code
     * PCollection<KV<Struct, List<EntityMatch>>> results = myInputPCollectionOfStructs
     *                 .apply("match events", CogniteIO.<Event>matchStructEntities()
     *                         .withProjectConfig(myProjectConfig)
     *                         .withExternalId(myModelExternalId)
     *                         .withTargetView(myPCollectionView)
     *                 );
     * }</pre>
     */
    @AutoValue
    public abstract static class MatchStructEntities
            extends EntityMatcherBase<PCollection<Struct>, PCollection<KV<Struct, List<EntityMatch>>>> {

        private static Builder builder() {
            return new AutoValue_Context_MatchStructEntities.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setMaxNumMatches(DEFAULT_MAX_NUM_MATCHES)
                    .setWorkerParallelization(DEFAULT_WORKER_PARALLELIZATION)
                    .setScoreThreshold(DEFAULT_SCORE_THRESHOLD)
                    ;
        }

        public static MatchStructEntities create() {
            return MatchStructEntities.builder()
                    .build();
        }

        public abstract Builder toBuilder();

        public MatchStructEntities withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public MatchStructEntities withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        @Deprecated
        public MatchStructEntities withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        @Deprecated
        public MatchStructEntities withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public MatchStructEntities withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public MatchStructEntities withId(ValueProvider<Long> modelId) {
            return toBuilder().setId(modelId).build();
        }
        public MatchStructEntities withId(long modelId) {
            return withId(ValueProvider.StaticValueProvider.of(modelId));
        }

        public MatchStructEntities withExternalId(ValueProvider<String> modelExternalId) {
            return toBuilder().setExternalId(modelExternalId).build();
        }
        public MatchStructEntities withExternalId(String modelExternalId) {
            return withExternalId(ValueProvider.StaticValueProvider.of(modelExternalId));
        }

        public MatchStructEntities withMaxNumMatches(int maxNumMatches) {
            return toBuilder().setMaxNumMatches(maxNumMatches).build();
        }

        public MatchStructEntities withScoreThreshold(double threshold) {
            return toBuilder().setScoreThreshold(threshold).build();
        }

        public MatchStructEntities withWorkerParallelization(int parallelization) {
            return toBuilder().setWorkerParallelization(parallelization).build();
        }

        public MatchStructEntities withTargetView(@Nullable PCollectionView<List<Struct>> targetView) {
            return toBuilder().setTargetView(targetView).build();
        }

        @Override
        public PCollection<KV<Struct, List<EntityMatch>>> expand(PCollection<Struct> input) {
            // validate required config parameters
                Preconditions.checkState(getId() != null || getExternalId() != null,
                        "No matching model specified. You must configure a matching model using [.withId(<id>)] "
                                + " or [.withExternalId(<externalId>)].");

            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<Struct> structCoder = ProtoCoder.of(Struct.class);
            KvCoder<String, Struct> keyValueCoder = KvCoder.of(utf8Coder, structCoder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<Iterable<Struct>> entityBatch = input
                    .apply("Shard items", WithKeys.of((Struct inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, Struct>of(keyValueCoder)
                            .withMaxBatchSize(getHints().getContextMaxBatchSize() * getWorkerParallelization())
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<Struct>>create());

            PCollection<KV<Struct, List<EntityMatch>>> outputCollection;
            if (null != getTargetView()) {
                // We have a [target] specification. Use the targeted entity matcher instead.
                LOG.info("[target] is specified. Will use the entity matcher with context");
                outputCollection = entityBatch
                        .apply("Match entities w/target", ParDo.of(new MatchEntitiesWithContextFn(getHints(),
                                getReaderConfig(), projectConfigView, getId(), getExternalId(),
                                getMaxNumMatches(), getScoreThreshold(), getTargetView()))
                                .withSideInputs(projectConfigView, getTargetView()));
            } else {
                // No [target] is specified
                LOG.info("No [target] specification. Will use the entity matcher without context.");
                outputCollection = entityBatch
                        .apply("Match entities", ParDo.of(new MatchEntitiesFn(getHints(), getReaderConfig(),
                                projectConfigView, getId(), getExternalId(), getMaxNumMatches(), getScoreThreshold()))
                                .withSideInputs(projectConfigView));
            }

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends EntityMatcherBase.Builder<Builder> {
            abstract MatchStructEntities build();
        }
    }

    /**
     * Detects annotations and builds interactive engineering diagrams/P&IDs (svg and png) from PDF files.
     *
     * The function detects entities (for example, assets) in the diagram and highlights them in the (optional)
     * SVG/PNG. The detected entities can be used to enrich the viewer experience in an application displaying
     * the diagram/P&ID.
     *
     * Annotations are detected based on a side input of {@link List} of {@link Struct}. {@code Struct.name} is used
     * for annotation matching.
     *
     * The input specifies which file (id) to process.
     */
    @AutoValue
    public abstract static class CreateInteractiveDiagram
            extends ConnectorBase<PCollection<Item>, PCollection<DiagramResponse>> {
        private static final String DEFAULT_SEARCH_FIELD = "name";
        private static final boolean DEFAULT_PARTIAL_MATCH = false;
        private static final boolean DEFAULT_CONVERT_FILE = false;
        private static final int DEFAULT_MIN_TOKENS = 2;
        private static final boolean DEFAULT_ENABLE_GRAYSCALE = false;

        private static Builder builder() {
            return new AutoValue_Context_CreateInteractiveDiagram.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setSearchField(DEFAULT_SEARCH_FIELD)
                    .setConvertFile(DEFAULT_CONVERT_FILE)
                    .setWorkerParallelization(DEFAULT_WORKER_PARALLELIZATION)
                    .setPartialMatch(DEFAULT_PARTIAL_MATCH)
                    .setMinTokens(DEFAULT_MIN_TOKENS)
                    .setGrayscale(DEFAULT_ENABLE_GRAYSCALE);
        }

        public static CreateInteractiveDiagram create() {
            return CreateInteractiveDiagram.builder()
                    .build();
        }

        public abstract Builder toBuilder();
        public abstract ReaderConfig getReaderConfig();
        public abstract String getSearchField();
        public abstract boolean isConvertFile();
        public abstract int getWorkerParallelization();
        public abstract boolean isPartialMatch();
        public abstract int getMinTokens();
        public abstract boolean isGrayscale();

        @Nullable
        public abstract PCollectionView<List<Struct>> getEntitiesView();

        public CreateInteractiveDiagram withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public CreateInteractiveDiagram withHints(Hints hints) {
            return toBuilder().setHints(hints).build();
        }

        @Deprecated
        public CreateInteractiveDiagram withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        @Deprecated
        public CreateInteractiveDiagram withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public CreateInteractiveDiagram withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        /**
         * This field determines the string to search for and to identify object entities.
         *
         * The default is {@code "name"}.
         *
         * @param searchField The name of the search field.
         * @return transform with the configuration applied.
         */
        public CreateInteractiveDiagram withSearchField(String searchField) {
            Preconditions.checkNotNull(searchField, "Search field cannot be null");
            Preconditions.checkArgument(!searchField.isEmpty(), "Search field cannot be an empty string.");
            return toBuilder().setSearchField(searchField).build();
        }

        /**
         * Set to {@code true} to include an interactive SVG (and PNG) version of the diagram in the result.
         *
         * The default is {@code false}.
         *
         * @param convertFile Set to true to include SVG version.
         * @return transform with the configuration applied.
         */
        public CreateInteractiveDiagram enableConvertFile(boolean convertFile) {
            return toBuilder().setConvertFile(convertFile).build();
        }

        /**
         * Set to {@code true} to reduce the size of the SVG (and PNG) by only using grayscale image.
         *
         * The default is {@code false}.
         *
         * @param grayscale Set to true to enable grayscale.
         * @return transform with the configuration applied.
         */
        public CreateInteractiveDiagram enableGrayscale(boolean grayscale) {
            return toBuilder().setGrayscale(grayscale).build();
        }

        /**
         * Allow partial (fuzzy) matching of entities in the engineering diagrams.
         * Creates a match only when it is possible to do so unambiguously.
         *
         * The default is {@code false}.
         *
         * @param partialMatch Set to true to enable partial matching.
         * @return transform with the configuration applied.
         */
        public CreateInteractiveDiagram enablePartialMatch(boolean partialMatch) {
            return toBuilder().setPartialMatch(partialMatch).build();
        }

        /**
         * Each detected item must match the detected entity on at least this number of tokens.
         * A token is a substring of consecutive letters or digits.
         *
         * The default is {@code 2}.
         *
         * @param minTokens The minimum number of tokens.
         * @return transform with the configuration applied.
         */
        public CreateInteractiveDiagram withMinTokens(int minTokens) {
            return toBuilder().setMinTokens(minTokens).build();
        }

        /**
         * Set the maximum number of parallel requests per worker.
         *
         * The default is {@code 10}.
         *
         * @param parallelization Max number of parallel requests per worker.
         * @return transform with the configuration applied.
         */
        public CreateInteractiveDiagram withWorkerParallelization(int parallelization) {
            return toBuilder().setWorkerParallelization(parallelization).build();
        }

        /**
         * A list of entities to look for. For example, all the assets under a root node.
         * The {@code searchField} determines the strings that identify the entities.
         *
         * @param entities The list of entities.
         * @return transform with the configuration applied.
         */
        public CreateInteractiveDiagram withEntitiesView(PCollectionView<List<Struct>> entities) {
            return toBuilder().setEntitiesView(entities).build();
        }

        @Override
        public PCollection<DiagramResponse> expand(PCollection<Item> input) {
            // validate required config parameters
            Preconditions.checkState(null != getEntitiesView(),
                    "No entities view. You must configure a entities view using [.withEntitiesView(<view>)].");

            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<Item> itemCoder = ProtoCoder.of(Item.class);
            KvCoder<String, Item> keyValueCoder = KvCoder.of(utf8Coder, itemCoder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<DiagramResponse> outputCollection = input
                    .apply("Shard items", WithKeys.of((Item inputItem) ->
                            String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, Item>of(keyValueCoder)
                            .withMaxBatchSize(getWorkerParallelization())
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<Item>>create())
                    .apply("Create int. diagram", ParDo.of(new CreateInteractiveDiagramsFn(getHints(), getReaderConfig(),
                            projectConfigView, getEntitiesView(), getSearchField(), isConvertFile(),
                            isPartialMatch(), getMinTokens(), isGrayscale()))
                    .withSideInputs(projectConfigView, getEntitiesView()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder
                extends ConnectorBase.Builder<CreateInteractiveDiagram.Builder> {
            abstract Builder setReaderConfig(ReaderConfig value);
            abstract Builder setSearchField(String value);
            abstract Builder setConvertFile(boolean value);
            abstract Builder setGrayscale(boolean value);
            abstract Builder setWorkerParallelization(int value);
            abstract Builder setPartialMatch(boolean value);
            abstract Builder setMinTokens(int value);
            abstract Builder setEntitiesView(PCollectionView<List<Struct>> value);

            abstract CreateInteractiveDiagram build();
        }
    }
}
