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

package com.cognite.beam.io.fn.context;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.dto.EntityMatch;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.util.JsonUtil;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Base class for matching a set of entities using an entity matcher ML model.
 *
 * This function will match the inbound entities with a given ML modelId. It outputs the results as
 * a {@code KV<Struct, List<EntityMatch>} where the key is the inbound entity and the value contains
 * the candidate matches.
 *
 * The entities are specified as a {@link Struct} which is the protobuf equivalent of a JSON object. Match
 * candidates are also specified as {@link Struct} within the {@link EntityMatch} container object.
 *
 */
public abstract class MatchEntitiesBaseFn extends DoFn<Iterable<Struct>, KV<Struct, List<EntityMatch>>> {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected final ObjectReader objectReader = JsonUtil.getObjectMapperInstance().reader();
    protected final ConnectorServiceV1 connector;
    protected final Hints hints;
    protected final ReaderConfig readerConfig;
    protected final PCollectionView<List<ProjectConfig>> projectConfigView;
    protected final int maxNumMatches;
    protected final double scoreThreshold;

    @Nullable
    protected final ValueProvider<Long> modelId;
    @Nullable
    protected final ValueProvider<String> modelExternalId;

    private final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");
    private final Distribution jobQueueDuration = Metrics.distribution("cognite", "jobQueueDuration");
    private final Distribution jobExecutionDuration = Metrics.distribution("cognite", "jobExecutionDuration");

    public MatchEntitiesBaseFn(Hints hints,
                               ReaderConfig readerConfig,
                               PCollectionView<List<ProjectConfig>> projectConfigView,
                               @Nullable ValueProvider<Long> modelId,
                               @Nullable ValueProvider<String> modelExternalId) {
        this(hints, readerConfig, projectConfigView, modelId, modelExternalId, 1, 0d);
    }

    public MatchEntitiesBaseFn(Hints hints,
                               ReaderConfig readerConfig,
                               PCollectionView<List<ProjectConfig>> projectConfigView,
                               @Nullable ValueProvider<Long> modelId,
                               @Nullable ValueProvider<String> modelExternalId,
                               int maxNumMatches,
                               double scoreThreshold) {
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();
        this.hints = hints;
        this.readerConfig = readerConfig;
        this.projectConfigView = projectConfigView;
        this.modelId = modelId;
        this.modelExternalId = modelExternalId;
        this.maxNumMatches = maxNumMatches;
        this.scoreThreshold = scoreThreshold;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up matchEntitiesBaseFn.");
    }

    @ProcessElement
    public void processElement(@Element Iterable<Struct> element,
                               OutputReceiver<KV<Struct, List<EntityMatch>>> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "matchEntitiesBaseFn - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        LOG.info(batchLogPrefix + "Received a batch of entities to match.");
        Instant startInstant = Instant.now();

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchLogPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // Process the entities in batches.
        List<CompletableFuture<ResponseItems<String>>> futures = new ArrayList<>();
        int elementCount = 0;
        int totalElementCount = 0;
        List<Struct> writeBatch = new ArrayList<>(hints.getContextMaxBatchSize());
        for (Struct item : element) {
            writeBatch.add(item);
            elementCount++;
            totalElementCount++;
            if (elementCount >= hints.getContextMaxBatchSize()) {
                try {
                    futures.add(this.processBatch(writeBatch, projectConfig, context, batchLogPrefix));
                    writeBatch = new ArrayList<>(hints.getContextMaxBatchSize());
                    elementCount = 0;
                } catch (Exception e) {
                    LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
                    throw e;
                }
            }
        }
        if (writeBatch.size() > 0) {
            try {
                futures.add(this.processBatch(writeBatch, projectConfig, context, batchLogPrefix));
            } catch (Exception e) {
                LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
                throw e;
            }
        }

        LOG.info(batchLogPrefix + "Submitted {} entities across {} batches to the entity matcher.",
                totalElementCount,
                futures.size());

        try {
            CompletableFuture<Void> allDoneFuture =
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));

            List<ResponseItems<String>> completed =  allDoneFuture.thenApply(v ->
                    futures.stream()
                            .map(future -> future.join())
                            .collect(Collectors.toList()))
                    .get();

            for (ResponseItems<String> result : completed) {
                if (readerConfig.isMetricsEnabled()) {
                    MetricsUtil.recordApiBatchSize(result, apiBatchSize);
                    MetricsUtil.recordApiJobQueueDuration(result, jobQueueDuration);
                    MetricsUtil.recordApiJobExecutionDuration(result, jobExecutionDuration);
                }

                for (String item : result.getResultsItems()) {
                    outputReceiver.output(KV.of(parseEntityMatcherResultItemMatchFrom(item),
                            parseEntityMatcherResultItemMatchTo(item)));
                }
            }
            LOG.info(batchLogPrefix + "Completed matching {} items within a duration of {}.",
                    totalElementCount,
                    Duration.between(startInstant, Instant.now()).toString());
        } catch (Exception e) {
            LOG.error("Error reading results from the Cognite connector.", e);
            throw e;
        }
    }

    /**
     * The main processing logic for a single request.
     *
     * @param element
     * @param config
     * @param context
     * @param batchLogPrefix
     * @return
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> processBatch(List<Struct> element,
                                                                  ProjectConfig config,
                                                                  ProcessContext context,
                                                                  String batchLogPrefix) throws Exception {
        return connector.entityMatcherPredict()
                .getItemsAsync(this.buildRequestParameters(element, context).withProjectConfig(config))
                .thenApply(responseItems -> {
                    if (!responseItems.isSuccessful()) {
                        LOG.error(batchLogPrefix + "Api job did not complete successfully. Response body: {}",
                                responseItems.getResponseBodyAsString());
                        throw new CompletionException(
                                new Throwable("Api job did not complete successfully. Response body: "
                                        + responseItems.getResponseBodyAsString()));
                    }

                    return responseItems;
                })
                ;
    }

    /**
     * Extract the [source] entry from a result item. The result item contains a single [source]
     * object that is mapped to a {@link Struct}.
     *
     * @param item
     * @return
     * @throws Exception
     */
    private Struct parseEntityMatcherResultItemMatchFrom(String item) throws Exception {
        JsonNode root = objectReader.readTree(item);

        if (root.path("source").isObject()) {
            Struct.Builder structBuilder = Struct.newBuilder();
            JsonFormat.parser().merge(root.path("source").toString(), structBuilder);
            return structBuilder.build();
        } else {
            throw new Exception("Unable to parse result item. "
                    + "Result does not contain a valid [source] node. "
                    + "Source: " + root.path("source").getNodeType());
        }
    }

    /**
     * Extract the [target] entries from a result item. The result item contains 0..N
     * [target] entries.
     *
     * @param item
     * @return
     * @throws Exception
     */
    private List<EntityMatch> parseEntityMatcherResultItemMatchTo(String item) throws Exception {
        JsonNode root = objectReader.readTree(item);
        List<EntityMatch> matches = new ArrayList<>(10);

        if (root.path("matches").isArray()) {
            for (JsonNode node : root.path("matches")) {
                if (node.isObject()) {
                    EntityMatch.Builder entityMatchBuilder = EntityMatch.newBuilder();
                    if (node.path("target").isObject()) {
                        Struct.Builder structBuilder = Struct.newBuilder();
                        JsonFormat.parser().merge(node.path("target").toString(), structBuilder);
                        entityMatchBuilder.setTarget(structBuilder.build());
                    } else {
                        throw new Exception("Unable to parse result item. "
                                + "Result does not contain a valid [target] node. "
                                + "Target: " + node.path("target").getNodeType());
                    }

                    if (node.path("score").isNumber()) {
                        entityMatchBuilder.setScore(DoubleValue.of(node.path("score").doubleValue()));
                    }
                    matches.add(entityMatchBuilder.build());
                } else {
                    throw new Exception("Unable to parse result item. "
                            + "Result does not contain a valid [matches] entry node. "
                            + "maches: " + node.getNodeType());
                }
            }
        } else {
            throw new Exception("Unable to parse result item. "
                    + "Result does not contain a valid [matches] array node. "
                    + "matches array: " + root.path("matches").getNodeType());
        }

        return matches;
    }

    /**
     * Build the request for the entity matcher.
     *
     * @param element The list of entities to try and match from.
     * @param context The {@link ProcessContext}, offering access to side inputs.
     * @return The request for the entity matcher.
     */
    protected abstract RequestParameters buildRequestParameters(List<Struct> element,
                                                     ProcessContext context);
}