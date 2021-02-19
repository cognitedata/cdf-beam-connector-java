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

package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.dto.EntityMatchResult;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.EntityMatchingParser;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * This class represents the Cognite entity matching api endpoint
 *
 * It provides methods for interacting with the entity matching services.
 */
@AutoValue
public abstract class EntityMatching extends ApiBase {

    private static Builder builder() {
        return new AutoValue_EntityMatching.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(EntityMatching.class);

    /**
     * Construct a new {@link EntityMatching} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static EntityMatching of(CogniteClient client) {
        return EntityMatching.builder()
                .setClient(client)
                .build();
    }

    public List<EntityMatchResult> predict(String modelExternalId,
                                           List<Struct> sources,
                                           List<Struct> targets) throws Exception {
        return predict(modelExternalId, sources, targets, 1);
    }

    public List<EntityMatchResult> predict(String modelExternalId,
                                           List<Struct> sources,
                                           List<Struct> targets,
                                           int numMatches) throws Exception {
        return predict(modelExternalId, sources, targets, numMatches, 0d);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * @param modelExternalId
     * @param sources
     * @param targets
     * @param numMatches
     * @return
     * @throws Exception
     */
    public List<EntityMatchResult> predict(String modelExternalId,
                                           List<Struct> sources,
                                           List<Struct> targets,
                                           int numMatches,
                                           double scoreThreshold) throws Exception {
        final String loggingPrefix = "predict() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(modelExternalId, loggingPrefix + "Model external id cannot be null.");
        Preconditions.checkNotNull(sources, loggingPrefix + "Source cannot be null.");
        Preconditions.checkNotNull(targets, loggingPrefix + "Target cannot be null.");

        LOG.debug(loggingPrefix + "Received {} source entities to match with {} target entities.",
                sources.size(),
                targets.size());

        // Build the baseline request.
        RequestParameters request = RequestParameters.create()
                .withRootParameter("externalId", modelExternalId)
                .withRootParameter("numMatches", numMatches)
                .withRootParameter("scoreThreshold", scoreThreshold);

        // Add targets if any
        if (targets.size() > 0) {
            request = request.withRootParameter("targets", targets);
        }

        List<RequestParameters> requestBatches = new ArrayList<>();
        // Batch the source entities if any
        if (sources.size() > 0) {
            List<List<Struct>> sourceBatches =
                    Partition.ofSize(sources, getClient().getClientConfig().getEntityMatchingMaxBatchSize());
            for (List<Struct> sourceBatch : sourceBatches) {
                requestBatches.add(request.withRootParameter("sources", sourceBatch));
            }
        } else {
            requestBatches.add(request);
        }

        return predict(requestBatches);
    }

    public List<EntityMatchResult> predict(long modelId,
                                           List<Struct> sources,
                                           List<Struct> targets) throws Exception {
        return predict(modelId, sources, targets, 1);
    }

    public List<EntityMatchResult> predict(long modelId,
                                           List<Struct> sources,
                                           List<Struct> targets,
                                           int numMatches) throws Exception {
        return predict(modelId, sources, targets, numMatches, 0d);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * @param modelId
     * @param sources
     * @param targets
     * @param numMatches
     * @return
     * @throws Exception
     */
    public List<EntityMatchResult> predict(long modelId,
                                           List<Struct> sources,
                                           List<Struct> targets,
                                           int numMatches,
                                           double scoreThreshold) throws Exception {
        final String loggingPrefix = "predict() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(sources, loggingPrefix + "Source cannot be null.");
        Preconditions.checkNotNull(targets, loggingPrefix + "Target cannot be null.");

        LOG.debug(loggingPrefix + "Received {} source entities to match with {} target entities.",
                sources.size(),
                targets.size());

        // Build the baseline request.
        RequestParameters request = RequestParameters.create()
                .withRootParameter("id", modelId)
                .withRootParameter("numMatches", numMatches)
                .withRootParameter("scoreThreshold", scoreThreshold);

        // Add targets if any
        if (targets.size() > 0) {
            request = request.withRootParameter("targets", targets);
        }

        List<RequestParameters> requestBatches = new ArrayList<>();
        // Batch the source entities if any
        if (sources.size() > 0) {
            List<List<Struct>> sourceBatches =
                    Partition.ofSize(sources, getClient().getClientConfig().getEntityMatchingMaxBatchSize());
            for (List<Struct> sourceBatch : sourceBatches) {
                requestBatches.add(request.withRootParameter("sources", sourceBatch));
            }
        } else {
            requestBatches.add(request);
        }

        return predict(requestBatches);
    }

    /**
     * Matches a set of source entities with a set of targets via a given matching model.
     *
     * All input parameters are provided via the request object.
     * @param requests input parameters for the predict jobs.
     * @return The entity match results.
     * @throws Exception
     */
    public List<EntityMatchResult> predict(List<RequestParameters> requests) throws Exception {
        final String loggingPrefix = "predict() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(requests, loggingPrefix + "Requests cannot be null.");
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} entity matcher requests.",
                requests.size());

        if (requests.isEmpty()) {
            LOG.info(loggingPrefix + "No items specified in the request. Will skip the predict request.");
            return Collections.emptyList();
        }

        ItemReader<String> entityMatcher = getClient().getConnectorService().entityMatcherPredict();

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        for (RequestParameters request : requests) {
            resultFutures.add(entityMatcher.getItemsAsync(addAuthInfo(request)));
        }
        LOG.info(loggingPrefix + "Submitted {} entity matching jobs within a duration of {}.",
                requests.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(resultFutures.toArray(
                new CompletableFuture[resultFutures.size()]));
        // Wait until the uber future completes.
        allFutures.join();

        // Collect the response items
        List<String> responseItems = new ArrayList<>();
        for (CompletableFuture<ResponseItems<String>> responseItemsFuture : resultFutures) {
            if (!responseItemsFuture.join().isSuccessful()) {
                // something went wrong with the request
                String message = loggingPrefix + "Entity matching job failed: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItemsFuture.join().getResultsItems().forEach(result -> responseItems.add(result));
        }

        LOG.info(loggingPrefix + "Completed matching {} entities across {} matching jobs within a duration of {}.",
                responseItems.size(),
                requests.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return responseItems.stream()
                .map(this::parseEntityMatchResult)
                .collect(Collectors.toList());
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private EntityMatchResult parseEntityMatchResult(String json) {
        try {
            return EntityMatchingParser.parseEntityMatchResult(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract EntityMatching build();
    }
}
