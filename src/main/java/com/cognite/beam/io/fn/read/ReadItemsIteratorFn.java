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

package com.cognite.beam.io.fn.read;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.servicesV1.ConnectorConstants;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.cognite.beam.io.servicesV1.parser.ItemParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.fn.ResourceType;

/**
 * Reads a collection of items from Cognite. This function will iterate over potentially large results
 * sets and stream the results in an iterative fashion.
 *
 * This function is normally used to read results collections from the various filter endpoints. I.e.
 * {@code filter events}, {@code filter assets}, etc.
 */
public class ReadItemsIteratorFn extends DoFn<RequestParameters, String> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ResourceType resourceType;
    private final ConnectorServiceV1 connector;
    private final boolean isStreaming;
    private final boolean isMetricsEnabled;
    private final List<ResourceType> streamingSupported = ImmutableList.of(ResourceType.ASSET,
            ResourceType.EVENT, ResourceType.FILE_HEADER, ResourceType.TIMESERIES_HEADER);

    private final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    private final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");
    private final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    public ReadItemsIteratorFn(Hints hints, ResourceType resourceType, ReaderConfig readerConfig) {
        this.resourceType = resourceType;
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();

        isStreaming = readerConfig.isStreamingEnabled();
        isMetricsEnabled = readerConfig.isMetricsEnabled();
        Preconditions.checkArgument(!isStreaming || streamingSupported.contains(resourceType),
                "Streaming mode is not supported for resource type: " + resourceType);
    }

    @Setup
    public void setup() {
        LOG.info("Setting up ReadItemsIteratorFn.");
    }

    @ProcessElement
    public void processElement(@Element RequestParameters query,
                               OutputReceiver<String> outputReceiver) throws Exception {
        final String batchLogPrefix = "ReadItemsIteratorFn - batch: "
                + RandomStringUtils.randomAlphanumeric(6) + " - ";
        LOG.debug(batchLogPrefix + "Sending query to the Cognite data platform: {}", query.toString());

        try {
            Iterator<CompletableFuture<ResponseItems<String>>> results;
            switch (resourceType) {
                case ASSET:
                    results = connector.readAssets(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)));
                    break;
                case EVENT:
                    results = connector.readEvents(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)));
                    break;
                case SEQUENCE_HEADER:
                    results = connector.readSequencesHeaders(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)));
                    break;
                case SEQUENCE_BODY:
                    results = connector.readSequencesRows(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_SEQUENCES_ROWS)));
                    break;
                case TIMESERIES_HEADER:
                    results = connector.readTsHeaders(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)));
                    break;
                case FILE_HEADER:
                    results = connector.readFileHeaders(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)));
                    break;
                case DATA_SET:
                    results = connector.readDataSets(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)));
                    break;
                case RELATIONSHIP:
                    results = connector.readRelationships(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)));
                    break;
                case LABEL:
                    results = connector.readLabels(query
                            .withRootParameter("limit", query.getRequestParameters()
                                    .getOrDefault("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)));
                    break;
                /*case THREED_MODEL_HEADER:
                    results = connector.read3dModels(query);
                    break; */
                default:
                    LOG.error(batchLogPrefix + "Not a supported resource type: " + resourceType);
                    throw new Exception(batchLogPrefix + "Not a supported resource type: " + resourceType);
            }

            CompletableFuture<ResponseItems<String>> responseItemsFuture;
            ResponseItems<String> responseItems;

            while (results.hasNext()) {
                responseItemsFuture = results.next();
                responseItems = responseItemsFuture.join();
                Optional<Long> lastUpdatedTime = Optional.empty();

                if (!responseItems.isSuccessful()) {
                    // something went wrong with the request
                    String message = batchLogPrefix + "Error while iterating through the results from Fusion: "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }

                if (isMetricsEnabled) {
                    MetricsUtil.recordApiLatency(responseItems, apiLatency);
                    MetricsUtil.recordApiBatchSize(responseItems, apiBatchSize);
                    MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                }

                if (isStreaming) {
                    for (String item : responseItems.getResultsItems()) {
                        lastUpdatedTime = ItemParser.parseLong(item, "lastUpdatedTime");
                        outputReceiver.outputWithTimestamp(item,
                                org.joda.time.Instant.ofEpochMilli(lastUpdatedTime.orElse(0l)));
                    }
                } else {
                    for (String item : responseItems.getResultsItems()) {
                        outputReceiver.output(item);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
