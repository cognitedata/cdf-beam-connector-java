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

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.dto.Item;
import com.cognite.beam.io.fn.ResourceType;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Reads the results items from a read request to Cognite. This function will read the response results from
 * a single request and send them to the output. It will not iterate over large results collections
 * that require multiple request. If you need that functionality, please refer to {@link ReadItemsIteratorFn}.
 *
 * This function is normally used to read results from the various read endpoints that provide all results
 * in a single response, such as the {@code by id} endpoints.
 */
public class ReadItemsByIdFn extends DoFn<Iterable<Item>, String> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ResourceType resourceType;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;
    private final ConnectorServiceV1 connector;
    private final boolean isMetricsEnabled;
    private ItemReader<String> itemReader;

    private final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    private final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");
    private final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    public ReadItemsByIdFn(Hints hints,
                           ResourceType resourceType,
                           ReaderConfig readerConfig,
                           PCollectionView<List<ProjectConfig>> projectConfigView) {
        this.resourceType = resourceType;
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();
        this.projectConfigView = projectConfigView;

        isMetricsEnabled = readerConfig.isMetricsEnabled();
    }

    @Setup
    public void setup() {
        LOG.info("Setting up ReadItemsByIdFn.");
        switch (resourceType) {
            case ASSETS_BY_ID:
                itemReader = connector.readAssetsById();
                break;
            case EVENT_BY_ID:
                itemReader = connector.readEventsById();
                break;
            case SEQUENCE_BY_ID:
                itemReader = connector.readSequencesById();
                break;
            case FILE_BY_ID:
                itemReader = connector.readFilesById();
                break;
            case TIMESERIES_BY_ID:
                itemReader = connector.readTsById();
                break;
            case RELATIONSHIP_BY_ID:
                itemReader = connector.readRelationshipsById();
                break;
            default:
                LOG.error("Not a supported resource type: " + resourceType);
                throw new RuntimeException("Not a supported resource type: " + resourceType);
        }
    }

    @ProcessElement
    public void processElement(@Element Iterable<Item> element,
                               OutputReceiver<String> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "ReadItemsByIdFn - batch: "
                + RandomStringUtils.randomAlphanumeric(6) + " - ";

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchLogPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // check that ids are provided + remove duplicate ids
        Map<Long, Item> internalIdMap = new HashMap<>(1000);
        Map<String, Item> externalIdMap = new HashMap<>(1000);
        for (Item value : element) {
            if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                externalIdMap.put(value.getExternalId(), value);
            } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                internalIdMap.put(value.getId(), value);
            } else {
                String message = batchLogPrefix + "Item does not contain id nor externalId: " + value.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }
        LOG.info(batchLogPrefix + "Received items to read:{}", internalIdMap.size() + externalIdMap.size());
        if (internalIdMap.isEmpty() && externalIdMap.isEmpty()) {
            // should not happen, but need to safeguard against it
            LOG.warn(batchLogPrefix + "Empty input. Will skip the read process.");
            return;
        }

        ResponseItems<String> responseItems = this
                .sendRequest(internalIdMap.keySet(), externalIdMap.keySet(), projectConfig, batchLogPrefix)
                .join();

        if (!responseItems.isSuccessful()) {
            // something went wrong with the request
            String message = batchLogPrefix + "Error while reading the results from Fusion: "
                    + responseItems.getResponseBodyAsString();
            LOG.error(message);
            throw new Exception(message);
        }

        if (isMetricsEnabled) {
            MetricsUtil.recordApiLatency(responseItems, apiLatency);
            MetricsUtil.recordApiBatchSize(responseItems, apiBatchSize);
            MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
        }

        for (String item : responseItems.getResultsItems()) {
            outputReceiver.output(item);
        }
    }

    private CompletableFuture<ResponseItems<String>> sendRequest(Collection<Long> internalIds,
                                                                 Collection<String> externalIds,
                                                                 ProjectConfig config,
                                                                 String batchLogPrefix) throws Exception {
        Preconditions.checkArgument(!internalIds.isEmpty() || !externalIds.isEmpty(),
                "The request does not have any items.");

        // build initial request object
        List<Map<String, Object>> items = new ArrayList<>();
        for (Long internalId : internalIds) {
            items.add(ImmutableMap.of("id", internalId));
        }
        for (String externalId : externalIds) {
            items.add(ImmutableMap.of("externalId", externalId));
        }

        RequestParameters request = RequestParameters.create()
                .withItems(items)
                .withRootParameter("ignoreUnknownIds", true)
                .withProjectConfig(config);


        LOG.debug(batchLogPrefix + "Built read items request for {} items", items.size());

        // post delete request and monitor for known constraint violations (duplicates, missing, etc).
        CompletableFuture<ResponseItems<String>> requestResult = itemReader.getItemsAsync(request);
        LOG.info(batchLogPrefix + "Read items request sent.");

        return requestResult;
    }
}
