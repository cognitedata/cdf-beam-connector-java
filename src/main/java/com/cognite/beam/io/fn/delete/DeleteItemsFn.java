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

package com.cognite.beam.io.fn.delete;

import com.cognite.client.dto.Item;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.fn.ResourceType;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class DeleteItemsFn extends DoFn<Iterable<Item>, Item> {
    private static final int MAX_RETRIES = 2;

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ResourceType resourceType;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;
    private final ConnectorServiceV1 connector;
    private ConnectorServiceV1.ItemWriter itemWriter;

    public DeleteItemsFn(Hints hints, ResourceType resourceType, String appIdentifier,
                         String sessionIdentifier, PCollectionView<List<ProjectConfig>> projectConfigView) {
        Preconditions.checkNotNull(appIdentifier, "App identifier cannot be null.");
        Preconditions.checkNotNull(sessionIdentifier, "Session identifier cannot be null.");
        Preconditions.checkNotNull(hints, "Hints cannot be null");

        ImmutableList<ResourceType> validResourceType = ImmutableList.of(ResourceType.ASSET, ResourceType.EVENT,
                ResourceType.TIMESERIES_HEADER, ResourceType.FILE, ResourceType.FILE_HEADER, ResourceType.RELATIONSHIP,
                ResourceType.LABEL, ResourceType.SEQUENCE_HEADER);
        Preconditions.checkArgument(validResourceType.contains(resourceType), "Not a valid resource type");

        this.resourceType = resourceType;
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(appIdentifier)
                .setSessionIdentifier(sessionIdentifier)
                .build();
        this.projectConfigView = projectConfigView;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up DeleteItemsFn.");
        LOG.debug("Opening writer for {}", resourceType);
        switch (resourceType) {
            case ASSET:
                itemWriter = connector.deleteAssets();
                break;
            case EVENT:
                itemWriter = connector.deleteEvents();
                break;
            case TIMESERIES_HEADER:
                itemWriter = connector.deleteTsHeaders();
                break;
            case FILE_HEADER:
            case FILE:
                itemWriter = connector.deleteFiles();
                break;
            case RELATIONSHIP:
                itemWriter = connector.deleteRelationships();
                break;
            case LABEL:
                itemWriter = connector.deleteLabels();
                break;
            case SEQUENCE_HEADER:
                itemWriter = connector.deleteSequencesHeaders();
                break;
            default:
                LOG.error("Not a supported resource type: " + resourceType);
                throw new RuntimeException("Not a supported resource type: " + resourceType);
        }
    }

    @ProcessElement
    public void processElement(@Element Iterable<Item> element, OutputReceiver<Item> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "Batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";

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
        LOG.info(batchLogPrefix + "Received items to delete:{}", internalIdMap.size() + externalIdMap.size());

        ResponseItems<String> responseItems = this
                .sendRequest(internalIdMap.keySet(), externalIdMap.keySet(), projectConfig, batchLogPrefix)
                .join();
        boolean requestResult = responseItems.isSuccessful();

        int retries = 0;

        // if the request result is false, we have duplicates and/or missing items.
        while (!requestResult && retries <= MAX_RETRIES) {
            LOG.info(batchLogPrefix + "Delete items request failed. Removing duplicates and missing items and retrying the request");
            List<Item> duplicates = ItemParser.parseItems(responseItems.getDuplicateItems());
            List<Item> missing = ItemParser.parseItems(responseItems.getMissingItems());
            LOG.debug(batchLogPrefix + "No of duplicates reported: {}", duplicates.size());
            LOG.debug(batchLogPrefix + "No of missing items reported: {}", missing.size());

            for (Item value : missing) {
                if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                    externalIdMap.remove(value.getExternalId());
                } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                    internalIdMap.remove(value.getId());
                }
            }

            for (Item value : duplicates) {
                if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                    externalIdMap.remove(value.getExternalId());
                } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                    internalIdMap.remove(value.getId());
                }
            }

            // do not send empty requests
            if (internalIdMap.isEmpty() && externalIdMap.isEmpty()) {
                requestResult = true;
            } else {
                responseItems = this
                        .sendRequest(internalIdMap.keySet(), externalIdMap.keySet(), projectConfig, batchLogPrefix)
                        .join();
                requestResult = responseItems.isSuccessful();
            }
            retries++;
        }

        if (!requestResult) {
            LOG.error("Failed to delete items.", responseItems.getResponseBodyAsString());
            throw new Exception("Failed to delete items. " + responseItems.getResponseBodyAsString());
        }

        // output the deleted items (excluding duplicates and missing items)
        for (Item internalIdItem : internalIdMap.values()) {
            outputReceiver.output(internalIdItem);
        }
        for (Item externalIdItem : externalIdMap.values()) {
            outputReceiver.output(externalIdItem);
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
                .withProjectConfig(config);

        // add recursive parameters for assets
        if (resourceType == ResourceType.ASSET) {
            request = request.withRootParameter("recursive", true).withRootParameter("ignoreUnknownIds", true);
        }

        LOG.debug(batchLogPrefix + "Built delete items request for {} items", items.size());

        // post delete request and monitor for known constraint violations (duplicates, missing, etc).
        CompletableFuture<ResponseItems<String>> requestResult = itemWriter.writeItemsAsync(request);
        LOG.info(batchLogPrefix + "Delete items request sent.");

        return requestResult;
    }
}
