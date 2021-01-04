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

package com.cognite.beam.io.fn.write;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Function for upserting {@code T} to CDF. {@code T} requires special handling for upserts
 * so we cannot use the standard logic in {@link UpsertItemBaseFn}.
 *
 * This function will first try to write the items as new items. In case the items already exists (based on externalId),
 * the item will be deleted and then created. Effectively this results in an upsert.
 *
 */
public abstract class UpsertItemViaDeleteBaseFn<T> extends DoFn<Iterable<T>, String> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");
    final Distribution upsertRetries = Metrics.distribution("cognite", "upsertRetries");
    final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    final ConnectorServiceV1 connector;
    ConnectorServiceV1.ItemWriter itemWriterInsert;
    ConnectorServiceV1.ItemWriter itemWriterDelete;
    final WriterConfig writerConfig;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public UpsertItemViaDeleteBaseFn(Hints hints, WriterConfig writerConfig,
                                     PCollectionView<List<ProjectConfig>> projectConfigView) {
        Preconditions.checkNotNull(writerConfig, "Writer config cannot be null.");
        Preconditions.checkNotNull(hints, "Hints cannot be null");

        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(writerConfig.getAppIdentifier())
                .setSessionIdentifier(writerConfig.getSessionIdentifier())
                .build();
        this.projectConfigView = projectConfigView;
        this.writerConfig = writerConfig;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up UpsertRelationshipFn.");
        LOG.debug("Opening writers.");
        itemWriterInsert = getItemWriterInsert();
        itemWriterDelete = getItemWriterDelete();
    }

    @ProcessElement
    public void processElement(@Element Iterable<T> element, OutputReceiver<String> outputReceiver,
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

         // naive de-duplication based on ids
        Map<String, T> extIdInsertMap = new HashMap<>(1000);
        Map<String, T> extIdDeleteMap = new HashMap<>(500);

        try {
            populateMaps(element, extIdInsertMap);
        } catch (Exception e) {
            LOG.error(batchLogPrefix + e.getMessage());
            throw e;
        }

        LOG.info(batchLogPrefix + "Received items to write:{}", extIdInsertMap.size());

        // Insert / update lists
        List<T> elementList = new ArrayList<>(extIdInsertMap.size());
        List<T> elementListDelete = new ArrayList<>(extIdInsertMap.size());

        // Should not happen--but need to guard against empty input
        if (extIdInsertMap.isEmpty()) {
            LOG.warn(batchLogPrefix + "No input items received.");
            return;
        }

        // Results containers
        List<String> resultItems = new ArrayList<>(extIdInsertMap.size());
        String exceptionMessage = "";

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int maxRetries = 4;
        int upsertLoopCounter = 0;

        /*
        The upsert loop. If there are items left to insert:
        1. Insert elements
        2. If conflict, copy duplicates into the delete maps
        3. Delete elements
         */
        for (int i = 0; i < maxRetries && extIdInsertMap.size() > 0; i++,
                Thread.sleep(Math.min(1000L, (10L * (long) Math.exp(i)) + random.nextLong(20)))) {
            LOG.info(batchLogPrefix + "Start upsert loop {} with {} items to insert",
                    i,
                    extIdInsertMap.size());
            upsertLoopCounter++;

            /*
            Insert elements
             */
            elementList.clear();
            elementList.addAll(extIdInsertMap.values());

            if (elementList.isEmpty()) {
                LOG.info(batchLogPrefix + "Insert items list is empty. Skipping insert.");
            } else {
                LOG.info(batchLogPrefix + "Insert with {} items to write.", extIdInsertMap.size());
                ResponseItems<String> responseItemsInsert = this.sendRequestInsert(toRequestInsertItems(elementList),
                        projectConfig, batchLogPrefix).join();
                if (responseItemsInsert.isSuccessful()) {
                    if (writerConfig.isMetricsEnabled()) {
                        MetricsUtil.recordApiRetryCounter(responseItemsInsert, apiRetryCounter);
                        MetricsUtil.recordApiLatency(responseItemsInsert, apiLatency);
                        apiBatchSize.update(elementList.size());
                    }
                    LOG.info(batchLogPrefix + "Insert success. Adding {} insert result items to result collection.",
                            responseItemsInsert.getResultsItems().size());
                    resultItems.addAll(responseItemsInsert.getResultsItems());
                    extIdInsertMap.clear();
                } else {
                    LOG.warn(batchLogPrefix + "Insert failed. Most likely due to duplicate items. "
                            + "Converting duplicates to update and retrying the request");
                    exceptionMessage = responseItemsInsert.getResponseBodyAsString();
                    LOG.debug(batchLogPrefix + "Insert failed. {}", responseItemsInsert.getResponseBodyAsString());
                    if (i == maxRetries - 1) {
                        // Add the error message to std logging
                        LOG.error(batchLogPrefix + "Insert failed. {}", responseItemsInsert.getResponseBodyAsString());
                    }

                    List<Item> duplicates = ItemParser.parseItems(responseItemsInsert.getDuplicateItems());
                    LOG.info(batchLogPrefix + "Number of duplicate entries reported by CDF: {}", duplicates.size());

                    // Copy duplicates from insert to the delete request
                    for (Item value : duplicates) {
                        if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                            extIdDeleteMap.put(value.getExternalId(), extIdInsertMap.get(value.getExternalId()));
                        } else {
                            LOG.warn(batchLogPrefix + "Item returned without [externalId]. This should not happen.");
                        }
                    }
                }
            }

            /*
            Delete elements
             */
            elementListDelete.clear();
            elementListDelete.addAll(extIdDeleteMap.values());

            if (elementListDelete.isEmpty()) {
                LOG.info(batchLogPrefix + "Delete items list is empty. Skipping delete.");
            } else {
                LOG.info(batchLogPrefix + "Delete {} items.", extIdDeleteMap.size());

                List<Map<String, Object>> deleteItems = toRequestDeleteItems(elementListDelete);

                ResponseItems<String> responseItemsDelete = this.sendRequestDelete(deleteItems, projectConfig,
                        batchLogPrefix).join();
                if (responseItemsDelete.isSuccessful()) {
                    if (writerConfig.isMetricsEnabled()) {
                        MetricsUtil.recordApiRetryCounter(responseItemsDelete, apiRetryCounter);
                        MetricsUtil.recordApiLatency(responseItemsDelete, apiLatency);
                        apiBatchSize.update(elementListDelete.size());
                    }
                    LOG.info(batchLogPrefix + "Delete success.");
                    extIdDeleteMap.clear();
                } else {
                    LOG.warn(batchLogPrefix + "Delete failed. Most likely due to missing items."
                            + "Will retry the insert");
                    exceptionMessage = responseItemsDelete.getResponseBodyAsString();
                    LOG.debug(batchLogPrefix + "Delete failed. {}", responseItemsDelete.getResponseBodyAsString());
                    if (i == maxRetries - 1) {
                        // Add the error message to std logging
                        LOG.error(batchLogPrefix + "Delete failed. {}", responseItemsDelete.getResponseBodyAsString());
                    }

                    List<Item> missing = ItemParser.parseItems(responseItemsDelete.getMissingItems());
                    LOG.info(batchLogPrefix + "Number of missing items reported by CDF: {}", missing.size());
                }
            }
        }
        upsertRetries.update(upsertLoopCounter - 1);

        if (extIdInsertMap.size() > 0) {
            LOG.error(batchLogPrefix + "Failed to upsert items. {} items remaining."
                    + System.lineSeparator() + "{}",
                    extIdInsertMap.size(),
                    exceptionMessage);
            throw new Exception(batchLogPrefix + "Failed to upsert items. "
                    + extIdInsertMap.size() + " items remaining. "
                    + System.lineSeparator() + exceptionMessage);
        } else {
            LOG.info(batchLogPrefix + "Successfully upserted {} items.", resultItems.size());
        }

        // output the upserted items (excluding duplicates)
        for (String outputElement : resultItems) {
            outputReceiver.output(outputElement);
        }
    }

    CompletableFuture<ResponseItems<String>> sendRequestInsert(List<Map<String, Object>> itemList,
                                                               ProjectConfig config,
                                                               String batchLogPrefix) throws Exception {
        Preconditions.checkArgument(itemList.size() > 0, batchLogPrefix
                + "The insert item list cannot be empty.");

        // build initial request object
        RequestParameters request = RequestParameters.create()
                .withItems(itemList)
                .withProjectConfig(config);
        LOG.debug(batchLogPrefix + "Built upsert elements request for {} elements", itemList.size());

        // post write request and monitor for known constraint violations (duplicates, missing, etc).
        CompletableFuture<ResponseItems<String>> requestResult = itemWriterInsert.writeItemsAsync(request);
        LOG.info(batchLogPrefix + "Insert elements request sent.");

        return requestResult;
    }

    CompletableFuture<ResponseItems<String>> sendRequestDelete(List<Map<String, Object>> itemList,
                                                               ProjectConfig config,
                                                               String batchLogPrefix) throws Exception {
        Preconditions.checkArgument(itemList.size() > 0, batchLogPrefix
                + "The update item list cannot be empty.");

        // build initial request object
        RequestParameters request = RequestParameters.create()
                .withItems(itemList)
                .withProjectConfig(config);
        LOG.debug(batchLogPrefix + "Built update elements request for {} elements", itemList.size());

        // post delete request and monitor for known constraint violations (duplicates, missing, etc).
        CompletableFuture<ResponseItems<String>> requestResult = itemWriterDelete.writeItemsAsync(request);
        LOG.info(batchLogPrefix + "Update elements request sent.");

        return requestResult;
    }


    /**
     * Adds the individual elements into maps for externalId/id based de-duplication.
     *
     * @param element
     * @param externalIdMap
     */
    protected abstract void populateMaps(Iterable<T> element,
                              Map<String, T> externalIdMap) throws Exception;

    /**
     * Build the resource specific insert item writer.
     * @return
     */
    protected abstract ConnectorServiceV1.ItemWriter getItemWriterInsert();

    /**
     * Build the resource specific delete item writer.
     * @return
     */
    protected abstract ConnectorServiceV1.ItemWriter getItemWriterDelete();

    /**
     * Convert a collection of resource specific types into a generic representation of insert items.
     *
     * Insert items follow the Cognite API schema of inserting items of the resource type. The insert items
     * representation is a <code>List</code> of <code>Map<String, Object></code> where each <code>Map</code> represents
     * an insert object.
     *
     * @param input
     * @return
     * @throws Exception
     */
    protected abstract List<Map<String, Object>> toRequestInsertItems(Iterable<T> input) throws Exception;

    /**
     * Convert a collection of resource specific types into a generic representation of delete items.
     *
     * Delete items follow the Cognite API schema of deleting items of the resource type. The insert items
     * representation is a <code>List</code> of <code>Map<String, String></code> where each <code>Map</code> represents
     * an delete object. The delete object is basically {key:\"externalId\", value:<value of externalId>}
     *
     * @param input
     * @return
     * @throws Exception
     */
    protected abstract List<Map<String, Object>> toRequestDeleteItems(Iterable<T> input) throws Exception;
}
