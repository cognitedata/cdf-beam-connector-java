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
import com.cognite.beam.io.config.UpsertMode;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.Item;
import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.cognite.beam.io.servicesV1.parser.ItemParser;
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

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Base class for writing items to CDF.Clean. Specific resource types (Event, Asset, etc.) extend this class with
 * custom parsing logic.
 *
 * This function will first try to write the items as new items. In case the items already exists (based on externalId
 * or Id), the items will be updated. Effectively this results in an upsert.
 *
 */
public abstract class UpsertItemBaseFn<T> extends DoFn<Iterable<T>, String> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");
    final Distribution upsertRetries = Metrics.distribution("cognite", "upsertRetries");
    final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    final ConnectorServiceV1 connector;
    ConnectorServiceV1.ItemWriter itemWriterInsert;
    ConnectorServiceV1.ItemWriter itemWriterUpdate;
    final WriterConfig writerConfig;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public UpsertItemBaseFn(Hints hints, WriterConfig writerConfig,
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
        LOG.info("Setting up upsertItemsBase.");
        LOG.debug("Opening writers.");
        itemWriterInsert = getItemWriterInsert();
        itemWriterUpdate = getItemWriterUpdate();
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
        Map<Long, T> intIdInsertMap = new HashMap<>(1000);
        Map<String, T> extIdInsertMap = new HashMap<>(1000);
        Map<Long, T> intIdUpdateMap = new HashMap<>(500);
        Map<String, T> extIdUpdateMap = new HashMap<>(500);
        Map[] elementMaps = {intIdInsertMap, extIdInsertMap, intIdUpdateMap, extIdUpdateMap};

        try {
            populateMaps(element, intIdInsertMap, extIdInsertMap);
        } catch (Exception e) {
            LOG.error(batchLogPrefix + e.getMessage());
            throw e;
        }

        LOG.info(batchLogPrefix + "Received items to write:{}", mapsSize(elementMaps));

        // Insert / update lists
        List<T> elementList = new ArrayList<>(1000);
        List<T> elementListUpdate = new ArrayList<>(1000);

        // Should not happen--but need to guard against empty input
        if (!(mapsSize(elementMaps) > 0)) {
            LOG.warn(batchLogPrefix + "No input items received.");
            return;
        }

        // Results containers
        List<String> resultItems = new ArrayList<>(mapsSize(elementMaps));
        String exceptionMessage = "";

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int maxRetries = 4;
        int upsertLoopCounter = 0; // just tracking statistics

        /*
        The upsert loop. If there are items left to insert or updates:
        1. Insert elements
        2. If conflict, remove duplicates into the update maps
        3. Update elements
        4. If conflicts move missing items into the insert maps
         */
        for (int i = 0; i < maxRetries && mapsSize(elementMaps) > 0; i++,
                Thread.sleep(Math.min(500L, (10L * (long) Math.exp(i)) + random.nextLong(5)))) {
            LOG.info(batchLogPrefix + "Start upsert loop {} with {} items to insert, {} items to update and {} completed items",
                    i,
                    intIdInsertMap.size() + extIdInsertMap.size(),
                    intIdUpdateMap.size() + extIdUpdateMap.size(),
                    resultItems.size());
            upsertLoopCounter++;

            /*
            Insert elements
             */
            elementList.clear();
            elementList.addAll(extIdInsertMap.values());
            elementList.addAll(intIdInsertMap.values());

            if (elementList.isEmpty()) {
                LOG.info(batchLogPrefix + "Insert items list is empty. Skipping insert.");
            } else {
                LOG.info(batchLogPrefix + "Insert with {} items to write, {} by externalId and {} by id.",
                        intIdInsertMap.size() + extIdInsertMap.size(),
                        extIdInsertMap.size(),
                        intIdInsertMap.size());
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
                    intIdInsertMap.clear();
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

                    // Move duplicates from insert to the update request
                    for (Item value : duplicates) {
                        if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                            extIdUpdateMap.put(value.getExternalId(), extIdInsertMap.get(value.getExternalId()));
                            extIdInsertMap.remove(value.getExternalId());
                        } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                            intIdUpdateMap.put(value.getId(), intIdInsertMap.get(value.getId()));
                            intIdInsertMap.remove(value.getId());
                        } else if (value.getIdTypeCase() == Item.IdTypeCase.LEGACY_NAME) {
                            // Special case for v1 TS headers.
                            extIdUpdateMap.put(value.getLegacyName(), extIdInsertMap.get(value.getLegacyName()));
                            extIdInsertMap.remove(value.getLegacyName());
                        }
                    }
                }
            }

            /*
            Update elements
             */
            elementListUpdate.clear();
            elementListUpdate.addAll(extIdUpdateMap.values());
            elementListUpdate.addAll(intIdUpdateMap.values());

            if (elementListUpdate.isEmpty()) {
                LOG.info(batchLogPrefix + "Update items list is empty. Skipping update.");
            } else {
                LOG.info(batchLogPrefix + "Update with {} items to update, {} by externalId, {} by id.",
                        intIdUpdateMap.size() + extIdUpdateMap.size(),
                        extIdUpdateMap.size(),
                        intIdUpdateMap.size());

                List<Map<String, Object>> updateItems;
                if (writerConfig.getUpsertMode() == UpsertMode.REPLACE) {
                    updateItems = toRequestReplaceItems(elementListUpdate);
                } else {
                    updateItems = toRequestUpdateItems(elementListUpdate);
                }

                ResponseItems<String> responseItemsUpdate = this.sendRequestUpdate(updateItems, projectConfig,
                        batchLogPrefix).join();
                if (responseItemsUpdate.isSuccessful()) {
                    if (writerConfig.isMetricsEnabled()) {
                        MetricsUtil.recordApiRetryCounter(responseItemsUpdate, apiRetryCounter);
                        MetricsUtil.recordApiLatency(responseItemsUpdate, apiLatency);
                        apiBatchSize.update(updateItems.size());
                    }
                    LOG.info(batchLogPrefix + "Update success. Adding {} update result items to result collection.",
                            responseItemsUpdate.getResultsItems().size());
                    resultItems.addAll(responseItemsUpdate.getResultsItems());
                    intIdUpdateMap.clear();
                    extIdUpdateMap.clear();
                } else {
                    LOG.warn(batchLogPrefix + "Update failed. Most likely due to missing items."
                            + "Converting missing items to insert and retrying the request");
                    exceptionMessage = responseItemsUpdate.getResponseBodyAsString();
                    LOG.debug(batchLogPrefix + "Update failed. {}", responseItemsUpdate.getResponseBodyAsString());
                    if (i == maxRetries - 1) {
                        // Add the error message to std logging
                        LOG.error(batchLogPrefix + "Update failed. {}", responseItemsUpdate.getResponseBodyAsString());
                    }

                    List<Item> missing = ItemParser.parseItems(responseItemsUpdate.getMissingItems());
                    LOG.info(batchLogPrefix + "Number of missing items reported by CDF: {}", missing.size());

                    // Move missing items from update to the insert request
                    for (Item value : missing) {
                        if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                            extIdInsertMap.put(value.getExternalId(), extIdUpdateMap.get(value.getExternalId()));
                            extIdUpdateMap.remove(value.getExternalId());
                        } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                            intIdInsertMap.put(value.getId(), intIdUpdateMap.get(value.getId()));
                            intIdUpdateMap.remove(value.getId());
                        } else if (value.getIdTypeCase() == Item.IdTypeCase.LEGACY_NAME) {
                            // Special case for v1 TS headers.
                            extIdInsertMap.put(value.getLegacyName(), extIdUpdateMap.get(value.getLegacyName()));
                            extIdUpdateMap.remove(value.getLegacyName());
                        }
                    }
                }
            }
        }
        upsertRetries.update(upsertLoopCounter - 1);

        if (mapsSize(elementMaps) > 0) {
            LOG.error(batchLogPrefix + "Failed to upsert items. {} items remaining. {} items completed upsert."
                    + System.lineSeparator() + "{}",
                    mapsSize(elementMaps),
                    resultItems.size(),
                    exceptionMessage);
            throw new Exception(batchLogPrefix + "Failed to upsert items. "
                    + mapsSize(elementMaps) + " items remaining. "
                    + resultItems.size() + " items completed upsert."
                    + System.lineSeparator() + exceptionMessage);
        } else {
            LOG.info(batchLogPrefix + "Successfully upserted {} items.", resultItems.size());
        }

        // output the upserted items (excluding duplicates)
        for (String outputElement : resultItems) {
            outputReceiver.output(outputElement);
        }
    }

    int mapsSize(Map... maps) {
        int outputSize = 0;
        for (Map map : maps) {
            outputSize += map.size();
        }
        return outputSize;
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

    CompletableFuture<ResponseItems<String>> sendRequestUpdate(List<Map<String, Object>> itemList,
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
        CompletableFuture<ResponseItems<String>> requestResult = itemWriterUpdate.writeItemsAsync(request);
        LOG.info(batchLogPrefix + "Update elements request sent.");

        return requestResult;
    }


    /**
     * Adds the individual elements into maps for externalId/id based de-duplication.
     *
     * @param element
     * @param internalIdMap
     * @param externalIdMap
     */
    protected abstract void populateMaps(Iterable<T> element, Map<Long, T> internalIdMap,
                               Map<String, T> externalIdMap) throws Exception;

    /**
     * Build the resource specific insert item writer.
     * @return
     */
    protected abstract ConnectorServiceV1.ItemWriter getItemWriterInsert();

    /**
     * Build the resource specific update item writer.
     * @return
     */
    protected abstract ConnectorServiceV1.ItemWriter getItemWriterUpdate();

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
     * Convert a collection of resource specific types into a generic representation of update items. The update should
     * only set the provided fields, the provided metadata values and completely replace assetId(s).
     *
     * Insert items follow the Cognite API schema of updating items of the resource type. The insert items
     * representation is a <code>List</code> of <code>Map<String, Object></code> where each <code>Map</code> represents
     * an update object.
     *
     * @param input
     * @return
     * @throws Exception
     */
    protected abstract  List<Map<String, Object>> toRequestUpdateItems(Iterable<T> input) throws Exception;

    /**
     * Convert a collection of resource specific types into a generic representation of update replace items. The update
     * should completely replace all fields of the data object (i.e. set non-proviced fields to null).
     *
     * Insert items follow the Cognite API schema of updating items of the resource type. The insert items
     * representation is a <code>List</code> of <code>Map<String, Object></code> where each <code>Map</code> represents
     * an update object.
     *
     * @param input
     * @return
     * @throws Exception
     */
    protected abstract  List<Map<String, Object>> toRequestReplaceItems(Iterable<T> input) throws Exception;
}
