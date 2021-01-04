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
import com.cognite.client.dto.Asset;
import com.cognite.client.dto.Item;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.AssetParser;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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

/**
 * Upsert a sorted collection of assets to CDF.clean.
 *
 * This function will first try to insert the items as new assets. In case of conflict, it will retry as an update. The
 * update behavior is specified by the WriterConfig.
 *
 * Upsert of assets is handled in this separate function due to the special hierarchy constraints that requires the items
 * to be posted in a certain order.
 */
public class UpsertAssetFn extends DoFn<List<Asset>, String> {
    private final static int MAX_BATCH_SIZE = 200;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    private final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    private final ConnectorServiceV1 connector;
    private ConnectorServiceV1.ItemWriter itemWriterInsert;
    private ConnectorServiceV1.ItemWriter itemWriterUpdate;
    private final WriterConfig writerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public UpsertAssetFn(Hints hints, WriterConfig writerConfig,
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
        LOG.info("Setting up upsertAssetFn.");
        LOG.debug("Opening writers.");
        itemWriterInsert = connector.writeAssets();
        itemWriterUpdate = connector.updateAssets();
    }

    @ProcessElement
    public void processElement(@Element List<Asset> elements, OutputReceiver<String> outputReceiver,
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

        LOG.info(batchLogPrefix + "Received total number of assets to write: {}", elements.size());
        LOG.info(batchLogPrefix + "Will break the complete collection of assets into sub-batches for upsert.");

        // In case of no elements, just skip
        if (elements.isEmpty()) {
            return;
        }

        // Process ordered batches of the elements.
        int elementCount = 0;
        List<Asset> writeBatch = new ArrayList<>(MAX_BATCH_SIZE);
        for (Asset item : elements) {
            writeBatch.add(item);
            elementCount++;
            if (elementCount >= MAX_BATCH_SIZE) {
                this.processBatch(writeBatch, outputReceiver, projectConfig, batchLogPrefix);
                writeBatch = new ArrayList<>(MAX_BATCH_SIZE);
                elementCount = 0;
            }
        }
        if (writeBatch.size() > 0) {
            this.processBatch(writeBatch, outputReceiver, projectConfig, batchLogPrefix);
        }
    }

    /*
    Process each sub-batch of items. Try insert first, then update on conflict.
     */
    private void processBatch(List<Asset> batch,
                              OutputReceiver<String> outputReceiver,
                              ProjectConfig projectConfig,
                              String batchLogPrefix) throws Exception {

        // naive de-duplication based on ids
        Map<Long, Asset> internalIdInsertMap = new HashMap<>(MAX_BATCH_SIZE);
        Map<String, Asset> externalIdInsertMap = new HashMap<>(MAX_BATCH_SIZE);
        Map<Long, Asset> internalIdUpdateMap = new HashMap<>(MAX_BATCH_SIZE / 2);
        Map<String, Asset> externalIdUpdateMap = new HashMap<>(MAX_BATCH_SIZE / 2);

        for (Asset value : batch) {
            if (value.hasExternalId()) {
                externalIdInsertMap.put(value.getExternalId().getValue(), value);
            } else if (value.hasId()) {
                internalIdInsertMap.put(value.getId().getValue(), value);
            } else {
                String message = batchLogPrefix + "Asset does not contain id nor externalId: " + value.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }

        LOG.info(batchLogPrefix + "Received assets to write in sub-batch: {}",
                internalIdInsertMap.size() + externalIdInsertMap.size());

        // Combine into list
        List<Asset> elementList = new ArrayList<>(MAX_BATCH_SIZE);
        elementList.addAll(externalIdInsertMap.values());
        elementList.addAll(internalIdInsertMap.values());

        // Should not happen--but need to guard against empty input
        if (elementList.isEmpty()) {
            LOG.warn(batchLogPrefix + "No input assets received.");
            return;
        }

        // Results set container
        List<String> resultItems;

        // Containers for the most recent results. Used for logging errors
        ResponseItems<String> responseItems = null;
        ResponseItems<String> responseItemsInsert = null;
        ResponseItems<String> responseItemsUpdate = null;

        responseItems = this.sendRequestInsert(toRequestInsertItems(elementList),
                projectConfig, batchLogPrefix).join();
        boolean requestResult = responseItems.isSuccessful();
        if (responseItems.isSuccessful()) {
            if (writerConfig.isMetricsEnabled()) {
                MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                MetricsUtil.recordApiLatency(responseItems, apiLatency);
            }
            resultItems = responseItems.getResultsItems();
            LOG.info(batchLogPrefix + "Writing assets as insert successfully. Inserted {} assets.",
                    resultItems.size());
        } else {
            // if the request result is false, we have duplicates.
            LOG.info(batchLogPrefix + "Write assets as insert failed. Most likely due to duplicate assets."
                    + " Converting duplicates to update and retrying the request");
            resultItems = new ArrayList<>(MAX_BATCH_SIZE);
            List<Item> duplicates = ItemParser.parseItems(responseItems.getDuplicateItems());
            LOG.info(batchLogPrefix + "Number of duplicate entries reported by CDF: {}", duplicates.size());

            // Move duplicates from insert to the update request
            for (Item value : duplicates) {
                if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                    externalIdUpdateMap.put(value.getExternalId(), externalIdInsertMap.get(value.getExternalId()));
                    externalIdInsertMap.remove(value.getExternalId());
                } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                    internalIdUpdateMap.put(value.getId(), internalIdInsertMap.get(value.getId()));
                    internalIdInsertMap.remove(value.getId());
                } else if (value.getIdTypeCase() == Item.IdTypeCase.LEGACY_NAME) {
                    // Special case for v1 TS headers.
                    externalIdUpdateMap.put(value.getLegacyName(), externalIdInsertMap.get(value.getLegacyName()));
                    externalIdInsertMap.remove(value.getLegacyName());
                }
            }

            // Send inserts
            LOG.info(batchLogPrefix + "Retrying insert with assets to write: {}",
                    internalIdInsertMap.size() + externalIdInsertMap.size());
            elementList.clear();
            elementList.addAll(externalIdInsertMap.values());
            elementList.addAll(internalIdInsertMap.values());
            boolean requestResultInsert = false;
            if (elementList.isEmpty()) {
                LOG.info(batchLogPrefix + "Insert assets list is empty. Skipping insert.");
                requestResultInsert = true;
            } else {
                responseItemsInsert = this.sendRequestInsert(toRequestInsertItems(elementList),
                        projectConfig, batchLogPrefix).join();
                requestResultInsert = responseItemsInsert.isSuccessful();
                if (responseItemsInsert.isSuccessful()) {
                    if (writerConfig.isMetricsEnabled()) {
                        MetricsUtil.recordApiRetryCounter(responseItemsInsert, apiRetryCounter);
                        MetricsUtil.recordApiLatency(responseItemsInsert, apiLatency);
                    }
                    LOG.debug(batchLogPrefix + "Insert success. Adding insert result items to result collection.");
                    resultItems.addAll(responseItemsInsert.getResultsItems());
                }
            }

            //send updates
            LOG.info(batchLogPrefix + "Retrying insert with assets to update:{}",
                    internalIdUpdateMap.size() + externalIdUpdateMap.size());
            List<Asset> elementListUpdate = new ArrayList<>(MAX_BATCH_SIZE);
            elementListUpdate.addAll(externalIdUpdateMap.values());
            elementListUpdate.addAll(internalIdUpdateMap.values());
            boolean requestResultUpdate = false;
            if (elementListUpdate.isEmpty()) {
                LOG.info(batchLogPrefix + "Update items list is empty. Skipping update.");
                requestResultUpdate = true;
            } else {
                // Build replace or patch style update items
                List<Map<String, Object>> updateItems;
                if (writerConfig.getUpsertMode() == UpsertMode.REPLACE) {
                    LOG.debug(batchLogPrefix + "Upsert mode is set to [replace].");
                    updateItems = toRequestReplaceItems(elementListUpdate);
                } else {
                    LOG.debug(batchLogPrefix + "Upsert mode is set to [update].");
                    updateItems = toRequestUpdateItems(elementListUpdate);
                }

                responseItemsUpdate = this.sendRequestUpdate(updateItems, projectConfig,
                        batchLogPrefix).join();
                requestResultUpdate = responseItemsUpdate.isSuccessful();
                if (responseItemsUpdate.isSuccessful()) {
                    if (writerConfig.isMetricsEnabled()) {
                        MetricsUtil.recordApiRetryCounter(responseItemsUpdate, apiRetryCounter);
                        MetricsUtil.recordApiLatency(responseItemsUpdate, apiLatency);
                    }
                    LOG.info(batchLogPrefix + "Update success. Updated {} assets.",
                            responseItemsUpdate.getResultsItems().size());
                    resultItems.addAll(responseItemsUpdate.getResultsItems());
                }
            }

            requestResult = requestResultInsert && requestResultUpdate;
        }

        if (!requestResult) {
            String message = batchLogPrefix + "Failed to upsert assets. Initial response: "
                    + responseItems.getResponseBodyAsString();
            LOG.error(message);
            if (null != responseItemsInsert && !responseItemsInsert.isSuccessful()) {
                message = batchLogPrefix + "Failed to upsert assets. Most recent insert items response: "
                        + responseItemsInsert.getResponseBodyAsString();
                LOG.error(message);
            }
            if (null != responseItemsUpdate && !responseItemsUpdate.isSuccessful()) {
                message = batchLogPrefix + "Failed to upsert assets. Most recent update items response: "
                        + responseItemsUpdate.getResponseBodyAsString();
                LOG.error(message);
            }
            throw new Exception(message);
        }

        // output the upserted items (excluding duplicates)
        for (String outputElement : resultItems) {
            outputReceiver.output(outputElement);
        }
    }


    private CompletableFuture<ResponseItems<String>> sendRequestInsert(List<Map<String, Object>> itemList,
                                                                       ProjectConfig config,
                                                                       String batchLogPrefix) throws Exception {
        Preconditions.checkArgument(itemList.size() > 0, batchLogPrefix
                + "The insert assets list cannot be empty.");

        // build initial request object
        RequestParameters request = RequestParameters.create()
                .withItems(itemList)
                .withProjectConfig(config);
        LOG.debug(batchLogPrefix + "Built insert assets request for {} assets", itemList.size());

        // post write request and monitor for known constraint violations (duplicates, missing, etc).
        CompletableFuture<ResponseItems<String>> requestResult = itemWriterInsert.writeItemsAsync(request);
        LOG.info(batchLogPrefix + "Insert elements request sent.");

        return requestResult;
    }

    private CompletableFuture<ResponseItems<String>> sendRequestUpdate(List<Map<String, Object>> itemList,
                                                                       ProjectConfig config,
                                                                       String batchLogPrefix) throws Exception {
        Preconditions.checkArgument(itemList.size() > 0, batchLogPrefix
                + "The update assets list cannot be empty.");

        // build initial request object
        RequestParameters request = RequestParameters.create()
                .withItems(itemList)
                .withProjectConfig(config);
        LOG.debug(batchLogPrefix + "Built update assets request for {} assets", itemList.size());

        // post delete request and monitor for known constraint violations (duplicates, missing, etc).
        CompletableFuture<ResponseItems<String>> requestResult = itemWriterUpdate.writeItemsAsync(request);
        LOG.info(batchLogPrefix + "Update elements request sent.");

        return requestResult;
    }

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
    private List<Map<String, Object>> toRequestInsertItems(Iterable<Asset> input) {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Asset element : input) {
            listBuilder.add(AssetParser.toRequestInsertItem(element));
        }
        return listBuilder.build();
    }

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
    private List<Map<String, Object>> toRequestUpdateItems(Iterable<Asset> input) {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Asset element : input) {
            listBuilder.add(AssetParser.toRequestUpdateItem(element));
        }
        return listBuilder.build();
    }

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
    private List<Map<String, Object>> toRequestReplaceItems(Iterable<Asset> input) {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Asset element : input) {
            listBuilder.add(AssetParser.toRequestReplaceItem(element));
        }
        return listBuilder.build();
    }
}
