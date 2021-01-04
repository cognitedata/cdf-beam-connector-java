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
import com.cognite.client.dto.FileMetadata;
import com.cognite.client.dto.Item;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.FileParser;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Writes file headers to CDF.Clean.
 *
 * This function will first try to write the file headers as updates. In case the headers don't exist in CDF,
 * they will be created via the files upload endpoint. Effectively this results in an upsert.
 *
 */
public class UpsertFileHeaderFn extends UpsertItemBaseFn<FileMetadata> {
    final Counter insertCounter = Metrics.counter("cognite", "fileCreate");

    public UpsertFileHeaderFn(Hints hints, WriterConfig writerConfig,
                              PCollectionView<List<ProjectConfig>> projectConfig) {
        super(hints, writerConfig, projectConfig);
    }

    /**
     * Converts input to update requests + insert requests for files metadata/header.
     *
     * The superclass' method is overridden because the superclass performs insert and then update--file metadata
     * requires the reverse order: update and then insert.
     *
     * @param element
     * @param outputReceiver
     * @param context
     * @throws Exception
     */
    @Override
    @ProcessElement
    public void processElement(@Element Iterable<FileMetadata> element, OutputReceiver<String> outputReceiver,
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
        Map<Long, FileMetadata> internalIdInsertMap = new HashMap<>(500);
        Map<String, FileMetadata> externalIdInsertMap = new HashMap<>(500);
        Map<Long, FileMetadata> internalIdUpdateMap = new HashMap<>(1000);
        Map<String, FileMetadata> externalIdUpdateMap = new HashMap<>(1000);
        Map<Long, FileMetadata> internalIdAssetsMap = new HashMap<>(50);
        Map<String, FileMetadata> externalIdAssetsMap = new HashMap<>(50);

        try {
            populateMaps(element, internalIdUpdateMap, externalIdUpdateMap);
        } catch (Exception e) {
            LOG.error(batchLogPrefix + e.getMessage());
            throw e;
        }
        LOG.info(batchLogPrefix + "Received file headers to write:{}", internalIdUpdateMap.size() + externalIdUpdateMap.size());

        // Check for files with >1k assets
        for (Long key : internalIdUpdateMap.keySet()) {
            FileMetadata fileMetadata = internalIdUpdateMap.get(key);
            if (fileMetadata.getAssetIdsCount() > 1000) {
                internalIdUpdateMap.put(key, fileMetadata.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0,1000))
                        .build());
                internalIdAssetsMap.put(key, FileMetadata.newBuilder()
                        .setId(fileMetadata.getId())
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsList().size()))
                        .build());
            }
        }
        for (String key : externalIdUpdateMap.keySet()) {
            FileMetadata fileMetadata = externalIdUpdateMap.get(key);
            if (fileMetadata.getAssetIdsCount() > 1000) {
                externalIdUpdateMap.put(key, fileMetadata.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0,1000))
                        .build());
                externalIdAssetsMap.put(key, FileMetadata.newBuilder()
                        .setExternalId(fileMetadata.getExternalId())
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsList().size()))
                        .build());
            }
        }

        // Combine into list
        List<FileMetadata> elementListUpdate = new ArrayList<>(1000);
        elementListUpdate.addAll(externalIdUpdateMap.values());
        elementListUpdate.addAll(internalIdUpdateMap.values());

        // Should not happen--but need to guard against empty input
        if (elementListUpdate.isEmpty()) {
            LOG.warn(batchLogPrefix + "No input file headers received.");
            return;
        }

        // Results set container
        List<String> resultItems;

        List<Map<String, Object>> updateItems;
        if (writerConfig.getUpsertMode() == UpsertMode.REPLACE) {
            updateItems = toRequestReplaceItems(elementListUpdate);
        } else {
            updateItems = toRequestUpdateItems(elementListUpdate);
        }
        ResponseItems<String> responseItems = this.sendRequestUpdate(updateItems, projectConfig, batchLogPrefix).join();
        boolean requestResult = responseItems.isSuccessful();
        if (responseItems.isSuccessful()) {
            if (writerConfig.isMetricsEnabled()) {
                MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                MetricsUtil.recordApiLatency(responseItems, apiLatency);
                apiBatchSize.update(updateItems.size());
            }
            resultItems = responseItems.getResultsItems();
            LOG.info(batchLogPrefix + "Writing file headers as update successfully. Updated {} items.", resultItems.size());
        } else {
            // if the request result is false, we have missing items.
            LOG.info(batchLogPrefix + "Write file headers as update failed. Most likely due to missing items."
                    + "Converting missing items to insert and retrying the request");
            resultItems = new ArrayList<>(1000);
            List<Item> missingItems = ItemParser.parseItems(responseItems.getMissingItems());
            LOG.debug(batchLogPrefix + "Number of missing entries reported by CDF: {}", missingItems.size());

            // Move missing from update to insert
            for (Item value : missingItems) {
                LOG.trace(batchLogPrefix + "Iterating item in the missing items collection: {}", value.toString());
                if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                    externalIdInsertMap.put(value.getExternalId(), externalIdUpdateMap.get(value.getExternalId()));
                    externalIdUpdateMap.remove(value.getExternalId());
                } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                    internalIdInsertMap.put(value.getId(), internalIdUpdateMap.get(value.getId()));
                    internalIdUpdateMap.remove(value.getId());
                }
            }

            // Send updates without the missing items
            LOG.info(batchLogPrefix + "Retrying update with items to write: {}",
                    internalIdUpdateMap.size() + externalIdUpdateMap.size());
            elementListUpdate.clear();
            elementListUpdate.addAll(externalIdUpdateMap.values());
            elementListUpdate.addAll(internalIdUpdateMap.values());
            boolean requestResultUpdate = false;
            if (elementListUpdate.isEmpty()) {
                LOG.info(batchLogPrefix + "Update items list is empty. Skipping update.");
                requestResultUpdate = true;
            } else {
                if (writerConfig.getUpsertMode() == UpsertMode.REPLACE) {
                    updateItems = toRequestReplaceItems(elementListUpdate);
                } else {
                    updateItems = toRequestUpdateItems(elementListUpdate);
                }
                ResponseItems<String> responseItemsUpdate = this.sendRequestUpdate(updateItems,
                        projectConfig, batchLogPrefix).join();
                requestResultUpdate = responseItemsUpdate.isSuccessful();
                if (responseItemsUpdate.isSuccessful()) {
                    if (writerConfig.isMetricsEnabled()) {
                        MetricsUtil.recordApiRetryCounter(responseItemsUpdate, apiRetryCounter);
                        MetricsUtil.recordApiLatency(responseItemsUpdate, apiLatency);
                        apiBatchSize.update(updateItems.size());
                    }
                    LOG.debug(batchLogPrefix + "Update success. Adding update result items to result collection.");
                    resultItems.addAll(responseItemsUpdate.getResultsItems());
                }
            }

            //send inserts
            LOG.info(batchLogPrefix + "Retrying insert with items to write: {}", internalIdInsertMap.size()
                    + externalIdInsertMap.size());
            List<FileMetadata> elementListInsert = new ArrayList<>(1000);
            elementListInsert.addAll(externalIdInsertMap.values());
            elementListInsert.addAll(internalIdInsertMap.values());
            boolean requestResultInsert = false;
            if (elementListInsert.isEmpty()) {
                LOG.info(batchLogPrefix + "Insert items list is empty. Skipping insert.");
                requestResultInsert = true;
            } else {
                //ResponseItems<String> responseItemsInsert =
                requestResultInsert =
                        this.sendRequestInsertItems(toRequestInsertItems(elementListInsert), projectConfig, batchLogPrefix);
                if (requestResultInsert) {
                    LOG.debug(batchLogPrefix + "Update success. Adding insert result items to result collection.");
                    resultItems.addAll(responseItems.getResultsItems());
                }
            }

            requestResult = requestResultInsert && requestResultUpdate;
        }

        if (!requestResult) {
            LOG.error(batchLogPrefix + "Failed to upsert items.", responseItems.getResponseBodyAsString());
            throw new Exception(batchLogPrefix + "Failed to upsert items. " + responseItems.getResponseBodyAsString());
        }

        /*
        Write extra assets id links as separate updates. The api only supports 1k assetId links per file object
        per api request. If a file contains a large number of assetIds, we need to split them up into an initial
        file create/update (all the code above) and subsequent update requests which add the remaining
        assetIds (code below).
         */
        Map<Long, FileMetadata> internalIdTempMap = new HashMap<>(internalIdAssetsMap.size());
        Map<String, FileMetadata> externalIdTempMap = new HashMap<>(externalIdAssetsMap.size());
        while (internalIdAssetsMap.size() > 0 || externalIdAssetsMap.size() > 0) {
            LOG.info(batchLogPrefix + "Some files have very high assetId cardinality (+1k). Adding assetId to "
                    + (internalIdAssetsMap.size() + externalIdAssetsMap.size())
                    + " file(s).");
            internalIdUpdateMap.clear();
            externalIdUpdateMap.clear();
            internalIdTempMap.clear();
            externalIdTempMap.clear();

            // Check for files with >1k remaining assets
            for (Long key : internalIdAssetsMap.keySet()) {
                FileMetadata fileMetadata = internalIdAssetsMap.get(key);
                if (fileMetadata.getAssetIdsCount() > 1000) {
                    internalIdUpdateMap.put(key, fileMetadata.toBuilder()
                            .clearAssetIds()
                            .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0,1000))
                            .build());
                    internalIdTempMap.put(key, FileMetadata.newBuilder()
                            .setId(fileMetadata.getId())
                            .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsList().size()))
                            .build());
                } else {
                    // The entire assetId list can be pushed in a single update
                    internalIdUpdateMap.put(key, fileMetadata);
                }
            }
            internalIdAssetsMap.clear();
            internalIdAssetsMap.putAll(internalIdTempMap);

            for (String key : externalIdAssetsMap.keySet()) {
                FileMetadata fileMetadata = externalIdAssetsMap.get(key);
                if (fileMetadata.getAssetIdsCount() > 1000) {
                    externalIdUpdateMap.put(key, fileMetadata.toBuilder()
                            .clearAssetIds()
                            .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0,1000))
                            .build());
                    externalIdTempMap.put(key, FileMetadata.newBuilder()
                            .setExternalId(fileMetadata.getExternalId())
                            .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsList().size()))
                            .build());
                } else {
                    // The entire assetId list can be pushed in a single update
                    externalIdUpdateMap.put(key, fileMetadata);
                }
            }
            externalIdAssetsMap.clear();
            externalIdAssetsMap.putAll(externalIdTempMap);

            // prepare the update and send request
            LOG.info(batchLogPrefix + "Building update request to add assetIds for {} files.",
                    internalIdUpdateMap.size() + externalIdUpdateMap.size());
            elementListUpdate.clear();
            elementListUpdate.addAll(externalIdUpdateMap.values());
            elementListUpdate.addAll(internalIdUpdateMap.values());

            // should not happen, but need to check
            if (elementListUpdate.isEmpty()) {
                String message = batchLogPrefix + "Internal error. Not able to send assetId update. The payload is empty.";
                LOG.error(message);
                throw new Exception(message);
            }

            ResponseItems<String> responseItemsAssets = this.sendRequestUpdate(toRequestAddAssetIdsItems(elementListUpdate),
                    projectConfig, batchLogPrefix).join();
            if (!responseItemsAssets.isSuccessful()) {
                String message = batchLogPrefix
                        + "Failed to add assetIds. "
                        + responseItemsAssets.getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
        }

        // output the upserted items (excluding duplicates)
        for (String outputElement : resultItems) {
            outputReceiver.output(outputElement);
        }
    }


    boolean sendRequestInsertItems(List<Map<String, Object>> itemList,
                                   ProjectConfig config,
                                   String batchLogPrefix) throws Exception {
        Preconditions.checkArgument(itemList.size() > 0, batchLogPrefix
                + "The insert item list cannot be empty.");
        LOG.info(batchLogPrefix + "Building insert file headers requests for {} elements", itemList.size());

        for (Map<String, Object> insertItem : itemList) {
            ResponseItems<String> responseItems = sendSingleRequestInsert(insertItem, config, batchLogPrefix).join();
            if (responseItems.isSuccessful()) {
                if (writerConfig.isMetricsEnabled()) {
                    MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                    MetricsUtil.recordApiLatency(responseItems, apiLatency);
                    apiBatchSize.update(1);
                    insertCounter.inc();
                }
            } else {
                String message = batchLogPrefix + "Error when writing the following item: " + insertItem.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }
        LOG.info(batchLogPrefix + "File headers successfully written to CDF: {}", itemList.size());
        return true;
    }

    CompletableFuture<ResponseItems<String>> sendSingleRequestInsert(Map<String, Object> item,
                                                                     ProjectConfig config,
                                                                     String batchLogPrefix) throws Exception {
        // build request object
        RequestParameters request = RequestParameters.create()
                .withRequestParameters(item)
                .withProjectConfig(config);
        LOG.debug(batchLogPrefix + "Built insert file request for {}", item.toString());

        // post write request and monitor for known constraint violations (duplicates, missing, etc).
        CompletableFuture<ResponseItems<String>> requestResult = itemWriterInsert.writeItemsAsync(request);
        LOG.debug(batchLogPrefix + "Insert single file request set. Result returned: {}", requestResult);
        return requestResult;
    }

    @Override
    protected void populateMaps(Iterable<FileMetadata> element, Map<Long, FileMetadata> internalIdUpdateMap,
                            Map<String, FileMetadata> externalIdUpdateMap) throws Exception {
        for (FileMetadata value : element) {
            if (value.hasExternalId()) {
                externalIdUpdateMap.put(value.getExternalId().getValue(), value);
            } else if (value.hasId()) {
                internalIdUpdateMap.put(value.getId().getValue(), value);
            } else {
                throw new Exception("File metadata item does not contain id nor externalId: " + value.toString());
            }
        }
    }

    protected ConnectorServiceV1.ItemWriter getItemWriterInsert() {
        return connector.writeFile();
    }

    protected ConnectorServiceV1.ItemWriter getItemWriterUpdate() {
        return connector.updateFileHeaders();
    }

    protected List<Map<String, Object>> toRequestInsertItems(Iterable<FileMetadata> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (FileMetadata element : input) {
            listBuilder.add(FileParser.toRequestInsertItem(element));
        }
        return listBuilder.build();
    }

    protected List<Map<String, Object>> toRequestUpdateItems(Iterable<FileMetadata> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        try {
            for (FileMetadata element : input) {
                listBuilder.add(FileParser.toRequestUpdateItem(element));
            }
        } catch (Exception e) {
            LOG.error("Unable to build file insert request: " + e.getMessage());
            throw e;
        }
        return listBuilder.build();
    }

    protected List<Map<String, Object>> toRequestReplaceItems(Iterable<FileMetadata> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (FileMetadata element : input) {
            listBuilder.add(FileParser.toRequestReplaceItem(element));
        }
        return listBuilder.build();
    }

    protected List<Map<String, Object>> toRequestAddAssetIdsItems(Iterable<FileMetadata> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (FileMetadata element : input) {
            listBuilder.add(FileParser.toRequestAddAssetIdsItem(element));
        }
        return listBuilder.build();
    }
}
