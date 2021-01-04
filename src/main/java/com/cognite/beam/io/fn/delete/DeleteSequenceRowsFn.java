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

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.SequenceBody;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.client.servicesV1.parser.SequenceParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Deletes rows from {@code Sequences}.
 *
 * This function will delete all row numbers referenced in the {@link SequenceBody} input object.
 */
public class DeleteSequenceRowsFn extends DoFn<Iterable<SequenceBody>, SequenceBody> {
    private static final int MAX_RETRIES = 2;

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final PCollectionView<List<ProjectConfig>> projectConfigView;
    private final ConnectorServiceV1 connector;
    private ConnectorServiceV1.ItemWriter itemWriter;

    public DeleteSequenceRowsFn(Hints hints,
                                String appIdentifier,
                                String sessionIdentifier,
                                PCollectionView<List<ProjectConfig>> projectConfigView) {
        Preconditions.checkNotNull(appIdentifier, "App identifier cannot be null.");
        Preconditions.checkNotNull(sessionIdentifier, "Session identifier cannot be null.");
        Preconditions.checkNotNull(hints, "Hints cannot be null");

        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(appIdentifier)
                .setSessionIdentifier(sessionIdentifier)
                .build();
        this.projectConfigView = projectConfigView;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up DeleteSequenceRowsFn.");
        itemWriter = connector.deleteSequencesRows();
    }

    @ProcessElement
    public void processElement(@Element Iterable<SequenceBody> element,
                               OutputReceiver<SequenceBody> outputReceiver,
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
        List<SequenceBody> itemList = new ArrayList<>();
        long rowCounter = 0;
        for (SequenceBody value : element) {
            if (value.hasExternalId() || value.hasId()) {
                itemList.add(value);
                rowCounter += value.getRowsCount();
            } else {
                String message = batchLogPrefix + "Sequence does not contain id nor externalId: " + value.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }
        LOG.info(batchLogPrefix + "Received {} rows to remove from {} sequences.",
                rowCounter,
                itemList.size());

        // should not happen, but need to guard against empty input
        if (itemList.isEmpty()) {
            LOG.warn(batchLogPrefix + "No items in the input. Returning without deleting any rows.");
            return;
        }

        ResponseItems<String> responseItems = this
                .sendRequest(itemList, projectConfig, batchLogPrefix)
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
                    removeItemFromList(itemList, value.getExternalId());
                } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                    removeItemFromList(itemList, value.getId());
                }
            }

            for (Item value : duplicates) {
                if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                    removeItemFromList(itemList, value.getExternalId());
                } else if (value.getIdTypeCase() == Item.IdTypeCase.ID) {
                    removeItemFromList(itemList, value.getId());
                }
            }

            // do not send empty requests
            if (itemList.isEmpty()) {
                requestResult = true;
            } else {
                responseItems = this
                        .sendRequest(itemList, projectConfig, batchLogPrefix)
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
        itemList.forEach(item -> outputReceiver.output(item));
    }

    private CompletableFuture<ResponseItems<String>> sendRequest(Iterable<SequenceBody> elements,
                                                                 ProjectConfig config,
                                                                 String batchLogPrefix) throws Exception {
        ImmutableList.Builder<Map<String, Object>> itemsBuilder = ImmutableList.builder();
        for (SequenceBody rows : elements) {
            itemsBuilder.add(SequenceParser.toRequestDeleteRowsItem(rows));
        }

        RequestParameters request = RequestParameters.create()
                .withItems(itemsBuilder.build())
                .withProjectConfig(config);

        // post delete request and monitor for known constraint violations (duplicates, missing, etc).
        CompletableFuture<ResponseItems<String>> requestResult = itemWriter.writeItemsAsync(request);
        LOG.info(batchLogPrefix + "Delete items request sent.");

        return requestResult;
    }

    /*
    Removes the item with the specified id from the items list.
     */
    private boolean removeItemFromList(List<SequenceBody> items, long id) {
        boolean returnValue = false;
        for (SequenceBody rows : items) {
            if (rows.hasId() && rows.getId().getValue() == id) {
                items.remove(rows);
                returnValue = true;
            }
        }
        return returnValue;
    }

    /*
    Removes the item with the specified externalId from the items list.
     */
    private boolean removeItemFromList(List<SequenceBody> items, String externalId) {
        boolean returnValue = false;
        for (SequenceBody rows : items) {
            if (rows.hasExternalId() && rows.getExternalId().getValue().equals(externalId)) {
                items.remove(rows);
                returnValue = true;
            }
        }
        return returnValue;
    }
}
