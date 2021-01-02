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
import com.cognite.beam.io.dto.FileBinary;
import com.cognite.beam.io.dto.Item;
import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.cognite.beam.io.servicesV1.parser.ItemParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadFileBinaryByIdFn extends DoFn<Iterable<Item>, FileBinary> {
    private final int MAX_RETRIES = 2;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final PCollectionView<List<ProjectConfig>> projectConfigView;
    private final ConnectorServiceV1 connector;
    private ConnectorServiceV1.FileBinaryReader fileReader;
    private final ValueProvider<String> tempStorageURI;
    private final boolean forceTempStorage;

    private final Distribution downloadBatchDuration =
            Metrics.distribution("cognite", "downloadBatchDuration");

    public ReadFileBinaryByIdFn(Hints hints,
                                String appIdentifier,
                                String sessionIdentifier,
                                @Nullable ValueProvider<String> tempStorageURI,
                                boolean forceTempStorage,
                                PCollectionView<List<ProjectConfig>> projectConfigView) {
        Preconditions.checkNotNull(appIdentifier, "App identifier cannot be null.");
        Preconditions.checkNotNull(sessionIdentifier, "Session identifier cannot be null.");
        Preconditions.checkNotNull(hints, "Hints cannot be null");

        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(appIdentifier)
                .setSessionIdentifier(sessionIdentifier)
                .build();
        this.tempStorageURI = tempStorageURI;
        this.forceTempStorage = forceTempStorage;
        this.projectConfigView = projectConfigView;
    }

    @Setup
    public void setup() throws Exception {
        LOG.info("Setting up ReadFileBinaryByIdFn. Temp storage URI: {}, Force temp storage: {}",
                null != tempStorageURI ? tempStorageURI.get() : "null",
                forceTempStorage);
        fileReader = connector.readFileBinariesByIds()
                .enableForceTempStorage(forceTempStorage);
        if (null != tempStorageURI && tempStorageURI.isAccessible()) {
            LOG.info("Temp storage configured: {}", tempStorageURI.get());
            URI tempStorage = new URI(tempStorageURI.get());
            fileReader = fileReader.withTempStoragePath(tempStorage);
        }
    }

    @ProcessElement
    public void processElement(@Element Iterable<Item> element,
                               OutputReceiver<FileBinary> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "Batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
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

        // check that ids are provided + remove duplicate ids
        Map<Long, Item> internalIdMap = new HashMap<>(10);
        Map<String, Item> externalIdMap = new HashMap<>(10);
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
        LOG.info(batchLogPrefix + "Received {} file items to read", internalIdMap.size() + externalIdMap.size());
        LOG.debug(batchLogPrefix + "Input items: {}",
                ImmutableList.copyOf(element).toString());

        List<ResponseItems<FileBinary>> requestResult =
                this.sendRequest(internalIdMap.keySet(), externalIdMap.keySet(), projectConfig, batchLogPrefix);

        int retries = 0;

        /*
        Responses from readFileBinaryById will be a single item in case of an error. Check that item for success,
        missing items and duplicates.
         */
        // if the request result is false, we have duplicates and/or missing items.
        while (!requestResult.isEmpty() && !requestResult.get(0).isSuccessful() && retries <= MAX_RETRIES) {
            LOG.info(batchLogPrefix + "Read file request failed. Removing duplicates and missing items and retrying the request");
            List<Item> duplicates = ItemParser.parseItems(requestResult.get(0).getDuplicateItems());
            List<Item> missing = ItemParser.parseItems(requestResult.get(0).getMissingItems());
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

            requestResult = this.sendRequest(internalIdMap.keySet(), externalIdMap.keySet(), projectConfig, batchLogPrefix);
            retries++;
        }

        if (!requestResult.isEmpty() && !requestResult.get(0).isSuccessful()) {
            LOG.error("Failed to read file binaries.", requestResult.get(0).getResponseBodyAsString());
            throw new Exception("Failed to delete items. " + requestResult.get(0).getResponseBodyAsString());
        }

        // output the items (excluding duplicates and missing items)
        for (ResponseItems<FileBinary> responseItems : requestResult) {
            if (responseItems.isSuccessful()) {
                for (FileBinary fileBinary: responseItems.getResultsItems()) {
                    outputReceiver.output(fileBinary);
                }
            }
        }
        downloadBatchDuration.update(Instant.now().toEpochMilli() - startInstant.toEpochMilli());
        LOG.info(batchLogPrefix + "Completed download of {} files within a duration of {}.",
                requestResult.size(),
                Duration.between(startInstant, Instant.now()).toString());
    }

    private List<ResponseItems<FileBinary>> sendRequest(Iterable<Long> internalIds, Iterable<String> externalIds,
                                                        ProjectConfig config, String batchLogPrefix) throws Exception {
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

        LOG.debug(batchLogPrefix + "Built read file items request for {} items", items.size());

        // do not send empty requests.
        if (items.isEmpty()) {
            LOG.warn(batchLogPrefix + "Tried to send empty delete request. Will skip this request."
                    + " Items size: {}", items.size());
            return ImmutableList.of();
        }

        // post delete request and monitor for known constraint violations (duplicates, missing, etc).
        List<ResponseItems<FileBinary>> requestResult = fileReader.readFileBinaries(request);
        LOG.info(batchLogPrefix + "Read file items request sent. Result returned: {}", requestResult.size());

        return requestResult;
    }
}
