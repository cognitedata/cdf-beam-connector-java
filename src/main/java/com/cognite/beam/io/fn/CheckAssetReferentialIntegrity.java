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

package com.cognite.beam.io.fn;

import com.cognite.client.dto.Asset;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This function checks for a single asset hierarchy per key:
 * - A single root node.
 * - All other assets references an asset in this collection
 */
public class CheckAssetReferentialIntegrity extends DoFn<KV<String, Iterable<Asset>>, KV<String, Asset>> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @ProcessElement
    public void processElement(@Element KV<String, Iterable<Asset>> elements,
                               OutputReceiver<KV<String, Asset>> outputReceiver) throws Exception {
        final String batchLogPrefix = "Batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";

        Map<String, Asset> inputMap = new HashMap<>();
        List<Asset> invalidReferenceList = new ArrayList<>(50);
        List<Asset> rootNodeList = new ArrayList<>(2);

        LOG.info(batchLogPrefix + "Checking asset input table for integrity. Asset table root externalId: "
                + elements.getKey());
        for (Asset element : elements.getValue()) {
            if (element.getParentExternalId().getValue().isEmpty()) {
                rootNodeList.add(element);
            }
            inputMap.put(element.getExternalId().getValue(), element);
        }

        for (Asset element : elements.getValue()) {
            if (!element.getParentExternalId().getValue().isEmpty()
                    && !inputMap.containsKey(element.getParentExternalId().getValue())) {
                invalidReferenceList.add(element);
            }
        }

        if (rootNodeList.size() != 1) {
            String errorMessage = batchLogPrefix + "Found " + rootNodeList.size() + " root nodes.";
            StringBuilder message = new StringBuilder()
                    .append(errorMessage).append(System.lineSeparator());
            if (rootNodeList.size() > 11) {
                rootNodeList = rootNodeList.subList(0, 10);
            }
            message.append("Root nodes (max 10 displayed): " + System.lineSeparator());
            for (Asset item : rootNodeList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("externalId: [").append(item.getExternalId().getValue()).append("]").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("parentExternalId: [").append(item.getParentExternalId().getValue()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription().getValue()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.error(message.toString());
            throw new Exception(errorMessage);
        }

        if (!invalidReferenceList.isEmpty()) {
            StringBuilder message = new StringBuilder();
            String errorMessage = batchLogPrefix + "Found " + invalidReferenceList.size() + " assets with invalid parent reference.";
            message.append(errorMessage).append(System.lineSeparator());
            if (invalidReferenceList.size() > 11) {
                invalidReferenceList = invalidReferenceList.subList(0, 10);
            }
            message.append("Items with invalid parentExternalId (max 10 displayed): " + System.lineSeparator());
            for (Asset item : invalidReferenceList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("externalId: [").append(item.getExternalId().getValue()).append("]").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("parentExternalId: [").append(item.getParentExternalId().getValue()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription().getValue()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.error(message.toString());
            throw new Exception(errorMessage);
        }

        LOG.info(batchLogPrefix + "Asset input table contains a single root node and valid parent references.");

        for (Asset element : elements.getValue()) {
            outputReceiver.output(KV.of(elements.getKey(), element));
        }
    }
}
