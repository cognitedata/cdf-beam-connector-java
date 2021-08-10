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
import org.apache.commons.lang3.RandomStringUtils;
import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This function will sort a collection of assets into the correct order for upsert to CDF.
 *
 * Assets need to be written in a certain order to comply with the hierarchy constraints of CDF. In short, if an asset
 * references a parent (either via id or externalId), then that parent must exist either in CDF or in the same write
 * batch. Hence, a collection of assets must be written to CDF in topological order.
 *
 * This function requires that the input collection is complete. That is, all assets required to traverse the hierarchy
 * from the root node to the leaves must either exist in CDF and/or in the input collection.
 *
 * The sorting algorithm employed is a naive breadth-first O(n * depth(n)):
 * while (items in inputCollection) {
 *     for (items in inputCollection) {
 *         if (item references unknown OR item references null OR item references id) : write item and remove from inputCollection
 *     }
 * }
 *
 * Other requirements for the input:
 * - Assets must have externalId set.
 * - If both parentExternalId and parentId are set, then parentExternalId takes precedence in the sort.
 */
public class SortAssetsForUpsertFn extends DoFn<Iterable<Asset>, List<Asset>> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @ProcessElement
    public void processElement(@Element Iterable<Asset> elements,
                               OutputReceiver<List<Asset>> outputReceiver) throws Exception {
        final String batchLogPrefix = "Batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        List<Asset> assetList = new ArrayList<>();
        Map<String, Asset> inputMap = new HashMap<>();
        List<Asset> missingExternalIdList = new ArrayList<>(50);
        List<String> selfReferenceList = new ArrayList<>(50);
        List<String> duplicatesList = new ArrayList<>();

        LOG.info(batchLogPrefix + "Checking assets against constraints.");
        for (Asset element : elements) {
            if (!element.hasExternalId()) {
                missingExternalIdList.add(element);
            } else if (element.hasParentExternalId()
                    && element.getParentExternalId().equals(element.getExternalId())) {
                selfReferenceList.add(element.getExternalId());
            } else if (element.hasExternalId() && inputMap.containsKey(element.getExternalId())) {
                duplicatesList.add(element.getExternalId());
            }
            inputMap.put(element.getExternalId(), element);
        }

        if (!missingExternalIdList.isEmpty()) {
            StringBuilder message = new StringBuilder();
            String errorMessage = batchLogPrefix + "Found " + missingExternalIdList.size() + " assets missing externalId.";
            message.append(errorMessage).append(System.lineSeparator());
            if (missingExternalIdList.size() > 11) {
                missingExternalIdList = missingExternalIdList.subList(0, 10);
            }
            message.append("Items with missing externalId (max 10 displayed): " + System.lineSeparator());
            for (Asset item : missingExternalIdList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("parentExternalId: [").append(item.getParentExternalId()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.error(message.toString());
            throw new Exception(errorMessage);
        }
        if (!selfReferenceList.isEmpty()) {
            StringBuilder message = new StringBuilder();
            String errorMessage = batchLogPrefix + "Found " + selfReferenceList.size()
                    + " assets with self-referencing parentId.";
            message.append(errorMessage).append(System.lineSeparator());
            if (selfReferenceList.size() > 11) {
                selfReferenceList = selfReferenceList.subList(0, 10);
            }
            message.append("Items with self-referencing parentExternalId (max 10 displayed by externalId): "
                    + System.lineSeparator());
            selfReferenceList.forEach(item -> message.append("[").append(item).append("], "));
            LOG.error(message.toString());
            throw new Exception(errorMessage);
        }
        if (!duplicatesList.isEmpty()) {
            StringBuilder message = new StringBuilder();
            String errorMessage = batchLogPrefix + "Found " + duplicatesList.size() + " duplicates.";
            message.append(errorMessage).append(System.lineSeparator());
            if (duplicatesList.size() > 11) {
                duplicatesList = duplicatesList.subList(0, 10);
            }
            message.append("Duplicate items (max 10 displayed by externalId): " + System.lineSeparator());
            duplicatesList.forEach(item -> message.append("[").append(item).append("], "));
            LOG.error(message.toString());
            throw new Exception(errorMessage);
        }

        LOG.info(batchLogPrefix + "All assets contain externalId, no self-references detected, no duplicates detected.");

        // Checking for circular references
        Graph<Asset, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);
        // add vertices
        for (Asset vertex : inputMap.values()) {
            graph.addVertex(vertex);
        }
        // add edges
        for (Asset asset : inputMap.values()) {
            if (asset.hasParentExternalId() && inputMap.containsKey(asset.getParentExternalId())) {
                graph.addEdge(inputMap.get(asset.getParentExternalId()), asset);
            }
        }
        CycleDetector<Asset, DefaultEdge> cycleDetector = new CycleDetector<>(graph);
        if (cycleDetector.detectCycles()) {
            Set<String> cycle = new HashSet<>();
            cycleDetector.findCycles().stream().forEach((Asset item) -> cycle.add(item.getExternalId()));
            String message = batchLogPrefix + "Cycles detected. Number of asset in the cycle: " + cycle.size();
            LOG.error(message);
            LOG.error(batchLogPrefix + "Cycle: " + cycle.toString());
            throw new Exception(message);
        }
        LOG.info(batchLogPrefix + "No cycles detected.");
        LOG.info(batchLogPrefix + "Number of assets received: {}", inputMap.size());

        LOG.info(batchLogPrefix + "Starting sort.");
        while (!inputMap.isEmpty()) {
            int startInputMapSize = inputMap.size();
            LOG.info(batchLogPrefix + "Starting new sort iteration. Assets left to sort: {}", inputMap.size());
            for (Iterator<Asset> iterator = inputMap.values().iterator(); iterator.hasNext();) {
                Asset asset = iterator.next();
                if (asset.hasParentExternalId()) {
                    // Check if the parent asset exists in the input collection. If no, it is safe to write the asset.
                    if (!inputMap.containsKey(asset.getParentExternalId())) {
                        assetList.add(asset);
                        iterator.remove();
                    }
                } else {
                    // Asset either has no parent reference or references an (internal) id.
                    // Null parent is safe to write and (internal) id is assumed to already exist in cdf--safe to write.
                    assetList.add(asset);
                    iterator.remove();
                }
            }
            LOG.debug(batchLogPrefix + "Finished sort iteration. Assets left to sort: {}", inputMap.size());
            if (startInputMapSize == inputMap.size()) {
                String message = batchLogPrefix + "Possible circular reference detected when sorting assets, aborting.";
                LOG.error(message);
                throw new Exception(message);
            }
        }
        LOG.info(batchLogPrefix + "Sort finished.");
        outputReceiver.output(assetList);
    }
}
