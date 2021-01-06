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

package com.cognite.client;

import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Asset;
import com.cognite.client.config.ResourceType;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.AssetParser;
import com.cognite.client.servicesV1.parser.EventParser;
import com.google.auto.value.AutoValue;
import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite assets api endpoint.
 *
 * It provides methods for reading and writing {@link com.cognite.client.dto.Asset}.
 */
@AutoValue
public abstract class Assets extends ApiBase {

    private static Builder builder() {
        return new AutoValue_Assets.Builder();
    }

    /**
     * Constructs a new {@link Assets} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static Assets of(CogniteClient client) {
        return Assets.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link Asset} objects that matches the filters set in the {@link RequestParameters}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The assets are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Asset>> list(RequestParameters requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Asset} objects that matches the filters set in the {@link RequestParameters} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Asset>> list(RequestParameters requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.ASSET, requestParameters, partitions), this::parseAsset);
    }

    /**
     * Retrieve assets by id.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved events.
     * @throws Exception
     */
    public List<Asset> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.ASSET, items).stream()
                .map(this::parseAsset)
                .collect(Collectors.toList());
    }

    /**
     * Performs an item aggregation request to Cognite Data Fusion.
     *
     * The default aggregation is a total item count based on the (optional) filters in the request.
     * Multiple aggregation types are supported. Please refer to the Cognite API specification for more information
     * on the possible settings.
     *
     * @param requestParameters The filtering and aggregates specification
     * @return The aggregation results.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    public Aggregate aggregate(RequestParameters requestParameters) throws Exception {
        return aggregate(ResourceType.ASSET, requestParameters);
    }

    // todo make upsert assets + sync assets methods. Must implement sorting + integrity checking

    /**
     * Creates or updates a set of {@link Asset} objects.
     *
     * If it is a new {@link Asset} object (based on {@code id / externalId}, then it will be created.
     *
     * If an {@link Asset} object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * The assets will be checked for integrity and topologically sorted before an ordered upsert operation
     * is started.
     *
     * @param assets The assets to upsert.
     * @return The upserted assets.
     * @throws Exception
     */
    public List<Asset> upsert(List<Asset> assets) throws Exception {


        return Collections.emptyList(); // temp return statement to make the code compile
    }

    /**
     * Checks a collection of assets for integrity. The assets must represent a single, complete
     * hierarchy.
     *
     * This verifies if the collection satisfies the
     * constraints of the Cognite Data Fusion data model if you were to write them using the
     * {@link Assets#upsert(List)} method.
     *
     * The following constraints will be evaluated:
     * - All assets must specify an {@code externalId}.
     * - No duplicates (based on {@code externalId}.
     * - The collection must contain one and only one asset object with no parent reference (representing the root node)
     * - All other assets must contain a valid {@code parentExternalId} reference (no self-references).
     * - No circular references.
     *
     * @param assets A collection of {@link Asset} representing a single, complete asset hierarchy.
     * @return
     */
    public boolean verifyAssetHierarchyIntegrity(List<Asset> assets) {

        return false; // temp return statement to make the code compile
    }

    /**
     * Deletes a set of assets.
     *
     * The events to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * @param items a list of {@link Item} representing the assets (externalId / id) to be deleted
     * @return The deleted events via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> items) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteAssets()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildProjectConfig())
                .withParameter("ignoreUnknownIds", true);

        return deleteItems.deleteItems(items);
    }

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
     *
     * @param assets A collection of {@link Asset} to be topologically sorted.
     * @return The sorted assets collection.
     */
    private List<Asset> sortAssetsForUpsert(Collection<Asset> assets) {
        // todo: Implement sort
        return Collections.emptyList();
    }

    /**
     * Checks the assets for {@code externalId}.
     *
     * @param assets The assets to check.
     * @return true if all assets contain {@code externalId}. False if one or more assets do not have {@code externalId}.
     */
    private boolean checkExternalId(Collection<Asset> assets) {
        String loggingPrefix = "checkExternalId() - ";
        List<Asset> missingExternalIdList = new ArrayList<>(50);

        for (Asset asset : assets) {
            if (!asset.hasExternalId()) {
                missingExternalIdList.add(asset);
            }
        }

        // Report on missing externalId
        if (!missingExternalIdList.isEmpty()) {
            StringBuilder message = new StringBuilder();
            String errorMessage = loggingPrefix + "Found " + missingExternalIdList.size() + " assets missing externalId.";
            message.append(errorMessage).append(System.lineSeparator());
            if (missingExternalIdList.size() > 11) {
                missingExternalIdList = missingExternalIdList.subList(0, 10);
            }
            message.append("Items with missing externalId (max 10 displayed): " + System.lineSeparator());
            for (Asset item : missingExternalIdList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("parentExternalId: [").append(item.getParentExternalId().getValue()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription().getValue()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.error(message.toString());
            return false;
        }

        return true;
    }

    /**
     * Checks the assets for circular references.
     *
     * @param assets The assets to check.
     * @return True if circular references are detected. False if no circular references are detected.
     */
    private boolean checkCircularReferences(Collection<Asset> assets) {
        String loggingPrefix = "checkCircularReferences() - ";
        Map<String, Asset> assetMap = new HashMap<>();
        assets.stream()
                .forEach(asset -> assetMap.put(asset.getExternalId().getValue(), asset));

        // Checking for circular references
        Graph<Asset, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);

        // add vertices
        for (Asset vertex : assetMap.values()) {
            graph.addVertex(vertex);
        }
        // add edges
        for (Asset asset : assetMap.values()) {
            if (asset.hasParentExternalId() && assetMap.containsKey(asset.getParentExternalId().getValue())) {
                graph.addEdge(assetMap.get(asset.getParentExternalId().getValue()), asset);
            }
        }

        CycleDetector<Asset, DefaultEdge> cycleDetector = new CycleDetector<>(graph);
        if (cycleDetector.detectCycles()) {
            Set<String> cycle = new HashSet<>();
            cycleDetector.findCycles().stream().forEach((Asset item) -> cycle.add(item.getExternalId().getValue()));
            String message = loggingPrefix + "Cycles detected. Number of asset in the cycle: " + cycle.size();
            LOG.error(message);
            LOG.error(loggingPrefix + "Cycle: " + cycle.toString());
            return false;
        }

        LOG.info(loggingPrefix + "No cycles detected.");
        return true;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Asset parseAsset(String json) {
        try {
            return AssetParser.parseAsset(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(Asset item) {
        try {
            return AssetParser.toRequestUpdateItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(Asset item) {
        try {
            return AssetParser.toRequestReplaceItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an event. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getAssetId(Asset item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId().getValue());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId().getValue()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Assets build();
    }
}
