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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
