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

import com.cognite.client.dto.Asset;
import com.cognite.beam.io.fn.ResourceType;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.parser.AssetParser;
import com.google.auto.value.AutoValue;

import java.util.Iterator;
import java.util.List;

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

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Assets build();
    }
}
