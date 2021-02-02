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

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.SecurityCategory;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.SecurityCategoryParser;
import com.google.auto.value.AutoValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite security categories api endpoint.
 *
 * It provide methods for reading and writing {@link SecurityCategory}
 */
@AutoValue
public abstract class SecurityCategories extends ApiBase {

    private static Builder builder() {
        return new AutoValue_SecurityCategories.Builder();
    }

    /**
     * Construct a new {@link SecurityCategories} object using the provided configuration-
     *
     * This method is intended for internal use--SDK client should always use {@link CogniteClient} as the entry point
     * to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The labels api object.
     */
    public static SecurityCategories of(CogniteClient client) {
        return SecurityCategories.builder()
                .setClient(client)
                .build();
    }

    public Iterator<List<SecurityCategory>> list(RequestParameters requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    public Iterator<List<SecurityCategory>> list(RequestParameters requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.SECURITY_CATEGORY, requestParameters, partitions), this::parseSecurityCategories);
    }

    public List<SecurityCategory> create(List<SecurityCategory> securityCategories) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeSecurityCategories()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        UpsertItems<SecurityCategory> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildProjectConfig())
                .withIdFunction(this::getSecurityCategoryName);

        return upsertItems.create(securityCategories).stream()
                .map(this::parseSecurityCategories)
                .collect(Collectors.toList());
    }

    public List<Item> delete(List<Item> securityCategories) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteSecurityCategories()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildProjectConfig());

        return deleteItems.deleteItems(securityCategories);
    }

    private SecurityCategory parseSecurityCategories(String json) {
        try {
            return SecurityCategoryParser.parseSecurityCategory(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestInsertItem(SecurityCategory item) {
        try {
            return SecurityCategoryParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<String> getSecurityCategoryName(SecurityCategory item) {
        try {
            return Optional.of(item.getName());
        } catch (Exception e) {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract SecurityCategories build();
    }
}
