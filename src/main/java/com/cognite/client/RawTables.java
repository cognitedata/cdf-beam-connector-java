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
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.EventParser;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import jdk.nashorn.internal.ir.annotations.Immutable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite events api endpoint.
 *
 * It provides methods for reading and writing {@link Event}.
 */
@AutoValue
public abstract class RawTables extends ApiBase {

    private static Builder builder() {
        return new AutoValue_RawTables.Builder();
    }

    /**
     * Constructs a new {@link RawTables} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static RawTables of(CogniteClient client) {
        return RawTables.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all tables (names) in a database.
     *
     * @param dbName the data base to list tables for.
     * @return an {@link Iterator} to page through the table names.
     * @throws Exception
     */
    public Iterator<List<String>> list(String dbName) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ResultFutureIterator<String> futureIterator =
                connector.readRawTableNames(dbName, getClient().buildProjectConfig())
                        .withExecutorService(getClient().getExecutorService())
                        .withHttpClient(getClient().getHttpClient());

        return FanOutIterator.of(ImmutableList.of(futureIterator));
    }

    /**
     * Creates tables in a Raw database.
     *
     * @param dbName The Raw database to create tables in.
     * @param tables The tables to create.
     * @param ensureParent If set to true, will create the database if it doesn't exist from before.
     * @return The upserted events.
     * @throws Exception
     */
    public List<String> create(String dbName, List<String> tables, boolean ensureParent) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeRawTableNames(dbName)
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());


        //todo implement
        return Collections.emptyList();
    }

    /**
     * Deletes a set of tables from a Raw database.
     *
     * @param dbName The Raw database to create tables in.
     * @param tables The tables to delete.
     * @return The deleted events via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(String dbName, List<String> tables) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteEvents()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildProjectConfig())
                .addParameter("ignoreUnknownIds", true);

        // todo implement

        return Collections.emptyList();
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract RawTables build();
    }
}
