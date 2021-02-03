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
import com.cognite.client.dto.RawRow;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.RawParser;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * This class represents the Cognite Raw rows endpoint.
 *
 * It provides methods for interacting with the Raw row endpoint.
 */
@AutoValue
public abstract class RawRows extends ApiBase {

    private static Builder builder() {
        return new AutoValue_RawRows.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(RawRows.class);

    /**
     * Constructs a new {@link RawRows} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static RawRows of(CogniteClient client) {
        return RawRows.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns a set of rows from a table.
     *
     * @param dbName the database to list rows from.
     * @param tableName the table to list rows from.
     * @param requestParameters the column and filter specification for the rows.
     * @return an {@link Iterator} to page through the rows.
     * @throws Exception
     */
    public Iterator<List<RawRow>> list(String dbName,
                                       String tableName,
                                       RequestParameters requestParameters) throws Exception {

        //return FanOutIterator.of(ImmutableList.of(futureIterator));
        return Collections.emptyIterator();
    }

    /**
     * Returns a set of rows from a table.
     *
     * @param dbName the database to list rows from.
     * @param tableName the table to list rows from.
     * @param requestParameters the column and filter specification for the rows.
     * @return an {@link Iterator} to page through the rows.
     * @throws Exception
     */
    public Iterator<List<RawRow>> list(String dbName,
                                       String tableName,
                                       RequestParameters requestParameters,
                                       String... cursors) throws Exception {

        return AdapterIterator.of(listJson(ResourceType.RAW_ROW, requestParameters, "cursor", cursors),
                RawRowParser.of(dbName, tableName));
    }

    /**
     * Retrieves cursors for parallel retrieval of rows from Raw.
     *
     * This is intended for advanced use cases where you need granular control of the parallel retrieval from
     * Raw--for example in distributed processing frameworks. Most scenarios should just use
     * {@code list} directly as that will automatically handle parallelization for you.
     *
     * @param dbName The database to retrieve row cursors from.
     * @param tableName The table to retrieve row cursors from.
     * @param requestParameters Hosts query parameters like max and min time stamps and number of cursors to request.
     * @return A list of cursors.
     * @throws Exception
     */
    public List<String> retrieveCursors(String dbName,
                                        String tableName,
                                        RequestParameters requestParameters) throws Exception {
        String loggingPrefix = "retrieveCursors() - ";
        Instant startInstant = Instant.now();
        Preconditions.checkArgument(dbName != null && !dbName.isEmpty(),
                "You must specify a data base name.");
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
                "You must specify a table name.");

        // Build request
        RequestParameters request = requestParameters
                .withRootParameter("dbName", dbName)
                .withRootParameter("tableName", tableName);

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ItemReader<String> cursorItemReader = connector.readCursorsRawRows();
        List<String> results = cursorItemReader
                .getItems(addAuthInfo(request))
                .getResultsItems();

        LOG.info(loggingPrefix + "Retrieved {} cursors. Duration: {}",
                results.size(),
                Duration.between(startInstant, Instant.now()));

        return results;
    }

    /**
     * Creates tables in a Raw database.
     *
     * @param dbName The Raw database to create tables in.
     * @param tables The tables to create.
     * @param ensureParent If set to true, will create the database if it doesn't exist from before.
     * @return The created table names.
     * @throws Exception
     */
    public List<String> create(String dbName, List<String> tables, boolean ensureParent) throws Exception {
        String loggingPrefix = "create() - ";
        Instant startInstant = Instant.now();
        Preconditions.checkArgument(null!= dbName && !dbName.isEmpty(),
                "Database name cannot be empty.");
        LOG.info(loggingPrefix + "Received {} tables to create in database {}.",
                tables.size(),
                dbName);

        List<String> deduplicated = new ArrayList<>(new HashSet<>(tables));

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeRawTableNames(dbName)
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        List<List<String>> batches = Partition.ofSize(deduplicated, 100);
        for (List<String> batch : batches) {
            List<Map<String, Object>> items = new ArrayList<>();
            for (String table : batch) {
                items.add(ImmutableMap.of("name", table));
            }
            RequestParameters request = addAuthInfo(RequestParameters.create()
                    .withItems(items)
                    .withRootParameter("ensureParent", ensureParent));
            ResponseItems<String> response = createItemWriter.writeItems(request);
            if (!response.isSuccessful()) {
                throw new Exception(String.format(loggingPrefix + "Create table request failed: %s",
                        response.getResponseBodyAsString()));
            }
        }

        LOG.info(loggingPrefix + "Successfully created {} tables in database {}. Duration: {}",
                tables.size(),
                dbName,
                Duration.between(startInstant, Instant.now()));

        return deduplicated;
    }

    /**
     * Deletes a set of tables from a Raw database.
     *
     * @param dbName The Raw database to create tables in.
     * @param tables The tables to delete.
     * @return The deleted tables
     * @throws Exception
     */
    public List<String> delete(String dbName, List<String> tables) throws Exception {
        String loggingPrefix = "delete() - ";
        Instant startInstant = Instant.now();
        Preconditions.checkArgument(null!= dbName && !dbName.isEmpty(),
                "Database name cannot be empty.");
        LOG.info(loggingPrefix + "Received {} tables to delete from database {}.",
                tables.size(),
                dbName);

        List<String> deduplicated = new ArrayList<>(new HashSet<>(tables));

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteRawTableNames(dbName)
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        List<List<String>> batches = Partition.ofSize(deduplicated, 100);
        for (List<String> batch : batches) {
            List<Map<String, Object>> items = new ArrayList<>();
            for (String table : batch) {
                items.add(ImmutableMap.of("name", table));
            }
            RequestParameters request = addAuthInfo(RequestParameters.create()
                    .withItems(items));
            ResponseItems<String> response = deleteItemWriter.writeItems(request);
            if (!response.isSuccessful()) {
                throw new Exception(String.format(loggingPrefix + "Delete table request failed: %s",
                        response.getResponseBodyAsString()));
            }
        }

        LOG.info(loggingPrefix + "Successfully deleted {} tables from database {}. Duration: {}",
                tables.size(),
                dbName,
                Duration.between(startInstant, Instant.now()));

        return deduplicated;
    }

    /*
    Helper class to parse raw rows.
     */
    @AutoValue
    abstract static class RawRowParser implements Function<String, RawRow> {

        private static Builder builder() {
            return new AutoValue_RawRows_RawRowParser.Builder();
        }

        public static RawRowParser of(String dbName, String tableName) {
            return RawRowParser.builder()
                    .setDbName(dbName)
                    .setTableName(tableName)
                    .build();
        }

        abstract String getDbName();
        abstract String getTableName();

        @Override
        public RawRow apply(String json) {
            try {
                return RawParser.parseRawRow(getDbName(), getTableName(), json);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setDbName(String value);
            abstract Builder setTableName(String value);

            abstract RawRowParser build();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract RawRows build();
    }
}
