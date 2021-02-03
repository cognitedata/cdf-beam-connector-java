package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.util.DataGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RawTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteRaw() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteRaw() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start creating raw databases.");
            int noDatabases = 3;
            List<String> createDatabasesList = DataGenerator.generateListString(noDatabases);
            client.raw().databases().create(createDatabasesList);
            LOG.info(loggingPrefix + "Finished creating raw databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start creating raw tables.");
            Map<String, List<String>> createTablesLists = new HashMap<>();
            int noTables = 10;
            for (String dbName : createDatabasesList) {
                createTablesLists.put(dbName, DataGenerator.generateListString(noTables));
                client.raw().tables().create(dbName, createTablesLists.get(dbName), false);
            }
            LOG.info(loggingPrefix + "Finished creating raw tables. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(10000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading raw databases.");
            List<String> listDatabaseResults = new ArrayList<>();
            client.raw().databases()
                    .list()
                    .forEachRemaining(databases -> listDatabaseResults.addAll(databases));
            LOG.info(loggingPrefix + "Finished reading databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start reading raw tables.");
            Map<String, List<String>> listTablesResults = new HashMap<>();
            for (String dbName : createDatabasesList) {
                List<String> tablesResults = new ArrayList<>();
                client.raw().tables()
                        .list(dbName)
                        .forEachRemaining(databases -> tablesResults.addAll(databases));
                listTablesResults.put(dbName, tablesResults);
            }
            LOG.info(loggingPrefix + "Finished reading databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting raw tables.");
            Map<String, List<String>> deleteTablesResults = new HashMap<>();
            for (String dbName : createDatabasesList) {
                List<String> deleteItemsInput = new ArrayList<>();
                deleteItemsInput.addAll(createTablesLists.get(dbName));

                deleteTablesResults.put(dbName, client.raw().tables().delete(dbName, deleteItemsInput));
            }
            LOG.info(loggingPrefix + "Finished deleting raw tables. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting raw databases.");
            List<String> deleteItemsInput = new ArrayList<>();
            deleteItemsInput.addAll(createDatabasesList);

            List<String> deleteItemsResults = client.raw().databases().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting raw databases. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            // assertEquals(createDatabasesList.size(), listDatabaseResults.size());
            for (String dbName : createDatabasesList) {
                assertEquals(createTablesLists.get(dbName).size(), listTablesResults.get(dbName).size());
                assertEquals(createTablesLists.get(dbName).size(), deleteTablesResults.get(dbName).size());
            }
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

}