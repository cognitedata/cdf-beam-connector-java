package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Asset;
import com.cognite.client.dto.Item;
import com.cognite.client.util.DataGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AssetsTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteAssets() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteAssets() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting assets.");
            List<Asset> upsertAssetsList = DataGenerator.generateAssetHierarchy(1680);
            client.assets().upsert(upsertAssetsList);
            LOG.info(loggingPrefix + "Finished upserting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading assets.");
            List<Asset> listAssetsResults = new ArrayList<>();
            client.assets()
                    .list(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listAssetsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished reading assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.assets().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertAssetsList.size(), listAssetsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteAssets() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeRetrieveAndDeleteAssets() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting assets.");
            List<Asset> upsertAssetsList = DataGenerator.generateAssetHierarchy(1680);
            client.assets().upsert(upsertAssetsList);
            LOG.info(loggingPrefix + "Finished upserting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start listing assets.");
            List<Asset> listAssetsResults = new ArrayList<>();
            client.assets()
                    .list(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listAssetsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished listing assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start retrieving assets.");
            List<Item> assetItems = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> assetItems.add(item));

            List<Asset> retrievedAssets = client.assets().retrieve(assetItems);
            LOG.info(loggingPrefix + "Finished retrieving assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.assets().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertAssetsList.size(), listAssetsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(assetItems.size(), retrievedAssets.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeAggregateAndDeleteAssets() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeAggregateAndDeleteAssets() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting assets.");
            List<Asset> upsertAssetsList = DataGenerator.generateAssetHierarchy(1680);
            client.assets().upsert(upsertAssetsList);
            LOG.info(loggingPrefix + "Finished upserting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start aggregating assets.");
            Aggregate aggregateResult = client.assets()
                    .aggregate(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue));
            LOG.info(loggingPrefix + "Aggregate results: {}", aggregateResult);
            LOG.info(loggingPrefix + "Finished aggregating assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start listing assets.");
            List<Asset> listAssetsResults = new ArrayList<>();
            client.assets()
                    .list(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listAssetsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished listing assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listAssetsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.assets().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting assets. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertAssetsList.size(), listAssetsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }
}