package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
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
}