package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Label;
import com.cognite.client.dto.SecurityCategory;
import com.cognite.client.util.DataGenerator;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityCategoriesTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeListAndDeleteSecurityCategories() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeListAndDeleteSecurityCategories() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start creating security categories.");
            List<SecurityCategory> createSecurityCategoriesList = DataGenerator.generateSecurityGroups(20);
            client.securityCategories().create(createSecurityCategoriesList);
            LOG.info(loggingPrefix + "Finished creating labels. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(5000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading security categories.");
            List<SecurityCategory> listSecurityCategoriesResults = new ArrayList<>();
            client.securityCategories()
                    .list(RequestParameters.create())
                    .forEachRemaining(labels -> listSecurityCategoriesResults.addAll(labels));
            LOG.info(loggingPrefix + "Finished reading security categories. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting labels.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSecurityCategoriesResults.stream()
                    .map(securityCategories -> Item.newBuilder()
                            .setId(securityCategories.getId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.securityCategories().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting security categories. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(createSecurityCategoriesList.size(), listSecurityCategoriesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

}