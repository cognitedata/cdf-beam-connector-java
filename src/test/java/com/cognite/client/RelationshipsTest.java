package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Relationship;
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

class RelationshipsTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteRelationships() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteRelationships() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting relationships.");
            List<Relationship> upsertRelationshipsList = DataGenerator.generateRelationships(9876);
            client.relationships().upsert(upsertRelationshipsList);
            LOG.info(loggingPrefix + "Finished upserting relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading relationships.");
            List<Relationship> listRelationshipsResults = new ArrayList<>();
            client.relationships()
                    .list(RequestParameters.create()
                            )
                    .forEachRemaining(relationships -> listRelationshipsResults.addAll(relationships));
            LOG.info(loggingPrefix + "Finished reading relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listRelationshipsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.relationships().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting relationships. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertRelationshipsList.size(), listRelationshipsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteEvents() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteEvents() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting events.");
            List<Event> upsertEventsList = DataGenerator.generateEvents(16800);
            client.events().upsert(upsertEventsList);
            LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start listing events.");
            List<Event> listEventsResults = new ArrayList<>();
            client.events()
                    .list(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listEventsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished listing events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start retrieving events.");
            List<Item> eventItems = new ArrayList<>();
            listEventsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> eventItems.add(item));

            List<Event> retrievedEvents = client.events().retrieve(eventItems);
            LOG.info(loggingPrefix + "Finished retrieving events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            retrievedEvents.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.events().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertEventsList.size(), listEventsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(eventItems.size(), retrievedEvents.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeAggregateAndDeleteEvents() {
        int noItems = 745;
        Instant startInstant = Instant.now();

        String loggingPrefix = "UnitTest - writeAggregateAndDeleteEvents() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting events.");
            List<Event> upsertEventsList = DataGenerator.generateEvents(noItems);
            client.events().upsert(upsertEventsList);
            LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(10000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start aggregating events.");
            Aggregate aggregateResult = client.events()
                    .aggregate(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue));
            LOG.info(loggingPrefix + "Aggregate results: {}", aggregateResult);
            LOG.info(loggingPrefix + "Finished aggregating events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start reading events.");
            List<Event> listEventsResults = new ArrayList<>();
            client.events()
                    .list(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(events -> listEventsResults.addAll(events));
            LOG.info(loggingPrefix + "Finished reading events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting events.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listEventsResults.stream()
                    .map(event -> Item.newBuilder()
                            .setExternalId(event.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.events().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertEventsList.size(), listEventsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

}