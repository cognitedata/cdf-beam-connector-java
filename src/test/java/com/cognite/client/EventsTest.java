package com.cognite.client;

import com.cognite.beam.io.dto.Event;
import com.cognite.beam.io.dto.Item;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ClientConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EventsTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteEvents() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeAndReadEvents() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting events.");
            List<Event> upsertEventsList = TestUtils.generateEvents(16800);
            client.events().upsert(upsertEventsList);
            LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading events.");
            List<Event> listEventsResults = new ArrayList<>();
            client.events()
                    .list(RequestParameters.create()
                            .withFilterParameter("source", TestUtils.sourceValue))
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