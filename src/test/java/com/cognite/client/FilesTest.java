package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.util.DataGenerator;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilesTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteFiles() {
        Instant startInstant = Instant.now();
        byte[] fileByteA = new byte[0];
        byte[] fileByteB = new byte[0];
        try {
            fileByteA = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/csv-data.txt"));
            fileByteB = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/csv-data-bom.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<FileMetadata> fileMetadataList = DataGenerator.generateFileHeaderObjects(2);
        List<FileContainer> fileContainerInput = new ArrayList<>();
        for (FileMetadata fileMetadata:  fileMetadataList) {
            FileContainer fileContainer = FileContainer.newBuilder()
                    .setFileMetadata(fileMetadata)
                    .setFileBinary(FileBinary.newBuilder()
                            .setBinary(ByteString.copyFrom(ThreadLocalRandom.current().nextBoolean() ? fileByteA : fileByteB)))
                    .build();
            fileContainerInput.add(fileContainer);
        }

        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteFiles() -";
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        try {
            LOG.info(loggingPrefix + "Start uploading file binaries.");
            List<FileMetadata> uploadFileList = client.files().upload(fileContainerInput);
            LOG.info(loggingPrefix + "Finished uploading file binaries. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            Thread.sleep(2000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading file metadata.");
            List<FileMetadata> listFilesResults = new ArrayList<>();
            client.files()
                    .list(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(files -> listFilesResults.addAll(files));
            LOG.info(loggingPrefix + "Finished reading files. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            /*
            LOG.info(loggingPrefix + "Start downloading file binaries.");
            List<FileMetadata> listFilesResults = new ArrayList<>();
            client.files()
                    .list(RequestParameters.create()
                            .withFilterParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(files -> listFilesResults.addAll(files));
            LOG.info(loggingPrefix + "Finished reading files. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");




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

             */
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteFiles() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeEditAndDeleteEvents() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting events.");
            List<Event> upsertEventsList = DataGenerator.generateEvents(123);
            List<Event> upsertedEvents = client.events().upsert(upsertEventsList);
            LOG.info(loggingPrefix + "Finished upserting events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start updating events.");
            List<Event> editedEventsInput = upsertedEvents.stream()
                    .map(event -> event.toBuilder()
                            .setDescription(StringValue.of("new-value"))
                            .clearSubtype()
                            .clearMetadata()
                            .putMetadata("new-key", "new-value")
                            .build())
                    .collect(Collectors.toList());

            List<Event> eventUpdateResults = client.events().upsert(editedEventsInput);
            LOG.info(loggingPrefix + "Finished updating events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start update replace events.");
            client = client
                    .withClientConfig(ClientConfig.create()
                            .withUpsertMode(UpsertMode.REPLACE));

            List<Event> eventReplaceResults = client.events().upsert(editedEventsInput);
            LOG.info(loggingPrefix + "Finished update replace events. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(3000); // wait for eventual consistency

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

            BooleanSupplier updateCondition = () -> {
                for (Event event : eventUpdateResults)  {
                    if (event.getDescription().getValue().equals("new-value")
                            && event.hasSubtype()
                            && event.containsMetadata("new-key")
                            && event.containsMetadata(DataGenerator.sourceKey)) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            BooleanSupplier replaceCondition = () -> {
                for (Event event : eventReplaceResults)  {
                    if (event.getDescription().getValue().equals("new-value")
                            && !event.hasSubtype()
                            && event.containsMetadata("new-key")
                            && !event.containsMetadata(DataGenerator.sourceKey)) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            assertTrue(updateCondition, "Event update not correct");
            assertTrue(replaceCondition, "Event replace not correct");

            assertEquals(upsertEventsList.size(), listEventsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteFiles() {
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
    void writeAggregateAndDeleteFiles() {
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