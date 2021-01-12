package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.SequenceMetadata;
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

class SequencesTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeReadAndDeleteSequences() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeReadAndDeleteSequences() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting sequences.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(128);
            client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading sequences.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(RequestParameters.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished reading sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSequencesResults.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteSequences() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - writeEditAndDeleteSequences() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting sequences.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(123);
            List<SequenceMetadata> upsertedTimeseries = client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start updating sequences.");
            List<SequenceMetadata> editedSequencesInput = upsertedTimeseries.stream()
                    .map(sequences -> sequences.toBuilder()
                            .setDescription(StringValue.of("new-value"))
                            .clearMetadata()
                            .putMetadata("new-key", "new-value")
                            .build())
                    .collect(Collectors.toList());

            List<SequenceMetadata> sequencesUpdateResults = client.sequences().upsert(editedSequencesInput);
            LOG.info(loggingPrefix + "Finished updating sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start update replace sequences.");
            client = client
                    .withClientConfig(ClientConfig.create()
                            .withUpsertMode(UpsertMode.REPLACE));

            List<SequenceMetadata> sequencesReplaceResults = client.sequences().upsert(editedSequencesInput);
            LOG.info(loggingPrefix + "Finished update replace sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(3000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start reading sequences.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(RequestParameters.create()
                            .withFilterMetadataParameter("new-key", "new-value"))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished reading sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSequencesResults.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            BooleanSupplier updateCondition = () -> {
                for (SequenceMetadata sequences : sequencesUpdateResults)  {
                    if (sequences.getDescription().getValue().equals("new-value")
                            && sequences.containsMetadata("new-key")
                            && sequences.containsMetadata(DataGenerator.sourceKey)) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            BooleanSupplier replaceCondition = () -> {
                for (SequenceMetadata sequences : sequencesReplaceResults)  {
                    if (sequences.getDescription().getValue().equals("new-value")
                            && sequences.containsMetadata("new-key")
                            && !sequences.containsMetadata(DataGenerator.sourceKey)) {
                        // all good
                    } else {
                        return false;
                    }
                }
                return true;
            };

            assertTrue(updateCondition, "Sequences update not correct");
            assertTrue(replaceCondition, "Sequences replace not correct");

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeRetrieveAndDeleteSequences() {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeReadAndDeleteSequences() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting sequences.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(168);
            client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(15000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start listing sequences.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(RequestParameters.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished listing sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start retrieving sequences.");
            List<Item> sequencesItems = new ArrayList<>();
            listSequencesResults.stream()
                    .map(aequences -> Item.newBuilder()
                            .setExternalId(aequences.getExternalId().getValue())
                            .build())
                    .forEach(item -> sequencesItems.add(item));

            List<SequenceMetadata> retrievedSequences = client.sequences().retrieve(sequencesItems);
            LOG.info(loggingPrefix + "Finished retrieving sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            retrievedSequences.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
            assertEquals(sequencesItems.size(), retrievedSequences.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeAggregateAndDeleteSequences() {
        int noItems = 145;
        Instant startInstant = Instant.now();

        String loggingPrefix = "UnitTest - writeAggregateAndDeleteSequences() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start upserting sequences.");
            List<SequenceMetadata> upsertSequencesList = DataGenerator.generateSequenceMetadata(noItems);
            client.sequences().upsert(upsertSequencesList);
            LOG.info(loggingPrefix + "Finished upserting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            Thread.sleep(10000); // wait for eventual consistency

            LOG.info(loggingPrefix + "Start aggregating sequences.");
            Aggregate aggregateResult = client.sequences()
                    .aggregate(RequestParameters.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue));
            LOG.info(loggingPrefix + "Aggregate results: {}", aggregateResult);
            LOG.info(loggingPrefix + "Finished aggregating sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start reading sequences.");
            List<SequenceMetadata> listSequencesResults = new ArrayList<>();
            client.sequences()
                    .list(RequestParameters.create()
                            .withFilterMetadataParameter("source", DataGenerator.sourceValue))
                    .forEachRemaining(sequences -> listSequencesResults.addAll(sequences));
            LOG.info(loggingPrefix + "Finished reading sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            LOG.info(loggingPrefix + "Start deleting sequences.");
            List<Item> deleteItemsInput = new ArrayList<>();
            listSequencesResults.stream()
                    .map(sequences -> Item.newBuilder()
                            .setExternalId(sequences.getExternalId().getValue())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));

            List<Item> deleteItemsResults = client.sequences().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting sequences. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(upsertSequencesList.size(), listSequencesResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

}