package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Label;
import com.cognite.client.servicesV1.Connector;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.util.DataGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityMatchingTest {
    final ObjectMapper mapper = new ObjectMapper();
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void matchEntitiesStruct() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - matchEntitiesStruct() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        try {
            LOG.info(loggingPrefix + "Start upserting labels.");
            List<Label> upsertLabelsList = DataGenerator.generateLabels(58);
            client.labels().upsert(upsertLabelsList);
            LOG.info(loggingPrefix + "Finished upserting labels. Duration: {}",
                    Duration.between(startInstant, Instant.now()));



            assertEquals(upsertLabelsList.size(), listLabelsResults.size());
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    private long trainMatchingModel(String featureType, String loggingPrefix) throws Exception {
        // Set up the main data objects to use during the test
        LOG.info(loggingPrefix + "Building test data objects...");
        ImmutableList<Struct> source = generateSourceStructs();
        ImmutableList<Struct> target = generateTargetTrainingStructs();

        String[] featureTypes = {"simple", "bigram", "frequency-weighted-bigram", "bigram-extra-tokenizers"};

        // Train the matching model
        LOG.info(loggingPrefix + "Training matching model...");
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        Connector<String> entityMatchConnector = connectorService.entityMatcherFit();
        long modelId = -1L;
        RequestParameters entityMatchFitRequest = RequestParameters.create()
                .withRootParameter("sources",  source)
                .withRootParameter("targets", target)
                .withRootParameter("matchFields", ImmutableList.of(
                        ImmutableMap.of("source", "name", "target", "externalId")
                ))
                .withRootParameter("featureType", featureType)
                .withProjectConfig(projectConfig);

        CompletableFuture<ResponseItems<String>> responseItems = entityMatchConnector
                .executeAsync(entityMatchFitRequest);
        LOG.info(loggingPrefix + "Train matching model response: isSuccessful: {}, status: {}",
                responseItems.join().isSuccessful(),
                responseItems.join().getStatus().get(0));
        LOG.info(loggingPrefix + "Train matching model response body: {}",
                responseItems.join().getResponseBodyAsString());
        assertTrue(responseItems.join().isSuccessful());
        modelId = mapper.readTree(responseItems.join().getResultsItems().get(0)).path("id").longValue();
        LOG.info(loggingPrefix + "Matching model training finished. Model id: {}", modelId);

        return modelId;
    }

    private ImmutableList<Struct> generateSourceStructs() {
        Struct entityA = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(1D).build())
                .putFields("name", Value.newBuilder().setStringValue("23-DB-9101").build())
                .putFields("fooField", Value.newBuilder().setStringValue("bar").build())
                .build();
        Struct entityB = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(2D).build())
                .putFields("name", Value.newBuilder().setStringValue("23-PC-9101").build())
                .putFields("barField", Value.newBuilder().setStringValue("foo").build())
                .build();
        Struct entityC = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(3D).build())
                .putFields("name", Value.newBuilder().setStringValue("343-Å").build())
                .build();
        return ImmutableList.of(entityA, entityB, entityC);
    }

    private ImmutableList<Struct> generateTargetTrainingStructs() {
        Struct targetA = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(1D).build())
                .putFields("externalId", Value.newBuilder().setStringValue("IA-23_DB_9101").build())
                .build();
        Struct targetB = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(2D).build())
                .putFields("externalId", Value.newBuilder().setStringValue("VAL_23_PC_9101").build())
                .build();
        return ImmutableList.of(targetA, targetB);
    }

    private ImmutableList<Struct> generateTargetStructs() {
        Struct targetA = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(1D).build())
                .putFields("externalId", Value.newBuilder().setStringValue("IA-23_DB_9101").build())
                .putFields("uuid", Value.newBuilder().setStringValue(UUID.randomUUID().toString()).build())
                .build();
        Struct targetB = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setNumberValue(2D).build())
                .putFields("externalId", Value.newBuilder().setStringValue("VAL_23_PC_9101").build())
                .putFields("uuid", Value.newBuilder().setStringValue(UUID.randomUUID().toString()).build())
                .build();
        return ImmutableList.of(targetA, targetB);
    }
}