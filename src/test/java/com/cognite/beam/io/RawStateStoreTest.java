package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.statestore.RawStateStore;
import com.cognite.client.statestore.StateStore;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class RawStateStoreTest extends TestConfigProviderV1 {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    final String dbName = "states";
    final String tableName = "stateTable";
    final String highWatermarkKey = "high";

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void createCommitAndLoadStates() throws Exception {
        final Instant startInstant = Instant.now();

        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProviderV1.getClientId(),
                        TestConfigProviderV1.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProviderV1.getTenantId()))
                .withProject(TestConfigProviderV1.getProject())
                .withBaseUrl(TestConfigProviderV1.getHost());

        LOG.info("Starting raw state store unit test: createCommitAndLoadStates()");
        LOG.info("----------------------- Start setting high watermark. ----------------------");
        long originalWatermark = 10;

        Pipeline p = Pipeline.create();

        PCollection<KV<String, Long>> results = p
                .apply("Create values", Create.of(KV.of(highWatermarkKey, originalWatermark)))
                .apply("Set high watermark", CogniteIO.setHighRawStateStorage()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                )
                                .withDbName(dbName)
                                .withTableName(tableName)
                        );

        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();

        // Check that the watermark is correctly set.
        StateStore stateStore = RawStateStore.of(client, dbName, tableName);
        stateStore.load();
        long highWatermarkFromStateStore = stateStore.getHigh(highWatermarkKey).orElse(0);
        assertEquals(originalWatermark, highWatermarkFromStateStore);

        LOG.info("-------------- Finished setting high watermark. Duration : {} -------------------",
                java.time.Duration.between(startInstant, Instant.now()));


        LOG.info("----------------------- Start reading high watermark. -----------------------");
        Pipeline p2 = Pipeline.create();

        PCollection<KV<String, Long>> getHighResults = p2
                .apply("Get high", CogniteIO.getHighRawStateStorage()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test"))
                        .withDbName(dbName)
                        .withTableName(tableName)
                        .withKey(highWatermarkKey))
                .apply("print output", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via(kv -> {
                            System.out.println(String.format("Key: %s, Value: %d", kv.getKey(), kv.getValue()));
                            return kv;
                        }));

        p2.run().waitUntilFinish();

        LOG.info("------------- Finished reading high watermark. Duration : {} ------------------",
                java.time.Duration.between(startInstant, Instant.now()));


        LOG.info("----------------------- Start expanding high watermark. -----------------------");
        long newHighValidWatermark = 20;
        long newHighInvalidWatermark = 20;
        List<KV<String, Long>> inputsP3 = List.of(
                KV.of(highWatermarkKey, newHighInvalidWatermark),
                KV.of(highWatermarkKey, newHighValidWatermark)
        );

        Pipeline p3 = Pipeline.create();

        PCollection<KV<String, Long>> results3 = p3
                .apply("Create values", Create.of(inputsP3))
                .apply("Expand high watermark", CogniteIO.expandHighRawStateStorage()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                        )
                        .withDbName(dbName)
                        .withTableName(tableName)
                );

        p3.run().waitUntilFinish();

        // Check that the watermark is correctly set.
        stateStore.load();
        highWatermarkFromStateStore = stateStore.getHigh(highWatermarkKey).orElse(0);
        assertEquals(newHighValidWatermark, highWatermarkFromStateStore);

        LOG.info("------------- Finished expanding high watermark. Duration : {} ------------------",
                java.time.Duration.between(startInstant, Instant.now()));

        LOG.info("----------------------- Start deleting high watermark. -----------------------");

        Pipeline p4 = Pipeline.create();

        PCollection<String> results4 = p4
                .apply("Create values", Create.of(highWatermarkKey))
                .apply("Delete high watermark", CogniteIO.deleteStateRawStateStore()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                        )
                        .withDbName(dbName)
                        .withTableName(tableName)
                );

        p4.run().waitUntilFinish();

        // Check that the watermark is correctly set.
        stateStore.load();
        assertEquals(true, stateStore.getHigh(highWatermarkKey).isEmpty(),
                "State is not deleted from state store");

        LOG.info("------------- Finished deleting high watermark. Duration : {} ------------------",
                java.time.Duration.between(startInstant, Instant.now()));

        LOG.info("----------------------- Clean up raw state store. -----------------------");
        client.raw().databases().delete(List.of(dbName), true);
    }
}