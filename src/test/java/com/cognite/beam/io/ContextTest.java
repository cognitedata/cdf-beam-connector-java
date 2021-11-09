package com.cognite.beam.io;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.Request;
import com.cognite.client.dto.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;


class ContextTest extends TestConfigProviderV1 {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());
    static final String metaKey = "source";
    static final String metaValue = "unit_test";

    @BeforeAll
    static void tearup() {
        init();
    }

    /**
     * Test the entity matcher for struct matching:
     *  - Train an ML model
     *  - Execute entity matching via a Beam pipeline
     *  - Delete the ML model
     */
    @Test
    @Tag("remoteCDP")
    void matchEntitiesStruct() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";

        // Train the matching model
        long modelId = -1L;
        String[] modelTypes = {"simple", "bigram", "frequency-weighted-bigram", "bigram-extra-tokenizers"};
        try {
            modelId = trainMatchingModel(modelTypes[1], loggingPrefix);
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info(loggingPrefix + "Setting up pipeline...");
        Pipeline p = Pipeline.create();

        PCollection<KV<Struct, List<EntityMatch>>> results = p.apply(Create.of(generateSourceStructs()))
                .apply("match events", CogniteIO.matchStructEntities()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withId(modelId)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        );

        results.apply("Result to string", MapElements
                .into(TypeDescriptors.strings())
                .via(KV::toString))
                .apply("Write match output", TextIO.write().to("./UnitTest_context_matchEntitiesStruct_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        LOG.info(loggingPrefix + "Finished setting up the pipeline. Starting pipeline...");
        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();
        LOG.info(loggingPrefix + "Pipeline finished.");

        MetricQueryResults metrics = pipelineResult
                .metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.inNamespace("cognite"))
                        .build());

        for (MetricResult<Long> counter: metrics.getCounters()) {
            System.out.println(counter.getName() + ":" + counter.getAttempted());
        }
        for (MetricResult<DistributionResult> distribution : metrics.getDistributions()) {
            System.out.println(distribution.getName() + ":" + distribution.getAttempted().getMean());
        }

        // Remove the matching model
        LOG.info(loggingPrefix + "Clean up. Removing the matching model...");
        try {
            assertTrue(deleteEntityMatcherModel(modelId, loggingPrefix));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Test the entity matcher for entity matching:
     *  - Train an ML model
     *  - Execute entity matching via a Beam pipeline
     *  - Delete the ML model
     */
    @Test
    @Tag("remoteCDP")
    void matchEntities() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";

        // Train the matching model
        long modelId = -1L;
        String[] featureTypes = {"simple", "bigram", "frequency-weighted-bigram", "bigram-extra-tokenizers"};
        try {
            modelId = trainMatchingModel(featureTypes[1], loggingPrefix);
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info(loggingPrefix + "Setting up pipeline...");
        Pipeline p = Pipeline.create();

        PCollection<KV<Event, List<EntityMatch>>> results = p.apply(Create.of(generateEvents()))
                .apply("match events", CogniteIO.<Event>matchEntities()
                        .via(event ->
                                Struct.newBuilder()
                                        .putFields("id", Value.newBuilder()
                                                .setStringValue(event.getExternalId())
                                                .build())
                                        .putFields("name", Value.newBuilder()
                                                .setStringValue(event.getMetadataOrDefault("asset", "noAsset"))
                                                .build())
                                        .build()
                                )
                        .withProjectConfig(projectConfigClientCredentials)
                        .withId(modelId)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        results.apply("Result to string", MapElements
                .into(TypeDescriptors.strings())
                .via(KV::toString))
                .apply("Write match output", TextIO.write().to("./UnitTest_context_matchEntities_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        LOG.info(loggingPrefix + "Finished setting up the pipeline. Starting pipeline...");
        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();
        LOG.info(loggingPrefix + "Pipeline finished.");

        MetricQueryResults metrics = pipelineResult
                .metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.inNamespace("cognite"))
                        .build());

        for (MetricResult<Long> counter: metrics.getCounters()) {
            System.out.println(counter.getName() + ":" + counter.getAttempted());
        }
        for (MetricResult<DistributionResult> distribution : metrics.getDistributions()) {
            System.out.println(distribution.getName() + ":" + distribution.getAttempted().getMean());
        }

        // Remove the matching model
        LOG.info(loggingPrefix + "Clean up. Removing the matching model...");
        try {
            assertTrue(deleteEntityMatcherModel(modelId, loggingPrefix));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Test the entity matcher for struct matching:
     *  - Train an ML model
     *  - Execute entity matching with [target] specification via a Beam pipeline
     *  - Delete the ML model
     */
    @Test
    @Tag("remoteCDP")
    void matchEntitiesStructWithTarget() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";

        // Train the matching model
        long modelId = -1L;
        String[] featureTypes = {"simple", "bigram", "frequency-weighted-bigram", "bigram-extra-tokenizers"};
        try {
            modelId = trainMatchingModel(featureTypes[1], loggingPrefix);
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info(loggingPrefix + "Setting up pipeline...");
        Pipeline p = Pipeline.create();

        PCollectionView<List<Struct>> targetView = p.apply(Create.of(generateTargetStructs()))
                .apply(View.asList());

        PCollection<KV<Struct, List<EntityMatch>>> results = p.apply(Create.of(generateSourceStructs()))
                .apply("match events", CogniteIO.matchStructEntities()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withId(modelId)
                        .withTargetView(targetView)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        results.apply("Result to string", MapElements
                .into(TypeDescriptors.strings())
                .via(KV::toString))
                .apply("Write match output", TextIO.write().to("./UnitTest_context_matchEntitiesStructWithTarget_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        LOG.info(loggingPrefix + "Finished setting up the pipeline. Starting pipeline...");
        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();
        LOG.info(loggingPrefix + "Pipeline finished.");

        MetricQueryResults metrics = pipelineResult
                .metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.inNamespace("cognite"))
                        .build());

        for (MetricResult<Long> counter: metrics.getCounters()) {
            System.out.println(counter.getName() + ":" + counter.getAttempted());
        }
        for (MetricResult<DistributionResult> distribution : metrics.getDistributions()) {
            System.out.println(distribution.getName() + ":" + distribution.getAttempted().getMean());
        }

        // Remove the matching model
        LOG.info(loggingPrefix + "Clean up. Removing the matching model...");
        try {
            assertTrue(deleteEntityMatcherModel(modelId, loggingPrefix));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Test the entity matcher for entity matching:
     *  - Train an ML model
     *  - Execute entity matching via a Beam pipeline
     *  - Delete the ML model
     */
    @Test
    @Tag("remoteCDP")
    void matchEntitiesWithTarget() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";

        // Train the matching model
        long modelId = -1L;
        String[] featureTypes = {"simple", "bigram", "frequency-weighted-bigram", "bigram-extra-tokenizers"};
        try {
            modelId = trainMatchingModel(featureTypes[3], loggingPrefix);
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info(loggingPrefix + "Setting up pipeline...");
        Pipeline p = Pipeline.create();

        PCollectionView<List<Struct>> targetView = p.apply(Create.of(generateTargetStructs()))
                .apply(View.asList());

        PCollection<KV<Event, List<EntityMatch>>> results = p.apply(Create.of(generateEvents()))
                .apply("match events", CogniteIO.<Event>matchEntities()
                        .via(event ->
                                Struct.newBuilder()
                                        .putFields("id", Value.newBuilder()
                                                .setStringValue(event.getExternalId())
                                                .build())
                                        .putFields("name", Value.newBuilder()
                                                .setStringValue(event.getMetadataOrDefault("asset", "noAsset"))
                                                .build())
                                        .build()
                        )
                        .withProjectConfig(projectConfigClientCredentials)
                        .withId(modelId)
                        .withTargetView(targetView)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        results.apply("Result to string", MapElements
                .into(TypeDescriptors.strings())
                .via(KV::toString))
                .apply("Write match output", TextIO.write().to("./UnitTest_context_matchEntitiesWithtarget_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        LOG.info(loggingPrefix + "Finished setting up the pipeline. Starting pipeline...");
        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();
        LOG.info(loggingPrefix + "Pipeline finished.");

        MetricQueryResults metrics = pipelineResult
                .metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.inNamespace("cognite"))
                        .build());

        for (MetricResult<Long> counter: metrics.getCounters()) {
            System.out.println(counter.getName() + ":" + counter.getAttempted());
        }
        for (MetricResult<DistributionResult> distribution : metrics.getDistributions()) {
            System.out.println(distribution.getName() + ":" + distribution.getAttempted().getMean());
        }

        // Remove the matching model
        LOG.info(loggingPrefix + "Clean up. Removing the matching model...");
        try {
            assertTrue(deleteEntityMatcherModel(modelId, loggingPrefix));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void createInteractiveDiagramsTest() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);

        final List<Struct> entities = ImmutableList.of(
                Structs.of("name", Values.of("1N914")),
                Structs.of("name", Values.of("02-100-PE-N")),
                Structs.of("name", Values.of("01-100-PE-N")));

        /* Prepare the pnid files to write to cdf */
        final String fileExtIdA = "test_file_pnid_a";
        final String fileExtIdB = "test_file_pnid_b";
        final String fileExtIdC = "test_file_pnid_c";
        byte[] fileByteA = new byte[0];
        byte[] fileByteB = new byte[0];
        byte[] fileByteC = new byte[0];
        try {
            fileByteA = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/pn-id-example_1.pdf"));
            fileByteB = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/pn-id-example_2.pdf"));
            fileByteC = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/pn-id-example_3.pdf"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        final FileBinary fileBinaryA = FileBinary.newBuilder()
                .setBinary(ByteString.copyFrom(fileByteA))
                .setExternalId(fileExtIdA)
                .build();
        final FileBinary fileBinaryB = FileBinary.newBuilder()
                .setBinary(ByteString.copyFrom(fileByteB))
                .setExternalId(fileExtIdB)
                .build();
        final FileBinary fileBinaryC = FileBinary.newBuilder()
                .setBinary(ByteString.copyFrom(fileByteC))
                .setExternalId(fileExtIdC)
                .build();
        final FileMetadata fileMetadataA = FileMetadata.newBuilder()
                .setExternalId(fileExtIdA)
                .setName("Test_file_pnid")
                .setMimeType("application/pdf")
                .putMetadata(metaKey, metaValue)
                .build();
        final FileMetadata fileMetadataB = FileMetadata.newBuilder()
                .setExternalId(fileExtIdB)
                .setName("Test_file_pnid_2")
                .setMimeType("application/pdf")
                .putMetadata(metaKey, metaValue)
                .build();
        final FileMetadata fileMetadataC = FileMetadata.newBuilder()
                .setExternalId(fileExtIdC)
                .setName("Test_file_pnid_3")
                .setMimeType("application/pdf")
                .putMetadata(metaKey, metaValue)
                .build();

        LOG.info("unit test - Start uploading P&ID PDFs.");

        Pipeline writeFilesPipeline = Pipeline.create();

        writeFilesPipeline
                .apply("files input", Create.of(FileContainer.newBuilder()
                                .setFileMetadata(fileMetadataA)
                                .setFileBinary(fileBinaryA)
                                .build(),
                        FileContainer.newBuilder()
                                .setFileMetadata(fileMetadataB)
                                .setFileBinary(fileBinaryB)
                                .build(),
                        FileContainer.newBuilder()
                                .setFileMetadata(fileMetadataC)
                                .setFileBinary(fileBinaryC)
                                .build()))
                .apply("write files", CogniteIO.writeFiles()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        writeFilesPipeline.run().waitUntilFinish();
        LOG.info("unit test - Done uploading P&ID PDFs.");
        LOG.info("unit test - Start creating interactive P&IDs.");

        /* read pnid and create interactive features */
        Pipeline pipeline = Pipeline.create();
        Long startTime = System.currentTimeMillis();

        PCollection<FileContainer> readResults = pipeline
                .apply("Read files", CogniteIO.readFiles()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(metaKey, metaValue)));

        PCollectionView<List<Struct>> targetView = pipeline.apply(Create.of(entities))
                .apply("to view", View.asList());

        PCollection<DiagramResponse> createPnID = readResults
                .apply("Build Item", MapElements.into(TypeDescriptor.of(Item.class))
                        .via((FileContainer fileContainer) ->
                                Item.newBuilder()
                                        .setId(fileContainer.getFileMetadata().getId())
                                        .build()
                        ))
                .apply("Create pnId", CogniteIO.Experimental.createInteractiveDiagram()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withHints(Hints.create())
                        .withReaderConfig(ReaderConfig.create())
                        .withEntitiesView(targetView)
                        .enableConvertFile(true)
                        .enableGrayscale(true));


        createPnID.apply("to string", MapElements.into(TypeDescriptors.strings())
                .via((DiagramResponse pnid) -> pnid.toString()))
                .apply("Write P&ID Response output", TextIO.write().to("./UnitTest_createInteractivePnID_output")
                        .withSuffix(".txt")
                        .withNoSpilling());

        pipeline.run().waitUntilFinish();


        LOG.info("unit test - Finished creating interactive P&IDs in "
                + (System.currentTimeMillis() - startTime) + "millies.");
    }

    private long trainMatchingModel(String featureType, String loggingPrefix) throws Exception {
        // Set up the main data objects to use during the test
        ImmutableList<Struct> source = generateSourceStructs();
        ImmutableList<Struct> target = generateTargetTrainingStructs();

        // Train the matching model
        Request entityMatchFitRequest = Request.create()
                .withRootParameter("sources",  source)
                .withRootParameter("targets", target)
                .withRootParameter("matchFields", ImmutableList.of(
                        ImmutableMap.of("source", "name", "target", "externalId")
                ))
                .withRootParameter("featureType", featureType);

        CogniteClient client = CogniteClient.ofKey(getApiKey())
                .withBaseUrl(getHost());
        List<EntityMatchModel> models = client.contextualization().entityMatching()
                .create(ImmutableList.of(entityMatchFitRequest));

        LOG.debug(loggingPrefix + "Train matching model response body: {}",
                models.get(0));

        return models.get(0).getId();
    }

    private boolean deleteEntityMatcherModel(long modelId, String loggingPrefix) throws Exception {
        LOG.info(loggingPrefix + "Clean up. Removing the matching model...");
        Item modelItem = Item.newBuilder()
                .setId(modelId)
                .build();

        CogniteClient client = CogniteClient.ofKey(getApiKey())
                .withBaseUrl(getHost());
        List<Item> deleteResults = client.contextualization()
                .entityMatching()
                .delete(ImmutableList.of(modelItem));

        LOG.info(loggingPrefix + "Delete model response body: {}",
                deleteResults.get(0));
        return true;
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

    private ImmutableList<Event> generateEvents() {
        Event eventA = Event.newBuilder()
                .setExternalId("extId_A")
                .setDescription("Test_event_23-DB-9101")
                .setType(TestUtilsV1.sourceValue)
                .putMetadata("asset", "23-DB-9101")
                .build();
        Event eventB = Event.newBuilder()
                .setExternalId("extId_B")
                .setDescription("Test_event_23-PC-9101")
                .setType(TestUtilsV1.sourceValue)
                .putMetadata("asset", "23-PC-9101")
                .build();
        Event eventC = Event.newBuilder()
                .setExternalId("extId_C")
                .setDescription("Test_event_23-PC-9101")
                .setType(TestUtilsV1.sourceValue)
                .putMetadata("asset", "23-PC-9101")
                .build();
        Event eventD = Event.newBuilder()
                .setExternalId("extId_D")
                .setDescription("Test_event_343-Å")
                .setType(TestUtilsV1.sourceValue)
                .putMetadata("asset", "343-Å")
                .build();
        return ImmutableList.of(eventA, eventB, eventC, eventD);
    }
}