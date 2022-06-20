package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.ExtractionPipeline;
import com.cognite.client.dto.Item;
import com.cognite.client.util.DataGenerator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;


class EventsTest extends TestConfigProviderV1 {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeReadDeleteEvents() throws Exception {
        final Instant startInstant = Instant.now();
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);

        LOG.info("Starting events unit test: writeReadDeleteEvents()");
        LOG.info("----------------------- Creating extraction pipeline. ----------------------");
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProviderV1.getClientId(),
                        TestConfigProviderV1.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProviderV1.getTenantId()))
                .withProject(TestConfigProviderV1.getProject())
                .withBaseUrl(TestConfigProviderV1.getHost());

        String extractionPipelineExtId = "extPipeline:beam-unit-test-events";
        String extractionPipelineName = "beam-events-unit-test";
        List<DataSet> dataSetUpsertResult = client.datasets().upsert(List.of(
                DataSet.newBuilder()
                        .setExternalId(testDataSetExtId)
                        .setDescription(testDataSetExtId)
                        .setName(testDataSetExtId)
                        .build()
        ));
        long dataSetId = dataSetUpsertResult.get(0).getId();

        List<ExtractionPipeline> extractionPipelineUpsertResult = client.extractionPipelines().upsert(List.of(
                ExtractionPipeline.newBuilder()
                        .setExternalId(extractionPipelineExtId)
                        .setName(extractionPipelineName)
                        .setDescription(extractionPipelineName)
                        .setDataSetId(dataSetId)
                        .build()
        ));


        LOG.info("------------ Finished creating the extraction pipeline. Duration : {} -------------",
                java.time.Duration.between(startInstant, Instant.now()));

        LOG.info("----------------------- Start writing events. -----------------------");
        Pipeline p = Pipeline.create();

        TestStream<Event> events = TestStream.create(ProtoCoder.of(Event.class)).addElements(
                Event.newBuilder()
                        .setExternalId("extId_A")
                        .setDescription("Test_event")
                        .setType(DataGenerator.sourceValue)
                        .build(),
                DataGenerator.generateEvents(9567).toArray(new Event[0])
        )
                .advanceWatermarkToInfinity();

        PCollection<Event> results = p.apply(events)
                .apply("Add windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("write events", CogniteIO.writeEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)
                                .withExtractionPipeline(extractionPipelineExtId))
                        );

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();

        LOG.info("-------------- Finished writing events. Duration : {} -------------------",
                java.time.Duration.between(startInstant, Instant.now()));

        // pause for eventual consistency
        Thread.sleep(20000);

        LOG.info("----------------------- Start reading and deleting events. -----------------------");
        Pipeline p2 = Pipeline.create();

        PCollection<Event> readEventResults = p2
                .apply("Read events", CogniteIO.readEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterParameter("source", DataGenerator.sourceValue)));

        PCollection<Item> deleteResults =
                readEventResults.apply("Map into items", MapElements
                                .into(TypeDescriptor.of(Item.class))
                                .via((Event input) ->
                                        Item.newBuilder()
                                                .setId(input.getId())
                                                .build()
                                ))
                        .apply("Delete items", CogniteIO.deleteEvents()
                                .withProjectConfig(projectConfigClientCredentials)
                                .withWriterConfig(WriterConfig.create()
                                        .withAppIdentifier("Beam SDK unit test")
                                        .withSessionIdentifier(sessionId))
                        );

        p2.run().waitUntilFinish();

        LOG.info("------------- Finished reading and deleting events. Duration : {} ------------------",
                java.time.Duration.between(startInstant, Instant.now()));

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
    }


    @Test
    @Tag("remoteCDP")
    void writeReadEventsStreaming() throws Exception {
        final Instant startInstant = Instant.now();
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);

        LOG.info("Starting events unit test: writeReadEventsStreaming()");
        LOG.info("----------------------- Start writing events. -----------------------");
        Pipeline pipeline = Pipeline.create();

        TestStream<Event> events = TestStream.create(ProtoCoder.of(Event.class)).addElements(
                        Event.newBuilder()
                                .setExternalId("extId_A")
                                .setDescription("Test_event")
                                .setType(DataGenerator.sourceValue)
                                .build(),
                        DataGenerator.generateEvents(9567).toArray(new Event[0])
                )
                .advanceWatermarkToInfinity();

        PCollection<Event> results = pipeline.apply(events)
                .apply("Add windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("write events", CogniteIO.writeEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );
        pipeline.run().waitUntilFinish();
        LOG.info("------------- Finished writing events. Duration : {} ------------------",
                java.time.Duration.between(startInstant, Instant.now()));

        Thread.sleep(15000);

        LOG.info("----------------------- Start reading events. -----------------------");
        Pipeline p = Pipeline.create();
        PCollection<Event> readResults = p
                .apply("Read events", CogniteIO.readEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId)
                                .watchForNewItems()
                                .withPollInterval(java.time.Duration.ofSeconds(10))
                                .withPollOffset(java.time.Duration.ofSeconds(10)))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterParameter("type", TestUtilsV1.sourceValue))
                )
                .apply("Set window", Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(20))));

        readResults
                .apply("Count events received", Combine.globally(Count.<Event>combineFn())
                        .withoutDefaults())
                .apply("Format count output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Timestamp: " + System.currentTimeMillis() + ", Events: " + count))
                .apply("Write read count output", TextIO.write()
                        .to(new TestFilenamePolicy("./UnitTest_events_readStreaming_count_output", ".txt"))
                        .withTempDirectory(FileSystems.matchNewResource("./temp", true))
                        .withNumShards(2)
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        p.run().waitUntilFinish();
        LOG.info("------------- Finished reading events. Duration : {} -----------------",
                java.time.Duration.between(startInstant, Instant.now()));
    }

    /**
     * Test sequences:
     * - Add events.
     * - Read by id.
     * - Remove events
     */
    @Test
    @Tag("remoteCDP")
    void writeReadByIdAndDeleteEvents() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";
        LOG.info(loggingPrefix + "Starting events unit test.");
        LOG.info(loggingPrefix + "Add events headers.");

        Pipeline p = Pipeline.create();

        p.apply("Input data", Create.of(TestUtilsV1.generateEvents(2356)))
                .apply("write events", CogniteIO.writeEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished writing events headers.");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        try {
            Thread.sleep(5000); // Wait for eventual consistency
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info(loggingPrefix + "Start reading events by id.");
        p = Pipeline.create();
        p
                .apply("Read events aggregates", CogniteIO.readEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)
                        ))
                .apply("Build by id request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(event ->
                                Item.newBuilder()
                                        .setExternalId(event.getExternalId())
                                        .build()))
                .apply("Ready event by id", CogniteIO.readAllEventsByIds()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)))
                .apply("Count items", Count.globally())
                .apply("Build text output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of events: " + count))
                .apply("Write read count output", TextIO.write().to("./UnitTest_eventsById_items_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished reading events by id.");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting events");
        p = Pipeline.create();
        p
                .apply("Read events headers", CogniteIO.readEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Build delete events request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(element -> Item.newBuilder()
                                .setExternalId(element.getExternalId())
                                .build()
                        ))
                .apply("Delete events", CogniteIO.deleteEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)))
                .apply("Count deleted items", Count.globally())
                .apply("Build text output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of deleted events: " + count))
                .apply("Write delete item output", TextIO.write().to("./UnitTest_eventsById_deletedItems_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();
        LOG.info(loggingPrefix + "Finished deleting events");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
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
        LOG.info(loggingPrefix + "Finished the events unit test");
    }

    /**
     * Test sequences:
     * - Add events.
     * - Read firstN.
     * - Remove events
     */
    @Test
    @Tag("remoteCDP")
    void writeReadFirstNAndDeleteEvents() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";
        Instant startInstant = Instant.now();
        LOG.info(loggingPrefix + "Starting events unit test: writeReadFirstNAndDeleteEvents()");

        LOG.info("------------------- Start writing events -----------------------");
        Pipeline writePipeline = Pipeline.create();

        writePipeline.apply("Input data", Create.of(DataGenerator.generateEvents(12356)))
                .apply("write events", CogniteIO.writeEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        writePipeline.run().waitUntilFinish();
        LOG.info("------------------- Finished writing events at duration {} -----------------------",
                java.time.Duration.between(startInstant, Instant.now()));

        try {
            Thread.sleep(10000); // Wait for eventual consistency
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info("------------------- Start reading firstN events -----------------------");
        final Pipeline readPipeline = Pipeline.create();
        PCollection<Event> readEvents = readPipeline
                .apply("Read events", CogniteIO.readEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withReadFirstNResults(2000)
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(DataGenerator.sourceKey, DataGenerator.sourceValue)
                        ));
        readEvents
                .apply("Count items", Count.globally())
                .apply("Build text output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of firstN events: " + count))
                .apply("Write read count output", TextIO.write().to("./UnitTest_eventsFirstN_items_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        readPipeline.run().waitUntilFinish();
        LOG.info("------------------- Finished reading firstN events at duration {} -----------------------",
                java.time.Duration.between(startInstant, Instant.now()));

        LOG.info("------------------- Start deleting rows -----------------------");
        final Pipeline deletePipeline = Pipeline.create();
        PCollection<Item> deleteEvents = deletePipeline
                .apply("Read events", CogniteIO.readEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(DataGenerator.sourceKey, DataGenerator.sourceValue)))
                .apply("Build delete events request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(element -> Item.newBuilder()
                                .setExternalId(element.getExternalId())
                                .build()
                        ))
                .apply("Delete events", CogniteIO.deleteEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        deleteEvents
                .apply("Count deleted items", Count.globally())
                .apply("Build text output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of deleted events: " + count))
                .apply("Write delete item output", TextIO.write().to("./UnitTest_eventsFirstN_deletedItems_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        PipelineResult pipelineResult = deletePipeline.run();
        pipelineResult.waitUntilFinish();
        LOG.info("------------------- Finished deleting events at duration {} -----------------------",
                java.time.Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Finished the events unit test");
    }

    static class ItemToStringFn extends DoFn<Item, String> {
        @ProcessElement
        public void processElement(@Element Item input, OutputReceiver<String> outputReceiver) {
            outputReceiver.output(input.toString());
        }
    }

}