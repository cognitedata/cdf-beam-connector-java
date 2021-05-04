package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.util.DataGenerator;
import com.google.protobuf.StringValue;
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


class EventsTest extends TestConfigProviderV1 {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeGroupBasicBatch() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p = Pipeline.create();

        TestStream<Event> events = TestStream.create(ProtoCoder.of(Event.class)).addElements(
                Event.newBuilder()
                        .setExternalId(StringValue.of("extId_A"))
                        .setDescription(StringValue.of("Test_event"))
                        .setType(StringValue.of(DataGenerator.sourceValue))
                        .build(),
                DataGenerator.generateEvents(9567).toArray(new Event[9567])
        )
                .advanceWatermarkToInfinity();

        PCollection<Event> results = p.apply(events)
                .apply("Add windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("write events", CogniteIO.writeEvents()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        );

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();

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
    void readAndDeleteEvents() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p2 = Pipeline.create();

        PCollection<Event> readResults = p2
                .apply("Read events", CogniteIO.readEvents()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterParameter("source", DataGenerator.sourceValue)))
               // .apply("Filter events", Filter
               //         .by(event -> event.getExternalId().getValue().equalsIgnoreCase("1_2017-12-14 13:49:36vdUr2")))
                 ;

        PCollection<Item> deleteResults =
                readResults.apply("Map into items", MapElements
                .into(TypeDescriptor.of(Item.class))
                .via((Event input) ->
                        Item.newBuilder()
                                .setId(input.getId().getValue())
                                .build()
                ))
                .apply("Delete items", CogniteIO.deleteEvents()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        readResults.apply("Event to string", MapElements.into(TypeDescriptors.strings())
                .via(Event::toString))
                .apply("Write event output", TextIO.write().to("./UnitTest_event_deleteItems_event_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        deleteResults.apply("Item to string", ParDo.of(new ItemToStringFn()))
                .apply("Write delete output", TextIO.write().to("./UnitTest_event_deleteItems_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        PipelineResult pipelineResult = p2.run();
        pipelineResult.waitUntilFinish();

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
    void readEventsStreaming() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<Event> readResults = pipeline
                .apply("Read events", CogniteIO.readEvents()
                        .withProjectConfig(projectConfigApiKey)
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
        pipeline.run().waitUntilFinish();
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
                        .withProjectConfig(projectConfigApiKey)
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
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)
                        ))
                .apply("Build by id request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(event ->
                                Item.newBuilder()
                                        .setExternalId(event.getExternalId().getValue())
                                        .build()))
                .apply("Ready event by id", CogniteIO.readAllEventsByIds()
                        .withProjectConfig(projectConfigApiKey)
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
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Build delete events request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(element -> Item.newBuilder()
                                .setExternalId(element.getExternalId().getValue())
                                .build()
                        ))
                .apply("Delete events", CogniteIO.deleteEvents()
                        .withProjectConfig(projectConfigApiKey)
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

    static class ItemToStringFn extends DoFn<Item, String> {
        @ProcessElement
        public void processElement(@Element Item input, OutputReceiver<String> outputReceiver) {
            outputReceiver.output(input.toString());
        }
    }

}