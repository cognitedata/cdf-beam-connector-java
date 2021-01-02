package com.cognite.beam.io;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.UpdateFrequency;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.TimeseriesMetadata;
import com.cognite.beam.io.dto.TimeseriesPoint;
import com.cognite.beam.io.dto.TimeseriesPointPost;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.parser.TimeseriesParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

class TSPointsTest extends TestConfigProviderV1 {
    static final String appIdentifier = "Beam SDK unit test";
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @BeforeAll
    static void tearup() {
        init();
    }

    /*
    Writes a set of TS with varying frequency and number of datapoints.
    Reads the datapoints back with multi TS requests in raw form and in aggregated form.
     */
    @Test
    @Tag("remoteCDP")
    void writeReadMultiTsPointsBatch() {
        final String loggingPrefix = "Unit test - writeReadMultiTsPointsBatch - ";
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        final int noTsHeaders = 3;
        final int noTsPoints = 30;
        final int noTsPointsVariation = 200;
        final double tsPointsFrequency = 0.1d;
        final int frequencyVariation = 100;
        final List<TimeseriesMetadata> tsMetaList = TestUtilsV1.generateTsHeaderObjects(noTsHeaders);

        Pipeline pipeline = Pipeline.create();
        // write the TS headers
        LOG.info(loggingPrefix + "Starting write TS headers and points.");
        PCollection<TimeseriesMetadata> headerWriteResults = pipeline
                .apply("Create TS objects", Create.of(tsMetaList))
                .apply("Write TS headers", CogniteIO.writeTimeseriesMetadata()
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                );
        // write the TS points
        PCollection<TimeseriesPointPost> writeResults = headerWriteResults
                .apply("Create TS points", ParDo.of(new GenerateTsPointsFn(noTsPoints, tsPointsFrequency,
                        noTsPointsVariation, frequencyVariation)))
                .apply("Write TS points", CogniteIO.writeTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        .withHints(Hints.create()
                                .withWriteTsPointsUpdateFrequency(UpdateFrequency.SECOND)
                                .withWriteShards(1)
                                .withWriteMaxBatchLatency(Duration.ofMinutes(2))));

        // Report the no written points
        writeResults.apply("Count ts points", Count.globally())
                .apply("Format output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of TS data points: " + count))
                .apply("Write insert output", TextIO.write().to("./UnitTest_tsPoints_writeReadMultiTSBatch_writeCount_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished writing TS headers and points. Pausing to let the writes settle...");
        // Let the writes settle...
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info(loggingPrefix + "Start reading TS points");
        /* Read the raw TS points */
        Pipeline readPipeline = Pipeline.create();

        PCollection<TimeseriesPoint> readRawPointsSingleIteration = readPipeline
                .apply("Read Raw points", CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(this.generateReadMultiTSRequest(tsMetaList)));

        // Report the no read points
        readRawPointsSingleIteration.apply("Count raw points", Count.globally())
                .apply("Format raw output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of read raw single iteration TS data points: " + count))
                .apply("Write read s_iter output", TextIO.write().to("./UnitTest_tsPoints_writeReadMultiTSBatch_readRawSingleIterCount_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        PCollection<TimeseriesPoint> readRawPointsMultiIteration = readPipeline
                .apply("Read Raw points", CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(this.generateReadMultiTSRequest(tsMetaList)
                                .withRootParameter("limit", 5)));

        // Report the no written points
        readRawPointsMultiIteration.apply("Count raw m_iter points", Count.globally())
                .apply("Format raw m_iter output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of read raw multi iteration TS data points: " + count))
                .apply("Write read m_iter output", TextIO.write().to("./UnitTest_tsPoints_writeReadMultiTSBatch_readRawMultiIterCount_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        readPipeline.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished reading TS points. Will start reading TS aggregates.");
        /* Read the aggregated TS points */
        Pipeline readAggPipeline = Pipeline.create();

        PCollection<TimeseriesPoint> readAggPointsSingleIteration = readAggPipeline
                .apply("Read Agg points", CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(this.generateReadMultiTSRequest(tsMetaList)
                                .withRootParameter("aggregates", ImmutableList.of("count"))
                                .withRootParameter("granularity", "10m")));

        // Report the no read points
        readAggPointsSingleIteration
                .apply("Format agg output", MapElements.into(TypeDescriptors.strings())
                        .via(tsPoint -> new StringBuilder()
                                .append("externalId: " + tsPoint.getExternalId().getValue() + ";")
                                .append("id: " + tsPoint.getId() + ";")
                                .append("timestamp: " + tsPoint.getTimestamp() + ";")
                                .append("count: " + tsPoint.getValueAggregates().getCount().getValue() + ";")
                                .toString()
                        ))
                .apply("Write read s_iter output", TextIO.write().to("./UnitTest_tsPoints_writeReadMultiTSBatch_readAggSingleICount_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        PCollection<TimeseriesPoint> readAggPointsMultiIteration = readAggPipeline
                .apply("Read Agg points", CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(this.generateReadMultiTSRequest(tsMetaList)
                                .withRootParameter("aggregates", ImmutableList.of("count"))
                                .withRootParameter("granularity", "10m")
                                .withRootParameter("limit", 2))
                                );

        // Report the no read points
        readAggPointsMultiIteration
                .apply("Format agg output", MapElements.into(TypeDescriptors.strings())
                        .via(tsPoint -> new StringBuilder()
                                .append("externalId: " + tsPoint.getExternalId().getValue() + ";")
                                .append("id: " + tsPoint.getId() + ";")
                                .append("timestamp: " + tsPoint.getTimestamp() + ";")
                                .append("count: " + tsPoint.getValueAggregates().getCount().getValue() + ";")
                                .toString()
                        ))
                .apply("Write read s_iter output", TextIO.write().to("./UnitTest_tsPoints_writeReadMultiTSBatch_readAggMultiICount_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        readAggPipeline.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished reading TS aggregates.");
    }

    @Test
    @Tag("remoteCDP")
    void writeTsPointsBatch() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        final int noTsHeaders = 5;
        final int noTsPoints = 120000;
        final int noTsPointsVariation = 1;
        final double tsPointsFrequency = 0.1d;
        final int frequencyVariation = 1;
        final List<TimeseriesMetadata> tsMetaList = TestUtilsV1.generateTsHeaderObjects(noTsHeaders);

        Pipeline pipeline = Pipeline.create();

        // Write the TS headers
        PCollection<TimeseriesMetadata> headerWriteResults = pipeline
                .apply("Create TS objects", Create.of(tsMetaList))
                .apply("Write TS headers", CogniteIO.writeTimeseriesMetadata()
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        // Write the TS points
        PCollection<TimeseriesPointPost> writeResults = headerWriteResults
                .apply("Create TS points", ParDo.of(new GenerateTsPointsFn(noTsPoints, tsPointsFrequency,
                        noTsPointsVariation, frequencyVariation)))
                .apply("Write TS points", CogniteIO.writeTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withHints(Hints.create()
                            .withWriteTsPointsUpdateFrequency(UpdateFrequency.SECOND)
                            .withWriteShards(2)
                            .withWriteMaxBatchLatency(Duration.ofMinutes(5))));

        writeResults.apply("Count ts points", Count.globally())
                .apply("Format output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of TS data points: " + count))
                .apply("Write insert output", TextIO.write().to("./UnitTest_tsPoints_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void writeTsPointsBatchNoMetadata() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<TimeseriesPointPost> writeResults = pipeline
                .apply("Create TS points", Create.of(TestUtilsV1.generateTsDatapointsObjects(243,
                        0.02d, TestUtilsV1.sourceValue)))
                .apply("Write TS points", CogniteIO.writeTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        writeResults.apply("TS points to string", MapElements
                .into(TypeDescriptors.strings())
                .via(TimeseriesPointPost::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_tsPoints_writeBatchNoTsMeta_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void readTsPointsRaw() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        /*
        PCollection<TimeseriesMetadata> headers = pipeline
                .apply("Read TS headers", CogniteIO.readTimeseriesMetadata()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter("source", TestUtilsV1.sourceValue)));

        PCollection<TimeseriesPoint> readResults = headers
                .apply("Create request", MapElements.into(TypeDescriptor.of(RequestParameters.class))
                        .via((TimeseriesMetadata header) ->
                                RequestParameters.create()
                                        .withItemExternalId(header.getExternalId().getValue())
                                        .withRootParameter("limit", 100000)))
                .apply("Read TS points", CogniteIO.readAllTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        );
*/

        // Cannot read a collection of time series as the direct runner will crash on the aggregate.
        PCollection<TimeseriesPoint> readResults = pipeline
                .apply("Read TS points", CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withItemExternalId(TestUtilsV1.sourceValue)
                                .withRootParameter("limit", 100000)));

        /*
        readResults.apply("TS points to string", ParDo.of(new ParseTsPoint()))
                .apply("Write read output", TextIO.write().to("./UnitTest_tsPoints_readRaw_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());
                        */


        readResults.apply("Count ts points received", Count.globally())
                .apply("Format count output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of TS data points: " + count))
                .apply("Write read count output", TextIO.write().to("./UnitTest_tsPoints_readRaw_count_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void readTsPointsAggregated() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<TimeseriesPoint> readResults = pipeline
                .apply("Read TS points", CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withItemExternalId(TestUtilsV1.sourceValue)
                                .withRootParameter("aggregates", ImmutableList.of("average", "max", "count"))
                                .withRootParameter("granularity", "d")));

        readResults.apply("TS points to string", ParDo.of(new ParseTsPoint()))
                .apply("Write read output", TextIO.write().to("./UnitTest_tsPoints_readAgg_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void readTsPointsStreaming() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();



        PCollection<TimeseriesPoint> readResults = pipeline
                .apply("Read TS points", CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .enableNewCodePath()
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId)
                                .watchForNewItems()
                                .withPollInterval(Duration.ofSeconds(10)))
                        .withRequestParameters(RequestParameters.create()
                                .withItemExternalId(TestUtilsV1.sourceValue)
                                .withRootParameter("limit", 100000)
                                .withRootParameter("start", Instant.now()
                                        .minus(10, ChronoUnit.MINUTES).toEpochMilli()))
                )
                .apply("Set window", Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(10))));

        readResults
                .apply("convert to post points", MapElements.into(TypeDescriptor.of(TimeseriesPointPost.class))
                        .via(inputPoint -> TimeseriesPointPost.newBuilder()
                                .setExternalId(TestUtilsV1.updatedSourceValue)
                                .setTimestamp(inputPoint.getTimestamp())
                                .setValueNum(inputPoint.getValueNum())
                                .build()))
                .apply("Write TS", CogniteIO.writeTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId)));

/*
        readResults
                .apply("Count ts points received", Combine.globally(Count.<TimeseriesPoint>combineFn())
                        .withoutDefaults())
                .apply("Format count output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "TS: " + System.currentTimeMillis() + ", TS data points: " + count))
                .apply("Write read count output", TextIO.write().to("./UnitTest_tsPoints_readStreaming_count_output")
                        .withSuffix(".txt")
                        .withNumShards(2)
                        .withWindowedWrites());

 */

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    private RequestParameters generateReadMultiTSRequest(List<TimeseriesMetadata> tsList) {
        RequestParameters requestParameters = RequestParameters.create();
        List<Map<String, Object>> items = new ArrayList<>();
        for (TimeseriesMetadata item : tsList) {
            items.add(ImmutableMap.of("externalId", item.getExternalId().getValue()));
        }
        return requestParameters.withItems(items);
    }

    static class GenerateTsPointsFn extends DoFn<TimeseriesMetadata, TimeseriesPointPost> {
        final int noItems;
        final double frequency;
        final int noItemsVariation;
        final int frequencyVariation;

        GenerateTsPointsFn(int noItems, double frequency, int noItemsVariation, int frequencyVariation) {
            this.noItems = noItems;
            this.frequency = frequency;
            this.noItemsVariation = noItemsVariation;
            this.frequencyVariation = frequencyVariation;
        }

        @ProcessElement
        public void processElement(@Element TimeseriesMetadata input, OutputReceiver<TimeseriesPointPost> outputReceiver) {
            final int numberOfItems = (int) (1 + (ThreadLocalRandom.current().nextInt(noItemsVariation) / 100d)) * noItems;
            final double newFrequency = (1 + (ThreadLocalRandom.current().nextInt(frequencyVariation) / 100d)) * frequency;
            Instant timeStamp = Instant.now();

            for (int i = 0; i < numberOfItems; i++) {
                timeStamp = timeStamp.minusMillis(Math.round(1000l / newFrequency));
                outputReceiver.output(TimeseriesPointPost.newBuilder()
                        .setExternalId(input.getExternalId().getValue())
                        .setTimestamp(timeStamp.toEpochMilli())
                        .setValueNum(ThreadLocalRandom.current().nextLong(-10, 20))
                        .build());
            }
        }
    }

    static class ParseTsPoint extends DoFn<TimeseriesPoint, String> {
        @ProcessElement
        public void processElement(@Element TimeseriesPoint input, OutputReceiver<String> outputReceiver) {
            StringBuilder outputString = new StringBuilder();
            outputString.append("-----------------------").append(System.lineSeparator());
            outputString.append("externalId: ").append(input.getExternalId().getValue()).append(System.lineSeparator());
            outputString.append("id: ").append(input.getId()).append(System.lineSeparator());
            outputString.append("timestamp: ").append(input.getTimestamp()).append(System.lineSeparator());
            outputString.append("valueNum: ").append(input.getValueNum()).append(System.lineSeparator());
            outputString.append("valueString: ").append(input.getValueString()).append(System.lineSeparator());
            outputString.append("valueAgg: ").append(input.getValueAggregates().toString()).append(System.lineSeparator());
            outputString.append("-----------------------").append(System.lineSeparator());
            outputReceiver.output(outputString.toString());
        }
    }

}