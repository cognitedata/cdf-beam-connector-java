package com.cognite.beam.io.transform;

import com.cognite.beam.io.TestConfigProviderV1;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.RawRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


class TimestampTest extends TestConfigProviderV1 {
    final static String identifier = "unitTest";
    final static String rawTable = "timestamp.test";

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeAndReadTimestamp() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        final long timestampA = 1570301730000L;
        final long timestampB = 1570301740000L;

        Pipeline p = Pipeline.create();

        TestStream<Long> timestamps = TestStream.create(VarLongCoder.of()).addElements(timestampA, timestampB)
                .advanceWatermarkToInfinity();

        PCollection<RawRow> writeResults = p.apply(timestamps)
                .apply("Add windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("write timestamps", WriteTimestamp.to(rawTable)
                        .withIdentifier(identifier)
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        );

        // test results to file
        writeResults.apply("Row timestamp to string", MapElements
                .into(TypeDescriptors.strings())
                .via(RawRow::toString))
                .apply("Write timestamp insert output", TextIO.write().to("./UnitTest_timestamp_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        p.run().waitUntilFinish();

        // Read the timestamps
        Pipeline p2 = Pipeline.create();
        PCollection<Long> readResults = p2
                .apply("Read timestamp", ReadTimestamp.from(rawTable)
                        .withIdentifier(identifier)
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));

        // test results to file
        readResults.apply("Long timestamps to string", MapElements.into(TypeDescriptors.strings())
                .via(number -> number.toString()))
                .apply("Write timestamp read", TextIO.write().to("./UnitTest_timestamp_read_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        p2.run().waitUntilFinish();

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
    }

    @Test
    @Tag("remoteCDP")
    void ReadEmptyTimestampTable() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p = Pipeline.create();

        // Read the timestamps
        Pipeline p2 = Pipeline.create();
        PCollection<Long> readResults = p2
                .apply("Read timestamp", ReadTimestamp.from(rawTable)
                        .withIdentifier(identifier + "_no")
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));

        // test results to file
        readResults.apply("Long timestamps to string", MapElements.into(TypeDescriptors.strings())
                .via(number -> number.toString()))
                .apply("Write timestamp read", TextIO.write().to("./UnitTest_timestamp_readEmptyTable_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        p2.run().waitUntilFinish();

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
    }

    @Test
    @Tag("remoteCDP")
    void deleteTimestamps() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        final long timestampA = 1570301730000L;
        final long timestampB = 1570301740000L;
        final String identifier = "unitTest";
        final String rawTable = "timestamp.test";

        Pipeline p = Pipeline.create();

        TestStream<Long> timestamps = TestStream.create(VarLongCoder.of()).addElements(timestampA, timestampB)
                .advanceWatermarkToInfinity();

        PCollection<RawRow> writeResults = p.apply(timestamps)
                .apply("Add windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("write timestamps", WriteTimestamp.to(rawTable)
                        .withIdentifier(identifier)
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        // test results to file
        writeResults.apply("Row timestamp to string", MapElements
                .into(TypeDescriptors.strings())
                .via(RawRow::toString))
                .apply("Write timestamp insert output", TextIO.write().to("./UnitTest_timestamp_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        p.run().waitUntilFinish();

        // Read the timestamps
        Pipeline p2 = Pipeline.create();
        PCollection<Long> readResults = p2
                .apply("Read timestamp", ReadTimestamp.from(rawTable)
                        .withIdentifier(identifier)
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));

        // test results to file
        readResults.apply("Long timestamps to string", MapElements.into(TypeDescriptors.strings())
                .via(number -> number.toString()))
                .apply("Write timestamp read", TextIO.write().to("./UnitTest_timestamp_read_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        p2.run().waitUntilFinish();

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
    }

}