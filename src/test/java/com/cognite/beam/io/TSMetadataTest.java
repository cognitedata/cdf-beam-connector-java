package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.TimeseriesMetadata;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class TSMetadataTest extends TestConfigProviderV1 {

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeTsMetadataBatch() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<TimeseriesMetadata> writeResults = pipeline
                .apply("Create TS objects", Create.of(TestUtilsV1.generateTsHeaderObjects(1500)))
                .apply("Write TS headers", CogniteIO.writeTimeseriesMetadata()
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        writeResults.apply("TS header to string", MapElements
                .into(TypeDescriptors.strings())
                .via(TimeseriesMetadata::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_tsHeader_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void readAndUpdateTsMetadata() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p2 = Pipeline.create();

        PCollection<TimeseriesMetadata> readResults = p2
                .apply("Read TS headers", CogniteIO.readTimeseriesMetadata()
                        .withProjectConfig(projectConfig)
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter("source", TestUtilsV1.sourceValue))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        PCollection<TimeseriesMetadata> updateResults =
                readResults.apply("Map into items", MapElements
                        .into(TypeDescriptor.of(TimeseriesMetadata.class))
                        .via((TimeseriesMetadata input) ->
                                TimeseriesMetadata.newBuilder(input)
                                    .putMetadata("type", TestUtilsV1.updatedSourceValue)
                                .build()
                        ))
                        .apply("write items", CogniteIO.writeTimeseriesMetadata()
                                .withProjectConfig(projectConfig)
                                .withWriterConfig(WriterConfig.create()
                                        .withAppIdentifier("Beam SDK unit test")
                                        .withSessionIdentifier(sessionId))
                        );

        updateResults.apply("TS header to string", MapElements
                .into(TypeDescriptors.strings())
                .via(TimeseriesMetadata::toString))
                .apply("Write update output", TextIO.write().to("./UnitTest_ts_header_updateItems_output")
                        .withSuffix(".txt")
                        .withoutSharding());
        p2.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void readAndDeleteTsMetadata() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p2 = Pipeline.create();

        PCollection<TimeseriesMetadata> readResults = p2
                .apply("Read TS headers", CogniteIO.readTimeseriesMetadata()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                )
                .apply("Filter TS", Filter.by(element -> {
                    if (element.getExternalId().getValue().equals(TestUtilsV1.sourceValue)) {
                        return true;
                    }
                    if (!element.getMetadataMap().containsKey("source")) {
                        return false;
                    }
                    return element.getMetadataMap().get("source").equalsIgnoreCase(TestUtilsV1.sourceValue);
                }));

        PCollection<Item> deleteResults =
                readResults.apply("Map into items", MapElements
                        .into(TypeDescriptor.of(Item.class))
                        .via((TimeseriesMetadata input) ->
                                Item.newBuilder()
                                        .setId(input.getId().getValue())
                                        .build()
                        ))
                        .apply("Delete items", CogniteIO.deleteTimeseries()
                                .withProjectConfig(projectConfig)
                                .withWriterConfig(WriterConfig.create()
                                        .withAppIdentifier("Beam SDK unit test")
                                        .withSessionIdentifier(sessionId))
                        );

        deleteResults.apply("Item to string", ParDo.of(new ItemToStringFn()))
                .apply("Write delete output", TextIO.write().to("./UnitTest_ts_header_deleteItems_output")
                        .withSuffix(".txt")
                        .withoutSharding());
        p2.run().waitUntilFinish();
    }

    static class ItemToStringFn extends DoFn<Item, String> {
        @ProcessElement
        public void processElement(@Element Item input, OutputReceiver<String> outputReceiver) {
            outputReceiver.output(input.toString());
        }
    }

}