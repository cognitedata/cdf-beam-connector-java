package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.FileMetadata;
import com.cognite.beam.io.dto.Item;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
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

class FilesMetadataTest extends TestConfigProviderV1 {
    static final String fileType = "text/plain";
    static final String metaKey = "source";
    static final String metaValue = "unit_test";

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeFileMetaStreamBasicBatch() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p = Pipeline.create();

        TestStream<FileMetadata> events = TestStream.create(ProtoCoder.of(FileMetadata.class)).addElements(
                FileMetadata.newBuilder()
                        .setExternalId(StringValue.of("extId_A"))
                        .setName(StringValue.of("Test_file"))
                        .setMimeType(StringValue.of(fileType))
                        .putMetadata(metaKey, metaValue)
                        .build(),
                FileMetadata.newBuilder()
                        .setExternalId(StringValue.of("extId_B"))
                        .setName(StringValue.of("Test_file_2"))
                        .setMimeType(StringValue.of(fileType))
                        .putMetadata(metaKey, metaValue)
                        .build()
        )
                .advanceWatermarkToInfinity();

        PCollection<FileMetadata> results = p.apply(events)
                .apply("Add windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("write files", CogniteIO.writeFilesMetadata()
                        .withProjectConfig(projectConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        results.apply("File header to string", MapElements
                .into(TypeDescriptors.strings())
                .via(FileMetadata::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_fileMeta_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        p.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void readAndDeleteFiles() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p2 = Pipeline.create();

        PCollection<FileMetadata> readResults = p2
                .apply("Read file headers", CogniteIO.readFilesMetadata()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(metaKey, metaValue)))
                // .apply("Filter events", Filter
                //         .by(event -> event.getExternalId().getValue().equalsIgnoreCase("1_2017-12-14 13:49:36vdUr2")))
                ;

        PCollection<Item> deleteResults =
                readResults.apply("Map into items", MapElements
                        .into(TypeDescriptor.of(Item.class))
                        .via((FileMetadata input) ->
                                Item.newBuilder()
                                        .setId(input.getId().getValue())
                                        .build()
                        ))
                        .apply("Delete files", CogniteIO.deleteFiles()
                                .withProjectConfig(projectConfig)
                                .withWriterConfig(WriterConfig.create()
                                        .withAppIdentifier("Beam SDK unit test")
                                        .withSessionIdentifier(sessionId))
                        );

        readResults.apply("File header to string", MapElements.into(TypeDescriptors.strings())
                .via(FileMetadata::toString))
                .apply("Write file header output", TextIO.write().to("./UnitTest_files_deleteFiles_fileMeta_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        deleteResults.apply("Item to string", ParDo.of(new ItemToStringFn()))
                .apply("Write delete output", TextIO.write().to("./UnitTest_files_deleteFiles_output")
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