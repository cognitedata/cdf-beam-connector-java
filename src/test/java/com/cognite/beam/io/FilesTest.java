package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.*;
import com.google.protobuf.ByteString;
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

import java.nio.file.Paths;
import java.util.Arrays;

class FilesTest extends TestConfigProviderV1 {
    static final String fileType = "text/plain";
    static final String metaKey = "source";
    static final String metaValue = "unit_test";

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeFileStreamBasicBatch() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        final String fileExtIdA = "test_file_a";
        final String fileExtIdB = "test_file_b";
        byte[] fileByteA = new byte[0];
        byte[] fileByteB = new byte[0];
        try {
            fileByteA = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/csv-data.txt"));
            fileByteB = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/csv-data-bom.txt"));
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
        final FileMetadata fileMetadataA = FileMetadata.newBuilder()
                .setExternalId(fileExtIdA)
                .setName("Test_file")
                .setMimeType(fileType)
                .putMetadata(metaKey, metaValue)
                .build();
        final FileMetadata fileMetadataB = FileMetadata.newBuilder()
                .setExternalId(fileExtIdB)
                .setName("Test_file_2")
                .setMimeType(fileType)
                .putMetadata(metaKey, metaValue)
                .build();


        Pipeline p = Pipeline.create();

        TestStream<FileContainer> fileContainers = TestStream.create(ProtoCoder.of(FileContainer.class)).addElements(
                FileContainer.newBuilder()
                        .setFileMetadata(fileMetadataA)
                        .setFileBinary(fileBinaryA)
                        .build(),
                FileContainer.newBuilder()
                        .setFileMetadata(fileMetadataB)
                        .setFileBinary(fileBinaryB)
                        .build()
        )
                .advanceWatermarkToInfinity();

        PCollection<FileMetadata> results = p.apply(fileContainers)
                .apply("Add windowing", Window.<FileContainer>into(FixedWindows.of(Duration.standardSeconds(10)))
                        .withAllowedLateness(Duration.ZERO))
                .apply("write files", CogniteIO.writeFiles()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        results.apply("File header to string", MapElements
                .into(TypeDescriptors.strings())
                .via(FileMetadata::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_file_writeBatch_output")
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
        byte[] fileByteA = new byte[0];
        byte[] fileByteB = new byte[0];
        try {
            fileByteA = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/csv-data.txt"));
            fileByteB = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/csv-data-bom.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        final byte[] finalFileByteA = fileByteA;
        final byte[] finalFileByteB = fileByteB;

        Pipeline p2 = Pipeline.create();

        PCollection<FileContainer> readResults = p2
                .apply("Read files", CogniteIO.readFiles()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(metaKey, metaValue))
                        //.withTempStorageURI(ValueProvider.StaticValueProvider.of("gs://sa-kh/temp/"))
                        //.enableForceTempStorage(true)
                );

        readResults.apply("Check binary", MapElements.into(TypeDescriptor.of(FileContainer.class))
                .via(fileContainer -> {
                    byte[] binary = fileContainer.getFileBinary().getBinary().toByteArray();
                    if (Arrays.equals(binary, finalFileByteA)) {
                        System.out.println("File binary matches [fileByteA].");
                    } else if (Arrays.equals(binary, finalFileByteB)) {
                        System.out.println("File binary matches [fileByteB].");
                    }

                    return fileContainer;
                }));

        PCollection<Item> deleteResults =
                readResults.apply("Map into items", MapElements
                        .into(TypeDescriptor.of(Item.class))
                        .via((FileContainer input) ->
                                Item.newBuilder()
                                        .setId(input.getFileMetadata().getId())
                                        .build()
                        ))
                        .apply("Delete files", CogniteIO.deleteFiles()
                                .withProjectConfig(projectConfigClientCredentials)
                                .withWriterConfig(WriterConfig.create()
                                        .withAppIdentifier("Beam SDK unit test")
                                        .withSessionIdentifier(sessionId))
                        );

        readResults.apply("File header to string", MapElements.into(TypeDescriptors.strings())
                .via(container -> container.getFileMetadata().toString()))
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