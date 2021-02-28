package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.dto.*;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PnIDTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());
    static final String metaKey = "source";
    static final String metaValue = "unit_test";

    @Test
    @Tag("remoteCDP")
    void createInteractivePnId() {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - createInteractivePnId() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = CogniteClient.ofKey(TestConfigProvider.getApiKey())
                .withBaseUrl(TestConfigProvider.getHost())
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        try {
            LOG.info(loggingPrefix + "Start uploading P&IDs to CDF.");
            final String fileExtIdA = "test_file_pnid_a";
            final String fileExtIdB = "test_file_pnid_b";
            final String fileExtIdC = "test_file_pnid_c";
            byte[] fileByteA = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/pn-id-example_1.pdf"));
            byte[] fileByteB = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/pn-id-example_2.pdf"));
            byte[] fileByteC = java.nio.file.Files.readAllBytes(Paths.get("./src/test/resources/pn-id-example_3.pdf"));

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
                    .setExternalId(StringValue.of(fileExtIdA))
                    .setName(StringValue.of("Test_file_pnid"))
                    .setMimeType(StringValue.of("application/pdf"))
                    .putMetadata(metaKey, metaValue)
                    .build();
            final FileMetadata fileMetadataB = FileMetadata.newBuilder()
                    .setExternalId(StringValue.of(fileExtIdB))
                    .setName(StringValue.of("Test_file_pnid_2"))
                    .setMimeType(StringValue.of("application/pdf"))
                    .putMetadata(metaKey, metaValue)
                    .build();
            final FileMetadata fileMetadataC = FileMetadata.newBuilder()
                    .setExternalId(StringValue.of(fileExtIdC))
                    .setName(StringValue.of("Test_file_pnid_3"))
                    .setMimeType(StringValue.of("application/pdf"))
                    .putMetadata(metaKey, metaValue)
                    .build();

            ImmutableList<FileContainer> uploadFilesList = ImmutableList.of(
                    FileContainer.newBuilder()
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
                            .build()
            );

            List<FileMetadata> uploadFilesResult = client.files().upload(uploadFilesList);

            LOG.info(loggingPrefix + "Finished uploading P&IDs to CDF. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start detect annotations and convert to SVG.");
            final List<Struct> entities = ImmutableList.of(
                    Structs.of("name", Values.of("1N914")),
                    Structs.of("name", Values.of("02-100-PE-N")),
                    Structs.of("name", Values.of("01-100-PE-N")));

            final List<Item> fileItems = uploadFilesResult.stream()
                    .map(metadata -> Item.newBuilder()
                            .setExternalId(metadata.getExternalId().getValue())
                            .build())
                    .collect(Collectors.toList());

            List<PnIDResponse> detectResults = client.experimental()
                    .pnid()
                    .detectAnnotationsPnID(fileItems, entities, "name", true);

            LOG.info(loggingPrefix + "Finished detect annotations and convert to SVG. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start writing SVGs to local storage.");
            Path baseFilePath = Paths.get("./");
            for (PnIDResponse response : detectResults) {
                if (response.hasSvgBinary()) {
                    java.nio.file.Files.write(
                            baseFilePath.resolve("test-svg-" + RandomStringUtils.randomAlphanumeric(2) + ".svg"),
                            response.getSvgBinary().getValue().toByteArray());
                }
                if (response.hasPngBinary()) {
                    java.nio.file.Files.write(
                            baseFilePath.resolve("test-png-" + RandomStringUtils.randomAlphanumeric(2) + ".png"),
                            response.getPngBinary().getValue().toByteArray());
                }
            }

            LOG.info(loggingPrefix + "Finished writing SVGs to local storage. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            LOG.info(loggingPrefix + "----------------------------------------------------------------------");

            LOG.info(loggingPrefix + "Start deleting files.");
            List<Item> deleteItemsResults = client.files().delete(fileItems);
            LOG.info(loggingPrefix + "Finished deleting files. Duration: {}",
                    Duration.between(startInstant, Instant.now()));

            assertEquals(fileItems.size(), deleteItemsResults.size());
            assertEquals(detectResults.size(), fileItems.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }
}