package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.SequenceBody;
import com.cognite.client.dto.SequenceMetadata;
import com.cognite.client.dto.SequenceRow;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class SequencesTest extends TestConfigProviderV1 {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @BeforeAll
    static void tearup() {
        init();
    }

    /**
     * Test sequences:
     * - Add sequences headers.
     * - Add rows to all sequences
     * - Edit sequences headers.
     * - Remove rows from all sequences
     * - Remove sequences
     */
    @Test
    @Tag("remoteCDP")
    void writeUpdateAndDeleteSequences() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";
        LOG.info(loggingPrefix + "Starting sequences unit test.");
        LOG.info(loggingPrefix + "Add sequence headers.");

        Pipeline p = Pipeline.create();

        p.apply("Input data", Create.of(TestUtilsV1.generateSequenceMetadata(5)))
                .apply("write sequences", CogniteIO.writeSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished writing sequence headers.");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        try {
            Thread.sleep(2000); // Wait for eventual consistency
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info(loggingPrefix + "Start adding sequence rows.");
        p = Pipeline.create();
        p
                .apply("Read sequence headers", CogniteIO.readSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Add sequence rows", MapElements.into(TypeDescriptor.of(SequenceBody.class))
                        .via(sequence -> TestUtilsV1.generateSequenceRows(sequence, 567)))
                .apply("Write rows", CogniteIO.writeSequenceRows()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished adding sequence rows.");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start editing sequence headers");
        p = Pipeline.create();
        p
                .apply("Read sequence headers", CogniteIO.readSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Edit header", MapElements.into(TypeDescriptor.of(SequenceMetadata.class))
                        .via(sequence -> sequence.toBuilder()
                                .setName(StringValue.of("Edited_value_" + sequence.getName().getValue()))
                                .build()
                        ))
                .apply("Write updates", CogniteIO.writeSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished editing sequence headers");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        try {
            Thread.sleep(4000); // Wait for eventual consistency
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info(loggingPrefix + "Start deleting sequences rows");
        p = Pipeline.create();
        p
                .apply("Read sequence headers", CogniteIO.readSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Build rows query", MapElements.into(TypeDescriptor.of(RequestParameters.class))
                        .via(sequence -> RequestParameters.create()
                                .withRootParameter("externalId", sequence.getExternalId().getValue())
                        ))
                .apply("Read rows", CogniteIO.readAllSequenceRows()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)))
                .apply("Build delete rows request", MapElements.into(TypeDescriptor.of(SequenceBody.class))
                        .via((SequenceBody sequenceBody) -> {
                            // build a new SequenceBody object only containing the row numbers to use
                            /*
                            LOG.info("Build delete row request - input externalId = {}, no rows = {}",
                                    sequenceBody.getExternalId().getValue(),
                                    sequenceBody.getRowsCount());*/
                                    List<SequenceRow> rows = new ArrayList<>();
                                    for (SequenceRow row : sequenceBody.getRowsList()) {
                                        rows.add(SequenceRow.newBuilder()
                                                .setRowNumber(row.getRowNumber())
                                                .build());
                                    }
                                    /*
                                    LOG.info("Build delete row request - output externalId = {}, no rows = {}",
                                            sequenceBody.getExternalId().getValue(),
                                            rows.size());*/
                                    return SequenceBody.newBuilder()
                                            .setExternalId(sequenceBody.getExternalId())
                                            .addAllRows(rows)
                                            .build();
                                }
                                ))
                .apply("Delete rows", CogniteIO.deleteSequenceRows()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished deleting sequence rows");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting sequences");
        p = Pipeline.create();
        p
                .apply("Read sequence headers", CogniteIO.readSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Build delete sequences request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(sequenceBody -> Item.newBuilder()
                                .setExternalId(sequenceBody.getExternalId().getValue())
                                .build()
                        ))
                .apply("Delete sequences", CogniteIO.deleteSequences()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished deleting sequences");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Finished the sequences unit test");
    }

    /**
     * Test sequences:
     * - Add sequences headers.
     * - Read aggregates.
     * - Remove sequences
     */
    @Test
    @Tag("remoteCDP")
    void writeAggregateAndDeleteSequences() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";
        LOG.info(loggingPrefix + "Starting sequences unit test.");
        LOG.info(loggingPrefix + "Add sequence headers.");

        Pipeline p = Pipeline.create();

        p.apply("Input data", Create.of(TestUtilsV1.generateSequenceMetadata(35)))
                .apply("write sequences", CogniteIO.writeSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished writing sequence headers.");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        try {
            Thread.sleep(5000); // Wait for eventual consistency
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info(loggingPrefix + "Start reading aggregates.");
        p = Pipeline.create();
        p
                .apply("Read sequence aggregates", CogniteIO.readAggregatesSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Format file output", MapElements.into(TypeDescriptors.strings())
                        .via(aggregate -> aggregate.toString()))
                .apply("Write read count output", TextIO.write().to("./UnitTest_sequences_aggregates_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished reading aggregates.");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting sequences");
        p = Pipeline.create();
        p
                .apply("Read sequence headers", CogniteIO.readSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Build delete sequences request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(sequenceBody -> Item.newBuilder()
                                .setExternalId(sequenceBody.getExternalId().getValue())
                                .build()
                        ))
                .apply("Delete sequences", CogniteIO.deleteSequences()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished deleting sequences");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Finished the sequences unit test");
    }

    /**
     * Test sequences:
     * - Add sequences headers.
     * - Read by id.
     * - Remove sequences
     */
    @Test
    @Tag("remoteCDP")
    void writeReadByIdAndDeleteSequences() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(5);
        final String loggingPrefix = "Unit test - " + sessionId + " - ";
        LOG.info(loggingPrefix + "Starting sequences unit test.");
        LOG.info(loggingPrefix + "Add sequence headers.");

        Pipeline p = Pipeline.create();

        p.apply("Input data", Create.of(TestUtilsV1.generateSequenceMetadata(1345)))
                .apply("write sequences", CogniteIO.writeSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)));
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished writing sequence headers.");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        try {
            Thread.sleep(15000); // Wait for eventual consistency--takes a long time for sequences
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info(loggingPrefix + "Start reading sequences by id.");
        p = Pipeline.create();
        p
                .apply("Read sequences", CogniteIO.readSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Build sequence id request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(sequenceMetadata ->
                                Item.newBuilder()
                                        .setExternalId(sequenceMetadata.getExternalId().getValue())
                                        .build()))
                .apply("Ready Seq by id", CogniteIO.readAllSequencesMetadataByIds()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)))
                .apply("Count items", Count.globally())
                .apply("Build text output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of sequences: " + count))
                .apply("Write read item output", TextIO.write().to("./UnitTest_sequencesById_items_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished reading sequences by id.");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start deleting sequences");
        p = Pipeline.create();
        p
                .apply("Read sequence headers", CogniteIO.readSequencesMetadata()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter(TestUtilsV1.sourceKey, TestUtilsV1.sourceValue)))
                .apply("Build delete sequences request", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(sequenceBody -> Item.newBuilder()
                                .setExternalId(sequenceBody.getExternalId().getValue())
                                .build()
                        ))
                .apply("Delete sequences", CogniteIO.deleteSequences()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId)))
                .apply("Count deleted items", Count.globally())
                .apply("Build text output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Number of deleted sequences: " + count))
                .apply("Write delete item output", TextIO.write().to("./UnitTest_sequencesById_deletedItems_output")
                        .withSuffix(".txt")
                        .withoutSharding());
        p.run().waitUntilFinish();
        LOG.info(loggingPrefix + "Finished deleting sequences");
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "Finished the sequences unit test");
    }
}