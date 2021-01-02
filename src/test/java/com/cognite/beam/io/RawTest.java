package com.cognite.beam.io;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.RawRow;
import com.cognite.beam.io.dto.RawTable;
import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class RawTest extends TestConfigProviderV1 {

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeRows() {
        final RawRow row1 = RawRow.newBuilder()
                .setDbName(rawDbName)
                .setTableName(rawTableName)
                .setKey("A")
                .setColumns(Struct.newBuilder()
                        .putFields("string", Value.newBuilder().setStringValue("myStringValue").build())
                        .putFields("numeric", Value.newBuilder().setNumberValue(100d).build())
                        .putFields("bool", Value.newBuilder().setBoolValue(true).build())
                        .putFields("null_value", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                        .putFields("array", Value.newBuilder().setListValue(ListValue.newBuilder()
                                .addValues(Value.newBuilder().setNumberValue(1d).build())
                                .addValues(Value.newBuilder().setNumberValue(2d).build())
                                .addValues(Value.newBuilder().setNumberValue(3d).build())
                                .build()).build())
                        .putFields("struct", Value.newBuilder().setStructValue(Struct.newBuilder()
                                .putFields("nestedString", Value.newBuilder().setStringValue("myTrickyStringValue_æøå_äö")
                                        .build())).build())
                ).build();

        final RawRow row2 = RawRow.newBuilder()
                .setDbName(rawDbName)
                .setTableName(rawTableName)
                .setKey("B")
                .setColumns(Struct.newBuilder()
                        .putFields("string", Value.newBuilder().setStringValue("another value").build())
                        .putFields("numeric", Value.newBuilder().setNumberValue(10.123456789d).build())
                        .putFields("bool", Value.newBuilder().setBoolValue(false).build())
                        .putFields("null_value", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                        .putFields("array", Value.newBuilder().setListValue(ListValue.newBuilder()
                                .addValues(Value.newBuilder().setNumberValue(10d).build())
                                .addValues(Value.newBuilder().setNumberValue(20d).build())
                                .addValues(Value.newBuilder().setNumberValue(30d).build())
                                .build()).build())
                        .putFields("struct", Value.newBuilder().setStructValue(Struct.newBuilder()
                                .putFields("nestedString", Value.newBuilder().setStringValue("myStringValue")
                                        .build())).build())
                ).build();

        final Pipeline pipeline = Pipeline.create();
        pipeline.apply("Build input rows", Create.of(row1, row2))
                .apply("write rows", CogniteIO.writeRawRow()
                        .withProjectConfig(projectConfig))
                .apply("Format results", MapElements
                        .into(TypeDescriptors.strings())
                        .via((RawRow element) -> element.toString()))
                .apply("Write output", TextIO.write().to("./UnitTest_raw_write_row_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        pipeline.run();
    }

    @Test
    @Tag("remoteCDP")
    void writeAndReadRowsDelta() {
        System.out.println("Setting up rows" + System.lineSeparator());

        final RawRow row1 = RawRow.newBuilder()
                .setDbName(rawDbName)
                .setTableName(rawTableName)
                .setKey("C")
                .setColumns(Struct.newBuilder()
                        .putFields("description", Value.newBuilder().setStringValue("First row").build())
                        .build()
                ).build();

        final RawRow row2 = RawRow.newBuilder()
                .setDbName(rawDbName)
                .setTableName(rawTableName)
                .setKey("D")
                .setColumns(Struct.newBuilder()
                        .putFields("description", Value.newBuilder().setStringValue("Second row").build())
                        .build()
                ).build();

        final RawRow row3 = RawRow.newBuilder()
                .setDbName(rawDbName)
                .setTableName(rawTableName)
                .setKey("E")
                .setColumns(Struct.newBuilder()
                        .putFields("description", Value.newBuilder().setStringValue("third row").build())
                        .build()
                ).build();

        // write first row
        System.out.println("Writing first row" + System.lineSeparator());
        final Pipeline p1 = Pipeline.create();
        p1.apply("Build input rows", Create.of(row1))
                .apply("write rows", CogniteIO.writeRawRow()
                        .withProjectConfig(projectConfig));
        p1.run().waitUntilFinish();

        // write second row
        System.out.println("Writing second row" + System.lineSeparator());
        final Pipeline p3 = Pipeline.create();
        p3.apply("Build input rows2", Create.of(row2))
                .apply("write rows2", CogniteIO.writeRawRow()
                        .withProjectConfig(projectConfig));
        p3.run().waitUntilFinish();

        // Read first round
        System.out.println("Reading first round" + System.lineSeparator());
        final Pipeline p2 = Pipeline.create();
        p2.apply("Read rows", CogniteIO.readRawRow()
                .withProjectConfig(projectConfig)
                .withDbName(rawDbName)
                .withTableName(rawTableName)
                .withReaderConfig(ReaderConfig.create()
                        .enableDeltaRead(deltaTable)
                        .withDeltaIdentifier(deltaIdentifier)))
                .apply("Format results", MapElements
                        .into(TypeDescriptors.strings())
                        .via((RawRow element) -> element.toString()))
                .apply("Write output", TextIO.write().to("./UnitTest_raw_firstReadDelta_output")
                        .withSuffix(".txt")
                        .withoutSharding());
        p2.run().waitUntilFinish();

        // write third row
        System.out.println("Writing third row" + System.lineSeparator());
        final Pipeline p7 = Pipeline.create();
        p7.apply("Build input rows2", Create.of(row3))
                .apply("write rows2", CogniteIO.writeRawRow()
                        .withProjectConfig(projectConfig));
        p7.run().waitUntilFinish();

        // Read second round. Should output the second and third row
        System.out.println("Reading second round" + System.lineSeparator());
        final Pipeline p4 = Pipeline.create();
        p4.apply("Read 2nd rows", CogniteIO.readRawRow()
                .withProjectConfig(projectConfig)
                .withDbName(rawDbName)
                .withTableName(rawTableName)
                .withReaderConfig(ReaderConfig.create()
                                .enableDeltaRead(deltaTable)
                                .withDeltaIdentifier(deltaIdentifier)
                        //.withDeltaOffset(Duration.ZERO)
                ))
                .apply("Format 2nd results", MapElements
                        .into(TypeDescriptors.strings())
                        .via((RawRow element) -> element.toString()))
                .apply("Write 2nd output", TextIO.write().to("./UnitTest_raw_secondReadDelta_output")
                        .withSuffix(".txt")
                        .withoutSharding());
        p4.run().waitUntilFinish();

        // Read third round, full read. Should output all rows
        System.out.println("Reading third round" + System.lineSeparator());
        final Pipeline p5 = Pipeline.create();
        p5.apply("Read both rows", CogniteIO.readRawRow()
                .withProjectConfig(projectConfig)
                .withDbName(rawDbName)
                .withTableName(rawTableName)
                .withReaderConfig(ReaderConfig.create()
                        .enableDeltaRead(deltaTable)
                        .withDeltaIdentifier(deltaIdentifier)
                        .withFullReadOverride(true)))
                .apply("Format both results", MapElements
                        .into(TypeDescriptors.strings())
                        .via((RawRow element) -> element.toString()))
                .apply("Write 3rd output", TextIO.write().to("./UnitTest_raw_ReadDelta_fullReadOverride_output")
                        .withSuffix(".txt")
                        .withoutSharding());
        p5.run().waitUntilFinish();

        // Read fourth round, full read. Should output all rows
        System.out.println("Reading fourth round" + System.lineSeparator());
        final Pipeline p6 = Pipeline.create();
        p6.apply("Read all rows", CogniteIO.readRawRow()
                .withProjectConfig(projectConfig)
                .withDbName(rawDbName)
                .withTableName(rawTableName)
                .withReaderConfig(ReaderConfig.create()))
                .apply("Format all results", MapElements
                        .into(TypeDescriptors.strings())
                        .via((RawRow element) -> element.toString()))
                .apply("Write 4th output", TextIO.write().to("./UnitTest_raw_ReadDelta_fullRead_output")
                        .withSuffix(".txt")
                        .withoutSharding());
        p6.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void readRowsCursors() {
        final ConnectorServiceV1 connector = ConnectorServiceV1.builder()
                .setMaxRetries(1)
                .build();
        try {
            ResponseItems<String> results = connector.readCursorsRawRows().getItems(RequestParameters.create()
                    .withDbName(rawDbName)
                    .withTableName(rawTableName)
                    .withRootParameter("numberOfCursors", 5)
                    .withRootParameter("minLastUpdatedTime", 123456789L)
                    .withProjectConfig(projectConfig));

            for (String item : results.getResultsItems()) {
                System.out.println(item);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void readRowsToFile() {
        final Pipeline pipeline = Pipeline.create();

        final PCollection<RawRow> rows = pipeline.apply(CogniteIO.readRawRow()
                .withProjectConfig(projectConfig)
                .withHints(Hints.create()
                        .withReadShards(2))
                .withRequestParameters(RequestParameters.create()
                        .withRootParameter("dbName", rawDbName)
                        .withRootParameter("tableName", rawTableName)
                )
        );

        rows.apply("Format results", MapElements
                .into(TypeDescriptors.strings())
                .via((RawRow element) -> {
                    StringBuilder returnBuilder = new StringBuilder();
                    returnBuilder.append("-------------------------------").append(System.lineSeparator());
                    returnBuilder.append(element.toString());
                    if (element.getColumns().containsFields("struct")) {
                        returnBuilder.append("String value of nestedString: ")
                                .append(element.getColumns().getFieldsMap().get("struct").getStructValue()
                                        .getFieldsMap().get("nestedString").getStringValue()
                                ).append(System.lineSeparator());
                    }
                    returnBuilder.append("--------------------------------").append(System.lineSeparator());

                    return returnBuilder.toString();
                }))
                .apply("Write output", TextIO.write().to("./UnitTest_raw_row_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        pipeline.run();
    }

    @Test
    @Tag("remoteCDP")
    void readRowsToFileConfigCheck() {
        final Pipeline pipeline = Pipeline.create();

        final PCollection<RawRow> rows = pipeline.apply(CogniteIO.readRawRow()
                .withProjectConfig(projectConfig)
                .withHints(Hints.create()
                        .withReadShards(2))
                .withRequestParameters(RequestParameters.create()
                        .withRootParameter("dbName", "not_a_valid_db")
                        .withRootParameter("tableName", "not_a_valid_table")
                )
                .withDbName("test_db")
                .withTableName("test_table")
        );

        rows.apply("Format results", MapElements
                .into(TypeDescriptors.strings())
                .via((RawRow element) -> element.toString()))
                .apply("Write output", TextIO.write().to("./UnitTest_raw_config_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        pipeline.run();
    }


    @Test
    @Tag("remoteCDP")
    void deleteRowsToFile() {
        final Pipeline pipeline = Pipeline.create();

        final PCollection<RawRow> rows = pipeline.apply(CogniteIO.readRawRow()
                .withProjectConfig(projectConfig)
                .withHints(Hints.create()
                        .withReadShards(2))
                .withRequestParameters(RequestParameters.create()
                        .withRootParameter("dbName", rawDbName)
                        .withRootParameter("tableName", rawTableName)
                )
        );

        // delete rows
        rows.apply("Delete rows", CogniteIO.deleteRawRow().withProjectConfig(projectConfig))
                .apply("Format delete receipt", MapElements.into(TypeDescriptors.strings())
                        .via(row -> row.toString()))
                .apply("Write delete receipt to file", TextIO.write().to("./UnitTest_raw_deleteRow_output")
                        .withoutSharding()
                        .withSuffix(".txt"));


        // write read rows to file
        rows.apply("Format read results", MapElements
                .into(TypeDescriptors.strings())
                .via((RawRow element) -> element.toString()))
                .apply("Write output", TextIO.write().to("./UnitTest_raw_readRow_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        pipeline.run();
    }

    @Test
    @Tag("remoteCDP")
    void readDbNamesToFile() {
        final Pipeline pipeline = Pipeline.create();

        final PCollection<String> dbs = pipeline.apply(CogniteIO.readRawDatabase()
                .withProjectConfig(projectConfig)
        );

        dbs.apply("Write output", TextIO.write().to("./UnitTest_raw_db_output")
                .withSuffix(".txt")
                .withoutSharding());

        pipeline.run();
    }

    @Test
    @Tag("remoteCDP")
    void readDbNamesAndTableNamesToFile() {
        final Pipeline pipeline = Pipeline.create();

        final PCollection<RawTable> tables = pipeline.apply("Read DBs", CogniteIO.readRawDatabase()
                .withProjectConfig(projectConfig))
                .apply("Read tables", CogniteIO.readAllRawTable()
                        .withProjectConfig(projectConfig));

        tables.apply("Format output", MapElements
                .into(TypeDescriptors.strings())
                .via((RawTable element) -> element.toString()))
                .apply("Write output", TextIO.write().to("./UnitTest_raw_tableNames_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        pipeline.run();
    }

    @Test
    @Tag("remoteCDP")
    void readRawRowsStreaming() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<RawRow> readResults = pipeline
                .apply("Read rows", CogniteIO.readRawRow()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId)
                                .watchForNewItems()
                                .withPollInterval(Duration.ofSeconds(10))
                                .withPollOffset(Duration.ofSeconds(10)))
                        .withRequestParameters(RequestParameters.create()
                                .withDbName(rawDbName)
                                .withTableName(rawTableName))
                )
                .apply("Set window", Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(20))));

        readResults
                .apply("Count rows received", Combine.globally(Count.<RawRow>combineFn())
                        .withoutDefaults())
                .apply("Format count output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Raw row: " + System.currentTimeMillis() + ", Raw row: " + count))
                .apply("Write read count output", TextIO.write()
                        .to(new TestFilenamePolicy("./UnitTest_raw_readStreaming_count_output", ".txt"))
                        .withTempDirectory(FileSystems.matchNewResource("./temp", true))
                        .withNumShards(2)
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

}