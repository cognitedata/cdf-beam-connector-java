package com.cognite.beam.io.transform.csv;

import com.cognite.beam.io.TestConfigProviderV1;
import com.google.protobuf.Struct;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class ReadCsvFileTest extends TestConfigProviderV1 {
    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    void readCsvFileNoBom() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<Struct> structPCollection = pipeline
                .apply("Read csv", ReadCsvFile.from("./src/test/resources/csv-data.txt"));

        structPCollection.apply("Struct to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Struct::toString))
                .apply("Write config output", TextIO.write().to("./UnitTest_readCsvNoBom_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    @Test
    void readCsvFileWithBom() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<Struct> structPCollection = pipeline
                .apply("Read csv", ReadCsvFile.from("./src/test/resources/csv-data-bom.txt"));

        structPCollection.apply("Struct to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Struct::toString))
                .apply("Write config output", TextIO.write().to("./UnitTest_readCsvWithBom_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    @Test
    void readCsvFileWithBomCustomHeader() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<Struct> structPCollection = pipeline
                .apply("Read csv", ReadCsvFile.from("./src/test/resources/csv-data-bom.txt")
                        .withDelimiter(";")
                        .withHeader("Custom_a;;Custom_c"));

        structPCollection.apply("Struct to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Struct::toString))
                .apply("Write config output", TextIO.write().to("./UnitTest_readCsvWithBomCustomHeader_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }
}