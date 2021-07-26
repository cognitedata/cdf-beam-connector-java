package com.cognite.beam.io.transform.yaml;

import com.cognite.beam.io.TestConfigProviderV1;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class ReadYamlObjectMapTest extends TestConfigProviderV1 {
    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    void readYamlFile() {
        Pipeline pipeline = Pipeline.create();
        PCollection<Person> structPCollection = pipeline
                .apply("Read yaml",
                        ReadYamlObjectMap.from("./src/test/resources/person.yaml", Person.class))
                .setCoder(AvroCoder.of(Person.class));

        structPCollection.apply("Struct to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Person::toString))
                .apply("Write config output", TextIO.write().to("./UnitTest_readYamlFile_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());
        pipeline.run().waitUntilFinish();
    }

}
