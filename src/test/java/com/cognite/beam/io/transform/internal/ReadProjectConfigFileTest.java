package com.cognite.beam.io.transform.internal;

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.TestConfigProviderV1;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class ReadProjectConfigFileTest extends TestConfigProviderV1 {

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void readProjectConfigFile() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<ProjectConfig> projectConfig = pipeline
                .apply("Read project config file", ReadProjectConfigFile.builder()
                        .setFilePath(ValueProvider.StaticValueProvider.of("./src/main/resources/project-config.toml"))
                        //.setFilePath(ValueProvider.StaticValueProvider.of(""))
                        .build());

        projectConfig.apply("KV to string", MapElements
                .into(TypeDescriptors.strings())
                .via(ProjectConfig::toString))
                .apply("Write config output", TextIO.write().to("./UnitTest_readProjectConfigFile_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

}