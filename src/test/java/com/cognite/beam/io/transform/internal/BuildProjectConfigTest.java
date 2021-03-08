package com.cognite.beam.io.transform.internal;

import com.cognite.beam.io.TestConfigProviderV1;
import com.cognite.beam.io.config.GcpSecretConfig;
import com.cognite.beam.io.config.ProjectConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class BuildProjectConfigTest extends TestConfigProviderV1 {

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    //@Disabled
    @Tag("remoteCDP")
    void buildProjectConfigFromGcpSecretManager() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<ProjectConfig> projectConfig = pipeline
                .apply("Build project config", BuildProjectConfig.create()
                        .withProjectConfigParameters(ProjectConfig.create()
                                .withApiKeyFromGcpSecret(GcpSecretConfig.of(
                                        "719303257135",
                                        "test-secret"))));

        projectConfig.apply("To string", MapElements
                .into(TypeDescriptors.strings())
                .via(ProjectConfig::toString))
                .apply("Write config output", TextIO.write().to("./UnitTest_buildProjectConfigFromGcp_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

}