package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.DataSet;
import com.google.protobuf.BoolValue;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


class DataSetsTest extends TestConfigProviderV1 {

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeDataSet() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p = Pipeline.create();

        PCollection<DataSet> results = p.apply(Create.of(
                DataSet.newBuilder()
                        .setName(StringValue.of("unitTest_dataset"))
                        .setExternalId(StringValue.of("dataset:unitTest_dataset"))
                        .setDescription(StringValue.of("Dataset created by unit test."))
                        .setWriteProtected(BoolValue.of(false))
                        .putMetadata("type", TestUtilsV1.sourceValue)
                        .build()))
                .apply("write data set", CogniteIO.writeDataSets()
                        .withProjectConfig(projectConfigApiKey)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        );

        results.apply("Dataset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(DataSet::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_dataset_write_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        PipelineResult pipelineResult = p.run();
        pipelineResult.waitUntilFinish();

        MetricQueryResults metrics = pipelineResult
                .metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.inNamespace("cognite"))
                        .build());

        for (MetricResult<Long> counter: metrics.getCounters()) {
            System.out.println(counter.getName() + ":" + counter.getAttempted());
        }
        for (MetricResult<DistributionResult> distribution : metrics.getDistributions()) {
            System.out.println(distribution.getName() + ":" + distribution.getAttempted().getMean());
        }
    }

    @Test
    @Tag("remoteCDP")
    void readDataSet() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p2 = Pipeline.create();

        PCollection<DataSet> readResults = p2
                .apply("Read dataset", CogniteIO.readDataSets()
                        .withProjectConfig(projectConfigApiKey)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter("type", TestUtilsV1.sourceValue)));

        readResults.apply("Dataset to string", MapElements.into(TypeDescriptors.strings())
                .via(DataSet::toString))
                .apply("Write event output", TextIO.write().to("./UnitTest_dataset_read_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        PipelineResult pipelineResult = p2.run();
        pipelineResult.waitUntilFinish();

        MetricQueryResults metrics = pipelineResult
                        .metrics()
                        .queryMetrics(MetricsFilter.builder()
                                .addNameFilter(MetricNameFilter.inNamespace("cognite"))
                                .build());

        for (MetricResult<Long> counter: metrics.getCounters()) {
            System.out.println(counter.getName() + ":" + counter.getAttempted());
        }
        for (MetricResult<DistributionResult> distribution : metrics.getDistributions()) {
            System.out.println(distribution.getName() + ":" + distribution.getAttempted().getMean());
        }
    }
}