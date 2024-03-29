package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Relationship;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
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


class RelationshipsTest extends TestConfigProviderV1 {

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeGroupBasicBatch() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p = Pipeline.create();

        TestStream<Relationship> relationships = TestStream.create(ProtoCoder.of(Relationship.class)).addElements(
                Relationship.newBuilder()
                        .setExternalId("extId_A")
                        .setSourceExternalId("extId_A")
                        .setSourceType(Relationship.ResourceType.ASSET)
                        .setTargetExternalId("extId_B")
                        .setTargetType(Relationship.ResourceType.ASSET)
                        .setConfidence(1f)
                        .build(),
                Relationship.newBuilder()
                        .setExternalId("extId_B")
                        .setSourceExternalId("extId_B")
                        .setSourceType(Relationship.ResourceType.ASSET)
                        .setTargetExternalId("extId_A")
                        .setTargetType(Relationship.ResourceType.ASSET)
                        .setConfidence(1f)
                        .build()
        )
                .advanceWatermarkToInfinity();

        PCollection<Relationship> results = p.apply(relationships)
                .apply("Add windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("write events", CogniteIO.writeRelationships()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        );

        results.apply("Rel to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Relationship::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_relationship_writeBatch_output")
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
    void readAndDeleteRelationships() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline p2 = Pipeline.create();

        PCollection<Relationship> readResults = p2
                .apply("Read rels", CogniteIO.readRelationships()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                        );

        PCollection<Item> deleteResults =
                readResults.apply("Map into items", MapElements
                .into(TypeDescriptor.of(Item.class))
                .via((Relationship input) ->
                        Item.newBuilder()
                                .setExternalId(input.getExternalId())
                                .build()
                ))
                .apply("Delete items", CogniteIO.deleteRelationships()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withSessionIdentifier(sessionId))
                );

        readResults.apply("Rel to string", MapElements.into(TypeDescriptors.strings())
                .via(Relationship::toString))
                .apply("Write rel output", TextIO.write().to("./UnitTest_relationship_deleteItems_rel_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        deleteResults.apply("Item to string", ParDo.of(new ItemToStringFn()))
                .apply("Write delete output", TextIO.write().to("./UnitTest_relationship_deleteItems_output")
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

    static class ItemToStringFn extends DoFn<Item, String> {
        @ProcessElement
        public void processElement(@Element Item input, OutputReceiver<String> outputReceiver) {
            outputReceiver.output(input.toString());
        }
    }

}