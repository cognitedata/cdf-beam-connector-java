package com.cognite.beam.io.transform;

import com.cognite.beam.io.dto.Item;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.cognite.beam.io.TestConfigProviderV1.init;

class GroupIntoBatchesTest {
    private static final Instant NOW = Instant.now().minus(1, ChronoUnit.DAYS);
    private static final Instant SEC_1_DURATION = NOW.plus(1, ChronoUnit.SECONDS);
    private static final Instant SEC_2_DURATION = NOW.plus(2, ChronoUnit.SECONDS);
    private static final Instant SEC_3_DURATION = NOW.plus(3, ChronoUnit.SECONDS);
    private static final Instant SEC_5_DURATION = NOW.plus(5, ChronoUnit.SECONDS);
    private static final Instant SEC_10_DURATION = NOW.plus(10, ChronoUnit.SECONDS);


    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    void groupBasicBatch() {
        Pipeline p = Pipeline.create();
        Coder<String> utf8Coder = StringUtf8Coder.of();
        //Coder<Long> varLongCoder = VarLongCoder.of();
        Coder<Item> itemCoder = ProtoCoder.of(Item.class);
        KvCoder<String, Item> keyValueCoder = KvCoder.of(utf8Coder, itemCoder);
        TestStream<KV<String, Item>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a", Item.newBuilder().setId(1L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("a", Item.newBuilder().setId(2L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("a", Item.newBuilder().setId(3L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("a", Item.newBuilder().setId(4L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("c", Item.newBuilder().setId(10L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("c", Item.newBuilder().setId(11L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("d", Item.newBuilder().setId(100L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("d", Item.newBuilder().setId(101L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("d", Item.newBuilder().setId(102L).build()), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())))
                .advanceWatermarkToInfinity();

        org.joda.time.Duration windowDuration = org.joda.time.Duration.standardSeconds(10);
        Window<KV<String, Item>> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<String> results = p.apply(words)
                .apply(window)
                .apply(GroupIntoBatches.<String, Item>of(keyValueCoder))
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Iterable<Item>> element) -> {
                            StringBuilder stringBuilder = new StringBuilder();
                            String iterableKey = RandomStringUtils.randomAlphanumeric(5);
                            for (Item value : element.getValue()) {
                                stringBuilder.append("Key: " + element.getKey()
                                        + " Iteration: " + iterableKey
                                        + " Element value: " + value.toString());
                                //.append("\r\n");
                            }
                            System.out.println(stringBuilder.toString());
                            return element.getKey();
                                }
                        ));

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        p.run().waitUntilFinish();
    }

    @Test
    void groupStreamStaleTrigger() {
        Pipeline p = Pipeline.create();
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Long> varLongCoder = VarLongCoder.of();
        //Coder<Item> itemCoder = ProtoCoder.of(Item.class);
        KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
        TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a", 1L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("a", 2L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("c", 10L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())))
                .advanceProcessingTime(org.joda.time.Duration.standardSeconds(3))
                .addElements(
                TimestampedValue.of(KV.of("c", 11L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("d", 100L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("d", 101L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())))
                .advanceProcessingTime(org.joda.time.Duration.standardSeconds(1))
                .addElements(
                TimestampedValue.of(KV.of("d", 102L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())))
                .advanceWatermarkToInfinity();

        org.joda.time.Duration windowDuration = org.joda.time.Duration.standardSeconds(10);
        Window<KV<String, Long>> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<String> results = p.apply(words)
                .apply(window)
                .apply(GroupIntoBatches.<String, Long>of(keyValueCoder)
                        .withMaxLatency(Duration.ofSeconds(2)))
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Iterable<Long>> element) -> {
                                    StringBuilder stringBuilder = new StringBuilder();
                                    String iterableKey = RandomStringUtils.randomAlphanumeric(5);
                                    for (Long value : element.getValue()) {
                                        stringBuilder.append("Key: " + element.getKey()
                                                + " Iteration: " + iterableKey
                                                + " Element value: " + value)
                                        .append("\r\n");
                                    }
                                    System.out.println(stringBuilder.toString());
                                    return element.getKey();
                                }
                        ));

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        p.run().waitUntilFinish();
    }

    @Test
    void groupStreamWindowTrigger() {
        Pipeline p = Pipeline.create();
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Long> varLongCoder = VarLongCoder.of();
        //Coder<Item> itemCoder = ProtoCoder.of(Item.class);
        KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
        TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a", 1L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("a", 2L), org.joda.time.Instant.ofEpochMilli(SEC_2_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("c", 10L), org.joda.time.Instant.ofEpochMilli(SEC_2_DURATION.toEpochMilli())))
                .advanceProcessingTime(org.joda.time.Duration.standardSeconds(3))
                .addElements(
                        TimestampedValue.of(KV.of("c", 11L), org.joda.time.Instant.ofEpochMilli(SEC_5_DURATION.toEpochMilli())),
                        TimestampedValue.of(KV.of("d", 100L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                        TimestampedValue.of(KV.of("d", 101L), org.joda.time.Instant.ofEpochMilli(SEC_10_DURATION.toEpochMilli())))
                .advanceProcessingTime(org.joda.time.Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of(KV.of("d", 102L), org.joda.time.Instant.ofEpochMilli(SEC_10_DURATION.toEpochMilli())))
                .advanceWatermarkToInfinity();

        org.joda.time.Duration windowDuration = org.joda.time.Duration.standardSeconds(5);
        Window<KV<String, Long>> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<String> results = p.apply(words)
                .apply(window)
                .apply(GroupIntoBatches.<String, Long>of(keyValueCoder)
                        .withMaxLatency(Duration.ofSeconds(10)))
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Iterable<Long>> element) -> {
                                    StringBuilder stringBuilder = new StringBuilder();
                                    String iterableKey = RandomStringUtils.randomAlphanumeric(5);
                                    for (Long value : element.getValue()) {
                                        stringBuilder.append("Key: " + element.getKey()
                                                + " Iteration: " + iterableKey
                                                + " Element value: " + value)
                                                .append("\r\n");
                                    }
                                    System.out.println(stringBuilder.toString());
                                    return element.getKey();
                                }
                        ));

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        p.run().waitUntilFinish();
    }

    @Test
    void groupStreamBatchSize() {
        Pipeline p = Pipeline.create();
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Long> varLongCoder = VarLongCoder.of();
        //Coder<Item> itemCoder = ProtoCoder.of(Item.class);
        KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
        TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a", 1L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("a", 2L), org.joda.time.Instant.ofEpochMilli(SEC_2_DURATION.toEpochMilli())),
                TimestampedValue.of(KV.of("c", 10L), org.joda.time.Instant.ofEpochMilli(SEC_2_DURATION.toEpochMilli())))
                .advanceProcessingTime(org.joda.time.Duration.standardSeconds(3))
                .addElements(
                        TimestampedValue.of(KV.of("c", 11L), org.joda.time.Instant.ofEpochMilli(SEC_5_DURATION.toEpochMilli())),
                        TimestampedValue.of(KV.of("d", 100L), org.joda.time.Instant.ofEpochMilli(SEC_1_DURATION.toEpochMilli())),
                        TimestampedValue.of(KV.of("d", 101L), org.joda.time.Instant.ofEpochMilli(SEC_10_DURATION.toEpochMilli())))
                .advanceProcessingTime(org.joda.time.Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of(KV.of("d", 102L), org.joda.time.Instant.ofEpochMilli(SEC_10_DURATION.toEpochMilli())))
                .advanceWatermarkToInfinity();

        org.joda.time.Duration windowDuration = org.joda.time.Duration.standardSeconds(15);
        Window<KV<String, Long>> window = Window.<KV<String, Long>>into(FixedWindows.of(windowDuration))
                .withAllowedLateness(org.joda.time.Duration.standardSeconds(10))
                .discardingFiredPanes()
                ;

        PCollection<String> results = p.apply(words)
                .apply(window)
                .apply(GroupIntoBatches.<String, Long>of(keyValueCoder)
                        .withMaxLatency(Duration.ofSeconds(10))
                        .withMaxBatchSize(2))
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Iterable<Long>> element) -> {
                                    StringBuilder stringBuilder = new StringBuilder();
                                    String iterableKey = RandomStringUtils.randomAlphanumeric(5);
                                    for (Long value : element.getValue()) {
                                        stringBuilder.append("Key: " + element.getKey()
                                                + " Iteration: " + iterableKey
                                                + " Element value: " + value)
                                                .append("\r\n");
                                    }
                                    System.out.println(stringBuilder.toString());
                                    return element.getKey();
                                }
                        ));

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        p.run().waitUntilFinish();
    }
}