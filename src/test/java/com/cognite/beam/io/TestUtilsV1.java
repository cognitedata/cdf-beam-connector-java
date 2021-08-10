package com.cognite.beam.io;

import com.cognite.client.dto.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUtilsV1 {
    public static final String sourceKey = "source";
    public static final String sourceValue = "unitTest";
    public static final String updatedSourceValue = "unitTestModified";

    private static final long SECOND_MS = 1000L;
    private static final long MINUTE_MS = 60L * SECOND_MS;
    private static final long HOUR_MS = 60L * MINUTE_MS;
    private static final long DAY_MS = 24L * HOUR_MS;

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static String generateDeleteJson(List<Long> ids) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = JsonNodeFactory.instance.arrayNode();
        for (Long id : ids) {
            ObjectNode item = JsonNodeFactory.instance.objectNode();
            item.put("id", id);
            items.add(item);
        }
        payload.set("items", items);
        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    public static String generateDeleteJsonExternalId(List<String> ids) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = JsonNodeFactory.instance.arrayNode();
        for (String id : ids) {
            ObjectNode item = JsonNodeFactory.instance.objectNode();
            item.put("externalId", id);
            items.add(item);
        }
        payload.set("items", items);
        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    public static String generateAssetJson() {
        // Create a couple of assets.
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = payload.putArray("items");

        ObjectNode rootAsset = JsonNodeFactory.instance.objectNode();
        rootAsset.put("externalId", sourceValue + "_A");
        rootAsset.put("name", "unitTest_root");
        rootAsset.put("description", "The unit test's root asset");
        rootAsset.put("source", sourceValue);
        ObjectNode rootAssetMetadata = rootAsset.putObject("metadata");
        rootAssetMetadata.put("source", sourceValue);
        rootAssetMetadata.put("sourceId", "A");

        ObjectNode childAssetL1 = JsonNodeFactory.instance.objectNode();
        childAssetL1.put("externalId", sourceValue + "_B");
        childAssetL1.put("parentExternalId", sourceValue + "_A");
        childAssetL1.put("name", "unitTest_childL1");
        childAssetL1.put("description", "The unit test's child L1 asset");
        childAssetL1.put("source", sourceValue);
        ObjectNode ChildAssetL1Metadata = childAssetL1.putObject("metadata");
        ChildAssetL1Metadata.put("source", sourceValue);
        ChildAssetL1Metadata.put("sourceId", "B");

        items.add(rootAsset).add(childAssetL1);
        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    public static String generateEventJson() {
        // Create a couple of assets.
        return generateEventJson(2);
    }

    public static String generateEventJson(int numberOfEvents) {
        // Create a couple of assets.
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = payload.putArray("items");

        for (int i = 0; i < numberOfEvents; i++) {
            String sourceId = RandomStringUtils.randomAlphabetic(10);

            ObjectNode rootEvent = JsonNodeFactory.instance.objectNode();
            rootEvent.put("externalId", sourceValue + "_" + sourceId);
            rootEvent.put("startTime", 1552566113);
            rootEvent.put("endTime", 1552567113);
            rootEvent.put("description", "The unit test event " + RandomStringUtils.randomAlphabetic(10));
            ObjectNode rootAssetMetadata = rootEvent.putObject("metadata");
            rootAssetMetadata.put(sourceKey, sourceValue);
            rootAssetMetadata.put("sourceId", sourceId);
            rootAssetMetadata.put("type", "testEvent");
            rootAssetMetadata.put("subtype", "testEvent sub-type");

            items.add(rootEvent);
        }

        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    /* Extract the results items from a results json payload. */
    public static List<Long> extractIds(String json) throws Exception {
        ArrayList<Long> tempList = new ArrayList<>();

        JsonNode node = objectMapper.readTree(json).path("items");
        if (!node.isArray()) {
            assertTrue(false, "Items not found in Json payload");
            return tempList;
        }
        for (JsonNode child : node) {
            if (child.path("id").isIntegralNumber()) {
                tempList.add(child.path("id").longValue());
            } else {
                assertTrue(false, "Cannot parse id in result payload");
            }
        }
        return tempList;
    }

    public static List<String> extractExternalIds(String json) throws Exception {
        ArrayList<String> tempList = new ArrayList<>();

        JsonNode node = objectMapper.readTree(json).path("items");
        if (!node.isArray()) {
            assertTrue(false, "Items not found in Json payload");
            return tempList;
        }
        for (JsonNode child : node) {
            if (child.path("externalId").isTextual()) {
                tempList.add(child.path("externalId").textValue());
            } else {
                assertTrue(false, "Cannot parse id in result payload");
            }
        }
        return tempList;
    }

    public static Optional<String> extractExternalIdFromItem(String json) throws Exception {
        Optional<String> returnValue = Optional.empty();

        JsonNode node = objectMapper.readTree(json).path("externalId");
        if (!node.isTextual()) {
            assertTrue(false, "externalId not found in Json payload");
            return returnValue;
        }

        returnValue = Optional.of(node.textValue());

        return returnValue;
    }

    public static Optional<Long> extractIdFromItem(String json) throws Exception {
        Optional<Long> returnValue = Optional.empty();

        JsonNode node = objectMapper.readTree(json).path("id");
        if (!node.isIntegralNumber()) {
            assertTrue(false, "id not found in Json payload");
            return returnValue;
        }

        returnValue = Optional.of(node.longValue());

        return returnValue;
    }

    public static String generateSeqMetaJson() {

        // Create a couple of sequences.
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = payload.putArray("items");
        IntStream.range(0, 2).forEach(iteration -> {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("name", String.format("test_seq_%d", iteration));
            node.put("description", RandomStringUtils.randomAlphanumeric(128));

            ArrayNode columns = node.putArray("columns");
            IntStream.range(0, 3).forEach(j -> {
                ObjectNode column = JsonNodeFactory.instance.objectNode();
                column.put("name", String.format("test_seq_column_%d", j));
                column.put("description", RandomStringUtils.randomAlphanumeric(128));
                switch (j) {
                    case 0:
                        column.put("valueType", "STRING");
                        break;
                    case 1:
                        column.put("valueType", "LONG");
                        break;
                    default:
                        column.put("valueType", "DOUBLE");
                        break;
                }
                ObjectNode metadata = column.putObject("metadata");
                metadata.put("columnSource", sourceValue);
                metadata.put("columnSourceId", "A");

                columns.add(column);
            });

            ObjectNode metadata = node.putObject("metadata");
            metadata.put("source", sourceValue);
            metadata.put("sourceId", "A");
            items.add(node);
        });

        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    public static String generateTsMetaJson() {
        return generateTsMetaJson(2);
    }

    public static String generateTsMetaJson(int numberOfTsHeaders) {

        // Create a couple of timeseries.
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = payload.putArray("items");
        IntStream.range(0, numberOfTsHeaders).forEach(iteration -> {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("externalId", sourceValue + "_" + iteration);
            node.put("name", String.format("test_ts_%d", iteration));
            node.put("isString", "true");
            node.put("description", RandomStringUtils.randomAlphanumeric(128));
            node.put("unit", "Mortens");

            ObjectNode metadata = node.putObject("metadata");
            metadata.put("source", sourceValue);
            metadata.put("sourceId", String.valueOf(iteration));
            items.add(node);
        });

        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    public static String generateTsDataJson() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = payload.putArray("items");
        IntStream.range(0, 2).forEach(iteration -> {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("name", String.format("test_ts_%d", iteration));
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            IntStream.range(0, 2).forEach(points -> {
                ObjectNode point = JsonNodeFactory.instance.objectNode();
                point.put("timestamp", System.currentTimeMillis());
                point.put("value", "v" + System.currentTimeMillis() / 1000);
                array.add(point);
            });
            node.set("datapoints", array);
            items.add(node);
        });

        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    public static String generate3dModelJson() {
        // Create a couple of models.
        return generate3dModelJson(2);
    }

    public static String generate3dModelJson(int numberOfModels) {
        // Create a couple of models.
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = payload.putArray("items");

        for (int i = 0; i < numberOfModels; i++) {
            ObjectNode rootEvent = JsonNodeFactory.instance.objectNode();
            rootEvent.put("name", "test_model_" + i);

            items.add(rootEvent);
        }

        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    public static String generate3dRevisionJson() {
        // Create a couple of revisions.
        return generate3dRevisionJson(2);
    }

    static String generate3dRevisionJson(int numberOfRevisions) {
        // Create a couple of revisions for a model.
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        ArrayNode items = payload.putArray("items");

        for (int i = 0; i < numberOfRevisions; i++) {
            ObjectNode rootEvent = JsonNodeFactory.instance.objectNode();

            rootEvent.put("published", i % 2 == 0);
            ArrayNode rotation = rootEvent.putArray("rotation");
            rotation.add(0.0).add(-1.1).add(2.2);
            ObjectNode camera = rootEvent.putObject("camera");
            ArrayNode target = camera.putArray("target");
            target.add(0.0).add(-1.1).add(2.2);
            ArrayNode position = camera.putArray("position");
            position.add(0.0).add(-1.1).add(2.2);
            // TODO figure out what to do with this field so that we can test
            rootEvent.put("fileId", RandomStringUtils.randomNumeric(1, 9));

            items.add(rootEvent);
        }

        String jsonString = "";

        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        } catch (Exception e) {
            System.out.println("Could not serialize json, using toString: " + e.toString());
            jsonString = payload.toString();
        }
        return jsonString;
    }

    public static List<Map<String, Object>> generateEventItems(int noItems) {
        List<Map<String, Object>> items = new ArrayList<>(noItems);
        for (int i = 0; i < noItems; i++) {
            Map<String, Object> root = new HashMap<>();
            Map<String, String> metadata = new HashMap<>();
            root.put("externalId", RandomStringUtils.randomAlphanumeric(10));
            root.put("startTime", System.currentTimeMillis() - (2 * 7 * 24 * 3500 * 1000)
                            + ThreadLocalRandom.current().nextLong(60000));
            root.put("endTime", System.currentTimeMillis() - (1 * 7 * 24 * 3500 * 1000)
                    + ThreadLocalRandom.current().nextLong(60000));
            root.put("description", "Unit test event - " + RandomStringUtils.randomAlphanumeric(5));
            metadata.put("type", TestUtilsV1.sourceValue);
            root.put("metadata", metadata);

            items.add(root);
        }

        return items;
    }

    public static List<Map<String, Object>> generateTsHeaderItems(int noItems, boolean isString) {
        List<Map<String, Object>> items = new ArrayList<>(noItems);
        for (int i = 0; i < noItems; i++) {
            Map<String, Object> root = new HashMap<>();
            Map<String, String> metadata = new HashMap<>();
            root.put("externalId", RandomStringUtils.randomAlphanumeric(10));
            root.put("name", "test_ts_" + RandomStringUtils.randomAlphanumeric(5));
            root.put("isString", isString);
            root.put("description", RandomStringUtils.randomAlphanumeric(50));
            root.put("unit", "TestUnits");
            metadata.put("type", TestUtilsV1.sourceValue);
            metadata.put(sourceKey, TestUtilsV1.sourceValue);
            root.put("metadata", metadata);

            items.add(root);
        }

        return items;
    }

    public static List<TimeseriesMetadata> generateTsHeaderObjects(int noObjects) {
        List<TimeseriesMetadata> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(TimeseriesMetadata.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setName("test_ts_" + RandomStringUtils.randomAlphanumeric(5))
                    .setIsString(false)
                    .setIsStep(false)
                    .setDescription(RandomStringUtils.randomAlphanumeric(50))
                    .setUnit("TestUnits")
                    .putMetadata("type", TestUtilsV1.sourceValue)
                    .putMetadata(sourceKey, TestUtilsV1.sourceValue)
                    .build());
        }
        return objects;
    }

    public static List<Map<String, Object>> generateTsDatapointsItems(int noItems, double frequency, List<String> externalIdList) {
        List<Map<String, Object>> items = new ArrayList<>(externalIdList.size());
        for (String externalId : externalIdList) {
            Map<String, Object> root = new HashMap<>();
            List<Map<String, Object>> datapoints = new ArrayList<>(noItems);
            Instant timeStamp = Instant.now();

            root.put("externalId", externalId);
            for (int i = 0; i < noItems; i++) {
                timeStamp = timeStamp.minusMillis(Math.round(1000l / frequency));
                Map<String, Object> datapoint = ImmutableMap.of(
                        "timestamp", timeStamp.toEpochMilli(),
                        "value", ThreadLocalRandom.current().nextLong(-10, 20)
                );
                datapoints.add(datapoint);
            }
            root.put("datapoints", datapoints);

            items.add(root);
        }

        return items;
    }

    public static List<TimeseriesPointPost> generateTsDatapointsObjects(int noItems, double frequency,
                                                                        List<String> externalIdList) {
        List<TimeseriesPointPost> items = new ArrayList<>(noItems * externalIdList.size());
        for (String externalId : externalIdList) {
            items.addAll(generateTsDatapointsObjects(noItems, frequency, externalId));
        }
        return items;
    }

    public static List<TimeseriesPointPost> generateTsDatapointsObjects(int noItems, double frequency, String externalId) {
        List<TimeseriesPointPost> items = new ArrayList<>(noItems);
        Instant timeStamp = Instant.now();

        for (int i = 0; i < noItems; i++) {
            timeStamp = timeStamp.minusMillis(Math.round(1000l / frequency));
            items.add(TimeseriesPointPost.newBuilder()
                        .setExternalId(externalId)
                        .setTimestamp(timeStamp.toEpochMilli())
                        .setValueNum(ThreadLocalRandom.current().nextLong(-10, 20))
                        .build());
        }
        return items;
    }

    public static List<SequenceMetadata> generateSequenceMetadata(int noObjects) {
        List<SequenceMetadata> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            List<SequenceColumn> columns = new ArrayList<>();
            int noColumns = ThreadLocalRandom.current().nextInt(2,200);
            for (int j = 0; j < noColumns; j++) {
                columns.add(SequenceColumn.newBuilder()
                        .setExternalId(RandomStringUtils.randomAlphanumeric(20))
                        .setName("test_column_" + RandomStringUtils.randomAlphanumeric(5))
                        .setDescription(RandomStringUtils.randomAlphanumeric(50))
                        .setValueTypeValue(ThreadLocalRandom.current().nextInt(0,2))
                        .build());
            }

            objects.add(SequenceMetadata.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setName("test_sequence_" + RandomStringUtils.randomAlphanumeric(5))
                    .setDescription(RandomStringUtils.randomAlphanumeric(50))
                    .putMetadata("type", TestUtilsV1.sourceValue)
                    .putMetadata(sourceKey, TestUtilsV1.sourceValue)
                    .addAllColumns(columns)
                    .build());
        }
        return objects;
    }

    public static SequenceBody generateSequenceRows(SequenceMetadata header, int noRows) {
        List<SequenceColumn> columns = new ArrayList<>(header.getColumnsCount());
        List<SequenceRow> rows = new ArrayList<>(noRows);
        for (int i = 0; i < header.getColumnsCount(); i++) {
            columns.add(SequenceColumn.newBuilder()
                    .setExternalId(header.getColumns(i).getExternalId())
                    .build());
        }
        for (int i = 0; i < noRows; i++) {
            List<Value> values = new ArrayList<>(header.getColumnsCount());
            for (int j = 0; j < header.getColumnsCount(); j++) {
                if (ThreadLocalRandom.current().nextInt(1000) <= 2) {
                    // Add a random null value for for 0.1% of the values.
                    // Sequences support null values so we need to test for this
                    values.add(Values.ofNull());
                } else if (header.getColumns(j).getValueType() == SequenceColumn.ValueType.DOUBLE) {
                    values.add(Values.of(ThreadLocalRandom.current().nextDouble(1000000d)));
                } else if (header.getColumns(j).getValueType() == SequenceColumn.ValueType.LONG) {
                    values.add(Values.of(ThreadLocalRandom.current().nextLong(10000000)));
                } else {
                    values.add(Values.of(RandomStringUtils.randomAlphanumeric(20)));
                }
            }
            rows.add(SequenceRow.newBuilder()
                    .setRowNumber(i)
                    .addAllValues(values)
                    .build());
        }

        return SequenceBody.newBuilder()
                .setExternalId(header.getExternalId())
                .addAllColumns(columns)
                .addAllRows(rows)
                .build();
    }

    public static List<Event> generateEvents(int noObjects) {
        List<Event> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(Event.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setStartTime(1552566113 + ThreadLocalRandom.current().nextInt(10000))
                    .setEndTime(1553566113 + ThreadLocalRandom.current().nextInt(10000))
                    .setDescription("test_event_" + RandomStringUtils.randomAlphanumeric(50))
                    .setType("test_event")
                    .setSubtype(ThreadLocalRandom.current()
                            .nextInt(0,2) == 0 ? "test_event_sub_type" : "test_event_sub_type_2")
                    .putMetadata("type", TestUtilsV1.sourceValue)
                    .putMetadata(sourceKey, TestUtilsV1.sourceValue)
                    .build());
        }
        return objects;
    }
}
