package com.cognite.client;

import com.cognite.client.dto.*;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtils {
    public static final String sourceKey = "source";
    public static final String sourceValue = "unitTest";
    public static final String updatedSourceValue = "unitTestModified";

    private static final long SECOND_MS = 1000L;
    private static final long MINUTE_MS = 60L * SECOND_MS;
    private static final long HOUR_MS = 60L * MINUTE_MS;
    private static final long DAY_MS = 24L * HOUR_MS;

    public static List<TimeseriesMetadata> generateTsHeaderObjects(int noObjects) {
        List<TimeseriesMetadata> objects = new ArrayList<>(noObjects);
        for (int i = 0; i < noObjects; i++) {
            objects.add(TimeseriesMetadata.newBuilder()
                    .setExternalId(StringValue.of(RandomStringUtils.randomAlphanumeric(10)))
                    .setName(StringValue.of("test_ts_" + RandomStringUtils.randomAlphanumeric(5)))
                    .setIsString(false)
                    .setIsStep(false)
                    .setDescription(StringValue.of(RandomStringUtils.randomAlphanumeric(50)))
                    .setUnit(StringValue.of("TestUnits"))
                    .putMetadata("type", TestUtils.sourceValue)
                    .putMetadata(sourceKey, TestUtils.sourceValue)
                    .build());
        }
        return objects;
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
                        .setName(StringValue.of("test_column_" + RandomStringUtils.randomAlphanumeric(5)))
                        .setDescription(StringValue.of(RandomStringUtils.randomAlphanumeric(50)))
                        .setValueTypeValue(ThreadLocalRandom.current().nextInt(0,2))
                        .build());
            }

            objects.add(SequenceMetadata.newBuilder()
                    .setExternalId(StringValue.of(RandomStringUtils.randomAlphanumeric(10)))
                    .setName(StringValue.of("test_sequence_" + RandomStringUtils.randomAlphanumeric(5)))
                    .setDescription(StringValue.of(RandomStringUtils.randomAlphanumeric(50)))
                    .putMetadata("type", TestUtils.sourceValue)
                    .putMetadata(sourceKey, TestUtils.sourceValue)
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
                    .setExternalId(StringValue.of(RandomStringUtils.randomAlphanumeric(10)))
                    .setStartTime(Int64Value.of(1552566113 + ThreadLocalRandom.current().nextInt(10000)))
                    .setEndTime(Int64Value.of(1553566113 + ThreadLocalRandom.current().nextInt(10000)))
                    .setDescription(StringValue.of("test_event_" + RandomStringUtils.randomAlphanumeric(50)))
                    .setType(StringValue.of("test_event"))
                    .setSubtype(StringValue.of(
                            ThreadLocalRandom.current().nextInt(0,2) == 0 ? "test_event_sub_type" : "test_event_sub_type_2"))
                    .setSource(StringValue.of(sourceValue))
                    .putMetadata("type", TestUtils.sourceValue)
                    .putMetadata(sourceKey, TestUtils.sourceValue)
                    .build());
        }
        return objects;
    }
}
