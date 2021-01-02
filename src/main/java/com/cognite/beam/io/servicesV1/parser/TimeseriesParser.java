/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.beam.io.servicesV1.parser;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.dto.TimeseriesMetadata;
import com.cognite.beam.io.dto.TimeseriesPoint;
import com.cognite.v1.timeseries.proto.AggregateDatapoint;
import com.cognite.v1.timeseries.proto.DataPointListItem;
import com.cognite.v1.timeseries.proto.NumericDatapoint;
import com.cognite.v1.timeseries.proto.StringDatapoint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class contains a set of methods to help parsing timeseries object between Cognite api representations
 * (json and proto) and typed objects.
 */
public class TimeseriesParser {
    static final Logger LOG = LoggerFactory.getLogger(TimeseriesParser.class);
    static final String logPrefix = "TimeseriesParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a <code>DataPointListItem</code> (proto payload from the Cognite api) into a list
     * of <code>TimeseriesPoint</code>
     *
     * @return
     */
    public static List<TimeseriesPoint> parseDataPointListItem(DataPointListItem item) {
        // Find the no data points
        int inputLength = 0;
        if (item.getDatapointTypeCase() == DataPointListItem.DatapointTypeCase.NUMERICDATAPOINTS) {
            inputLength = item.getNumericDatapoints().getDatapointsCount();
        } else if (item.getDatapointTypeCase() == DataPointListItem.DatapointTypeCase.STRINGDATAPOINTS) {
            inputLength = item.getStringDatapoints().getDatapointsCount();
        } else if (item.getDatapointTypeCase() == DataPointListItem.DatapointTypeCase.AGGREGATEDATAPOINTS) {
            inputLength = item.getAggregateDatapoints().getDatapointsCount();
        }
        List<TimeseriesPoint> outputList = new ArrayList<>(inputLength);

        // Hold the outer data attributes
        long id = item.getId();
        Optional<String> externalId = Optional.empty();
        if (!item.getExternalId().isEmpty()) {
            externalId = Optional.of(item.getExternalId());
        }
        boolean isStep = item.getIsStep();

        if (item.getDatapointTypeCase() == DataPointListItem.DatapointTypeCase.NUMERICDATAPOINTS) {
            for (NumericDatapoint numPoint : item.getNumericDatapoints().getDatapointsList()) {
                TimeseriesPoint.Builder outputPointBuilder = TimeseriesPoint.newBuilder();
                outputPointBuilder.setId(id);
                if (externalId.isPresent()) {
                    outputPointBuilder.setExternalId(StringValue.of(externalId.get()));
                }
                outputPointBuilder.setIsStep(BoolValue.of(isStep));
                outputPointBuilder.setTimestamp(numPoint.getTimestamp());
                outputPointBuilder.setValueNum(numPoint.getValue());
                outputList.add(outputPointBuilder.build());
            }

        } else if (item.getDatapointTypeCase() == DataPointListItem.DatapointTypeCase.STRINGDATAPOINTS) {
            for (StringDatapoint stringPoint : item.getStringDatapoints().getDatapointsList()) {
                TimeseriesPoint.Builder outputPointBuilder = TimeseriesPoint.newBuilder();
                outputPointBuilder.setId(id);
                if (externalId.isPresent()) {
                    outputPointBuilder.setExternalId(StringValue.of(externalId.get()));
                }
                outputPointBuilder.setIsStep(BoolValue.of(isStep));
                outputPointBuilder.setTimestamp(stringPoint.getTimestamp());
                outputPointBuilder.setValueString(stringPoint.getValue());
                outputList.add(outputPointBuilder.build());
            }
        } else if (item.getDatapointTypeCase() == DataPointListItem.DatapointTypeCase.AGGREGATEDATAPOINTS) {
            for (AggregateDatapoint aggPoint : item.getAggregateDatapoints().getDatapointsList()) {
                TimeseriesPoint.Builder outputPointBuilder = TimeseriesPoint.newBuilder();
                outputPointBuilder.setId(id);
                if (externalId.isPresent()) {
                    outputPointBuilder.setExternalId(StringValue.of(externalId.get()));
                }
                outputPointBuilder.setIsStep(BoolValue.of(isStep));
                outputPointBuilder.setTimestamp(aggPoint.getTimestamp());

                TimeseriesPoint.Aggregates.Builder aggBuilder = TimeseriesPoint.Aggregates.newBuilder();
                aggBuilder.setAverage(DoubleValue.of(aggPoint.getAverage()))
                        .setMax(DoubleValue.of(aggPoint.getMax()))
                        .setMin(DoubleValue.of(aggPoint.getMin()))
                        .setCount(Int64Value.of(Math.round(aggPoint.getCount())))
                        .setSum(DoubleValue.of(aggPoint.getSum()))
                        .setInterpolation(DoubleValue.of(aggPoint.getInterpolation()))
                        .setStepInterpolation(DoubleValue.of(aggPoint.getStepInterpolation()))
                        .setContinuousVariance(DoubleValue.of(aggPoint.getContinuousVariance()))
                        .setDiscreteVariance(DoubleValue.of(aggPoint.getDiscreteVariance()))
                        .setTotalVariation(DoubleValue.of(aggPoint.getTotalVariation()))
                        .build();

                outputPointBuilder.setValueAggregates(aggBuilder.build());
                outputList.add(outputPointBuilder.build());
            }
        }
        return outputList;
    }

    /**
     * Parses a time series header json string to <code>TimeseriesMetadata</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static TimeseriesMetadata parseTimeseriesMetadata(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        TimeseriesMetadata.Builder builder = TimeseriesMetadata.newBuilder();
        String itemExerpt = json.substring(0, Math.min(json.length() - 1, CogniteIO.MAX_LOG_ELEMENT_LENGTH));

        // A TS metadata object must contain an id, isStep and isString.
        if (root.path("id").isIntegralNumber()) {
            builder.setId(Int64Value.of(root.get("id").longValue()));
        } else {
            throw new Exception("Unable to parse attribute: id. Item exerpt: " + itemExerpt);
        }

        if (root.path("isString").isBoolean()) {
            builder.setIsString(root.get("isString").booleanValue());
        } else {
            throw new Exception("Unable to parse attribute: isString. Item exerpt: " + itemExerpt);
        }

        if (root.path("isStep").isBoolean()) {
            builder.setIsStep(root.get("isStep").booleanValue());
        } else {
            builder.setIsStep(false);  // Temporary mitigation for the api sometimes failing to return a valid value
            // TODO, Update TS header parsing of "isStep".
      /*
      throw new RuntimeException("Unable to parse attribute: isStep. Item exerpt: " + itemExerpt);
              */
        }

        // The rest of the attributes are optional.
        if (root.path("externalId").isTextual()) {
            builder.setExternalId(StringValue.of(root.get("externalId").textValue()));
        }
        if (root.path("name").isTextual()) {
            builder.setName(StringValue.of(root.get("name").textValue()));
        }
        if (root.path("description").isTextual()) {
            builder.setDescription(StringValue.of(root.get("description").textValue()));
        }
        if (root.path("unit").isTextual()) {
            builder.setUnit(StringValue.of(root.get("unit").textValue()));
        }
        if (root.path("assetId").isIntegralNumber()) {
            builder.setAssetId(Int64Value.of(root.get("assetId").longValue()));
        }
        if (root.path("createdTime").isIntegralNumber()) {
            builder.setCreatedTime(Int64Value.of(root.get("createdTime").longValue()));
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            builder.setLastUpdatedTime(Int64Value.of(root.get("lastUpdatedTime").longValue()));
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            builder.setDataSetId(Int64Value.of(root.get("dataSetId").longValue()));
        }

        if (root.path("securityCategories").isArray()) {
            for (JsonNode node : root.path("securityCategories")) {
                if (node.isIntegralNumber()) {
                    builder.addSecurityCategories(node.longValue());
                }
            }
        }

        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root.path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    builder.putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }

        return builder.build();
    }

    /**
     * Builds a request insert item object from <code>TimeseriesMetadata</code>.
     *
     * An insert item object creates a new TS header data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(TimeseriesMetadata element) {
        // "legacyName" is populated based on "externalId".
        // Note that "id" cannot be a part of the insert object.

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            LOG.warn(logPrefix
                    + "The time series data object does not contain an externalId. Therefore legacyName cannot be"
                    + " populated. This prevents this object to be readable through earlier API versions (pre v1).");
        }

        if (element.hasName()) {
            mapBuilder.put("name", element.getName().getValue());
        }
        mapBuilder.put("isString", element.getIsString());
        mapBuilder.put("isStep", element.getIsStep());
        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription().getValue());
        }
        if (element.hasUnit()) {
            mapBuilder.put("unit", element.getUnit().getValue());
        }
        if (element.hasAssetId()) {
            mapBuilder.put("assetId", element.getAssetId().getValue());
        }
        if (element.getSecurityCategoriesCount() > 0) {
            mapBuilder.put("securityCategories", element.getSecurityCategoriesList());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId().getValue());
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from <code>TimeseriesMetadata</code>.
     *
     * An update item object updates an existing TS header object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(TimeseriesMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        // "isString" and "isStep" are immutable properties and will be ignored when building the update item

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        if (element.hasName()) {
            updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName().getValue()));
        }
        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription().getValue()));
        }
        if (element.hasUnit()) {
            updateNodeBuilder.put("unit", ImmutableMap.of("set", element.getUnit().getValue()));
        }
        if (element.hasAssetId()) {
            updateNodeBuilder.put("assetId", ImmutableMap.of("set", element.getAssetId().getValue()));
        }
        if (element.getSecurityCategoriesCount() > 0) {
            updateNodeBuilder.put("securityCategories", ImmutableMap.of("set", element.getSecurityCategoriesList()));
        }
        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }
        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId().getValue()));
        }
        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request insert item object from <code>TimeseriesMetadata</code>.
     *
     * A replace item object replaces an existingTS header object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(TimeseriesMetadata element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        // "isString" and "isStep" are immutable properties and will be ignored when building the update item

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        } else {
            mapBuilder.put("id", element.getId().getValue());
        }

        if (element.hasName()) {
            updateNodeBuilder.put("name", ImmutableMap.of("set", element.getName().getValue()));
        } else {
            updateNodeBuilder.put("name", ImmutableMap.of("setNull", true));
        }
        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription().getValue()));
        } else {
            updateNodeBuilder.put("description", ImmutableMap.of("setNull", true));
        }

        if (element.hasUnit()) {
            updateNodeBuilder.put("unit", ImmutableMap.of("set", element.getUnit().getValue()));
        } else {
            updateNodeBuilder.put("unit", ImmutableMap.of("setNull", true));
        }

        if (element.hasAssetId()) {
            updateNodeBuilder.put("assetId", ImmutableMap.of("set", element.getAssetId().getValue()));
        } else {
            updateNodeBuilder.put("assetId", ImmutableMap.of("setNull", true));
        }

        if (element.getSecurityCategoriesCount() > 0) {
            updateNodeBuilder.put("securityCategories", ImmutableMap.of("set", element.getSecurityCategoriesList()));
        } else {
            updateNodeBuilder.put("securityCategories", ImmutableMap.of("set", ImmutableList.<Long>of()));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId().getValue()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }
}
