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

package com.cognite.client.servicesV1.parser;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.dto.DataSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;

import java.util.Iterator;
import java.util.Map;

/**
 * This class contains a set of methods to help parsing data set objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class DataSetParser {
    static final String logPrefix = "DataSetParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a data set json string to <code>DataSet</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static DataSet parseDataSet(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        DataSet.Builder dataSetBuilder = DataSet.newBuilder();

        // An event must contain an id.
        if (root.path("id").isIntegralNumber()) {
            dataSetBuilder.setId(Int64Value.of(root.get("id").longValue()));
        } else {
            String message = logPrefix + "Unable to parse attribute: id. Item exerpt: "
                    + json.substring(0, Math.min(json.length() - 1, CogniteIO.MAX_LOG_ELEMENT_LENGTH));
            throw new Exception(message);
        }

        // The rest of the attributes are optional.
        if (root.path("externalId").isTextual()) {
            dataSetBuilder.setExternalId(StringValue.of(root.get("externalId").textValue()));
        }
        if (root.path("name").isTextual()) {
            dataSetBuilder.setName(
                    StringValue.of(root.get("name").textValue()));
        }
        if (root.path("description").isTextual()) {
            dataSetBuilder.setDescription(
                    StringValue.of(root.get("description").textValue()));
        }
        if (root.path("writeProtected").isBoolean()) {
            dataSetBuilder.setWriteProtected(
                    BoolValue.of(root.get("writeProtected").booleanValue()));
        }
        if (root.path("createdTime").isIntegralNumber()) {
            dataSetBuilder.setCreatedTime(Int64Value.of(root.get("createdTime").longValue()));
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            dataSetBuilder.setLastUpdatedTime(Int64Value.of(root.get("lastUpdatedTime").longValue()));
        }

        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root
                    .path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    dataSetBuilder
                            .putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }

       return dataSetBuilder.build();
    }

    /**
     * Builds a request insert item object from <code>DataSet</code>.
     *
     * An insert item object creates a new data set object in the Cognite system.
     *
     * @param element
     * @return
     * @throws Exception
     */
    public static Map<String, Object> toRequestInsertItem(DataSet element) throws Exception {
        // Note that "id" cannot be a part of an insert request.
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId().getValue());
        }

        if (element.hasName()) {
            mapBuilder.put("name", element.getName().getValue());
        }
        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription().getValue());
        }
        if (element.hasWriteProtected()) {
            mapBuilder.put("writeProtected", element.getWriteProtected().getValue());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from <code>DataSet</code>.
     *
     * An update item object updates an existing data set object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(DataSet element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

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
        if (element.hasWriteProtected()) {
            updateNodeBuilder.put("writeProtected", ImmutableMap.of("set", element.getWriteProtected().getValue()));
        }
        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request update replace item object from <code>DataSet</code>.
     *
     * A replace item object replaces an existing data set object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(DataSet element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

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

        if (element.hasWriteProtected()) {
            updateNodeBuilder.put("writeProtected", ImmutableMap.of("set", element.getWriteProtected().getValue()));
        } else {
            updateNodeBuilder.put("writeProtected", ImmutableMap.of("setNull", true));
        }


        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }
}
