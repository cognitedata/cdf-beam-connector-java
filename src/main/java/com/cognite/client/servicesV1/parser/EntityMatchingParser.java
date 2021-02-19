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

import com.cognite.client.dto.EntityMatch;
import com.cognite.client.dto.EntityMatchResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

/**
 * Methods for parsing entity matching results json into typed objects.
 */
public class EntityMatchingParser {
    static final String logPrefix = "EntityMatchingParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses an entity matching result json string into a typed {@link EntityMatchResult}.
     * @param json
     * @return
     * @throws Exception
     */
    public static EntityMatchResult parseEntityMatchResult(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);

        EntityMatchResult.Builder responseBuilder = EntityMatchResult.newBuilder();
        if (root.path("source").isObject()) {
            Struct.Builder structBuilder = Struct.newBuilder();
            JsonFormat.parser().merge(root.path("source").toString(), structBuilder);
            responseBuilder.setSource(structBuilder.build());
        } else {
            throw new Exception(logPrefix + "Unable to parse result item. "
                    + "Result does not contain a valid [source] node. "
                    + "Source: " + root.path("source").getNodeType());
        }

        if (root.path("matches").isArray()) {
            for (JsonNode node : root.path("matches")) {
                if (node.isObject()) {
                    EntityMatch.Builder entityMatchBuilder = EntityMatch.newBuilder();
                    if (node.path("target").isObject()) {
                        Struct.Builder structBuilder = Struct.newBuilder();
                        JsonFormat.parser().merge(node.path("target").toString(), structBuilder);
                        entityMatchBuilder.setTarget(structBuilder.build());
                    } else {
                        throw new Exception(logPrefix + "Unable to parse result item. "
                                + "Result does not contain a valid [target] node. "
                                + "Target: " + node.path("target").getNodeType());
                    }

                    if (node.path("score").isNumber()) {
                        entityMatchBuilder.setScore(DoubleValue.of(node.path("score").doubleValue()));
                    }
                    responseBuilder.addMatches(entityMatchBuilder.build());
                } else {
                    throw new Exception(logPrefix + "Unable to parse result item. "
                            + "Result does not contain a valid [matches] entry node. "
                            + "matches: " + node.getNodeType());
                }
            }
        } else {
            throw new Exception(logPrefix + "Unable to parse result item. "
                    + "Result does not contain a valid [matches] array node. "
                    + "matches array: " + root.path("matches").getNodeType());
        }

        return responseBuilder.build();
    }
}
