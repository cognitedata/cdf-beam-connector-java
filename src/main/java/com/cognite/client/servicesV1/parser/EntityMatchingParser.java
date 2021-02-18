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
import com.cognite.client.dto.EntityMatch;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.util.ArrayList;
import java.util.List;


public class EntityMatchingParser {
    static final String logPrefix = "EntityMatchingParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Extract the [source] entry from a result item. The result item contains a single [source]
     * object that is mapped to a {@link Struct}.
     *
     * @param item
     * @return
     * @throws Exception
     */
    private Struct parseEntityMatcherResultItemMatchFrom(String item) throws Exception {
        JsonNode root = objectMapper.readTree(item);

        if (root.path("source").isObject()) {
            Struct.Builder structBuilder = Struct.newBuilder();
            JsonFormat.parser().merge(root.path("source").toString(), structBuilder);
            return structBuilder.build();
        } else {
            throw new Exception("Unable to parse result item. "
                    + "Result does not contain a valid [source] node. "
                    + "Source: " + root.path("source").getNodeType());
        }
    }

    /**
     * Extract the [target] entries from a result item. The result item contains 0..N
     * [target] entries.
     *
     * @param item
     * @return
     * @throws Exception
     */
    private List<EntityMatch> parseEntityMatcherResultItemMatchTo(String item) throws Exception {
        JsonNode root = objectMapper.readTree(item);
        List<EntityMatch> matches = new ArrayList<>(10);

        if (root.path("matches").isArray()) {
            for (JsonNode node : root.path("matches")) {
                if (node.isObject()) {
                    EntityMatch.Builder entityMatchBuilder = EntityMatch.newBuilder();
                    if (node.path("target").isObject()) {
                        Struct.Builder structBuilder = Struct.newBuilder();
                        JsonFormat.parser().merge(node.path("target").toString(), structBuilder);
                        entityMatchBuilder.setTarget(structBuilder.build());
                    } else {
                        throw new Exception("Unable to parse result item. "
                                + "Result does not contain a valid [target] node. "
                                + "Target: " + node.path("target").getNodeType());
                    }

                    if (node.path("score").isNumber()) {
                        entityMatchBuilder.setScore(DoubleValue.of(node.path("score").doubleValue()));
                    }
                    matches.add(entityMatchBuilder.build());
                } else {
                    throw new Exception("Unable to parse result item. "
                            + "Result does not contain a valid [matches] entry node. "
                            + "maches: " + node.getNodeType());
                }
            }
        } else {
            throw new Exception("Unable to parse result item. "
                    + "Result does not contain a valid [matches] array node. "
                    + "matches array: " + root.path("matches").getNodeType());
        }

        return matches;
    }

    private static String buildErrorMessage(String fieldName, String inputElement) {
        return logPrefix + "Unable to parse attribute: "+ fieldName + ". Item payload: "
                + inputElement.substring(0, Math.min(inputElement.length() - 1, CogniteIO.MAX_LOG_ELEMENT_LENGTH));
    }
}
