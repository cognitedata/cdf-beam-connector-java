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

package com.cognite.beam.io.fn.parse;

import java.util.Optional;

import static com.cognite.beam.io.CogniteIO.MAX_LOG_ELEMENT_LENGTH;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cognite.client.dto.TimeseriesPoint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ParseTimeseriesPointFn extends DoFn<String, TimeseriesPoint> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String parseErrorDefaultPrefix = "Parsing error. Unable to parse result item. ";

    @Setup
    public void setup() {
        LOG.debug("Setting up ParseTimeseriesPointFn.");
    }

    @ProcessElement
    public void processElement(@Element String inputElement, OutputReceiver<TimeseriesPoint> out) throws Exception {
        JsonNode root = objectMapper.readTree(inputElement);

        try {
            // Hold the outer data attributes
            long id = -1;
            Optional<String> externalId = Optional.empty();
            Optional<Boolean> isString = Optional.empty();
            Optional<Boolean> isStep = Optional.empty();

            // A TS point must have an id.
            if (root.path("id").isIntegralNumber()) {
                id = root.get("id").longValue();
            } else {
                throw new Exception(parseErrorDefaultPrefix + "Unable to parse attribute: id. Item exerpt: "
                        + inputElement
                        .substring(0, Math.min(inputElement.length() - 1, MAX_LOG_ELEMENT_LENGTH)));
            }

            // Optional outer attributes
            if (root.path("externalId").isTextual()) {
                externalId = Optional.of(root.get("externalId").textValue());
            }
            if (root.path("isString").isBoolean()) {
                isString = Optional.of(root.get("isString").booleanValue());
            }
            if (root.path("isStep").isBoolean()) {
                isStep = Optional.of(root.get("isStep").booleanValue());
            }

            // Parse the inner array and produce one output per inner item.
            if (root.path("datapoints").isArray()) {
                ArrayNode datapoints = (ArrayNode) root.path("datapoints");
                for (JsonNode node : datapoints) {
                    TimeseriesPoint.Builder tsPointBuilder = TimeseriesPoint.newBuilder();
                    // a datapoint must have a timestamp
                    if (node.path("timestamp").isIntegralNumber()) {
                        tsPointBuilder.setTimestamp(node.get("timestamp").longValue());
                    } else {
                        throw new Exception(parseErrorDefaultPrefix + "Unable to parse attribute: datapoint.timestamp. Item exerpt: "
                                + node.toString()
                                .substring(0, Math.min(node.toString().length() - 1, MAX_LOG_ELEMENT_LENGTH)));
                    }
                    // all constraints are satisfied, add outer fields
                    tsPointBuilder.setId(id);
                    if (externalId.isPresent()) {
                        tsPointBuilder.setExternalId(StringValue.of(externalId.get()));
                    }
                    if (isStep.isPresent()) {
                        tsPointBuilder.setIsStep(BoolValue.of(isStep.get()));
                    }

                    // other values are optional, add inner fields.
                    // Can be one of three states: 1) numeric, 2) string and 3) aggregate
                    if (isString.isPresent() && !isString.get()) {
                        if (node.path("value").isNumber()) {
                            tsPointBuilder.setValueNum(node.path("value").doubleValue());
                        }
                    } else if (isString.isPresent() && isString.get()) {
                        if (node.path("value").isTextual()) {
                            tsPointBuilder.setValueString(node.path("value").textValue());
                        }
                    } else {
                        TimeseriesPoint.Aggregates.Builder aggBuilder = TimeseriesPoint.Aggregates.newBuilder();
                        if (node.path("average").isNumber()) {
                            aggBuilder.setAverage(DoubleValue.of(node.path("average").doubleValue()));
                        }
                        if (node.path("max").isNumber()) {
                            aggBuilder.setMax(DoubleValue.of(node.path("max").doubleValue()));
                        }
                        if (node.path("min").isNumber()) {
                            aggBuilder.setMin(DoubleValue.of(node.path("min").doubleValue()));
                        }
                        if (node.path("count").isIntegralNumber()) {
                            aggBuilder.setCount(Int64Value.of(node.path("count").longValue()));
                        }
                        if (node.path("sum").isNumber()) {
                            aggBuilder.setSum(DoubleValue.of(node.path("sum").doubleValue()));
                        }
                        if (node.path("interpolation").isNumber()) {
                            aggBuilder.setInterpolation(DoubleValue.of(node.path("interpolation").doubleValue()));
                        }
                        if (node.path("stepInterpolation").isNumber()) {
                            aggBuilder.setStepInterpolation(DoubleValue.of(node.path("stepInterpolation").doubleValue()));
                        }
                        if (node.path("continuousVariance").isNumber()) {
                            aggBuilder.setContinuousVariance(DoubleValue.of(node.path("continuousVariance").doubleValue()));
                        }
                        if (node.path("discreteVariance").isNumber()) {
                            aggBuilder.setDiscreteVariance(DoubleValue.of(node.path("discreteVariance").doubleValue()));
                        }
                        if (node.path("totalVariation").isNumber()) {
                            aggBuilder.setTotalVariation(DoubleValue.of(node.path("totalVariation").doubleValue()));
                        }

                        tsPointBuilder.setValueAggregates(aggBuilder.build());
                    }
                    out.output(tsPointBuilder.build());
                }
            } else {
                LOG.warn("No datapoints array in results item. Item exerpt: "
                        + inputElement
                        .substring(0, Math.min(inputElement.length() - 1, MAX_LOG_ELEMENT_LENGTH)));
            }
        } catch (Exception e) {
            LOG.error(parseErrorDefaultPrefix, e);
            throw e;
        }
    }
}
