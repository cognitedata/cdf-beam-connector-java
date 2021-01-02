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

package com.cognite.beam.io.fn.request;

import com.cognite.beam.io.servicesV1.RequestParameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This function builds a read raw table request based on input from a config file.
 *
 * It is intended to be used downstream of the TOML reader which produces a collection of {@code KV<String, String>}
 * object representing a config map.
 *
 * You configure which input key(s) to read from. The values must contain a string with minimum two parts:
 * raw dbName and tableName. The value string is split using a custom delimiter.
 */
public class BuildReadRawRequestFn extends DoFn<KV<String, String>, RequestParameters> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ImmutableList<String> lookupKeys;
    private final ImmutableList<String> columns;
    private final String splitter; //

    public BuildReadRawRequestFn(List<String> keys,
                          String splitter,
                          List<String> columns) {
        Preconditions.checkNotNull(keys, "Key cannot be null.");
        Preconditions.checkNotNull(columns, "Column spec cannot be null.");
        lookupKeys = ImmutableList.copyOf(keys);
        this.splitter = splitter;
        this.columns = ImmutableList.copyOf(columns);
    }

    @ProcessElement
    public void processElement(@DoFn.Element KV<String, String> input,
                               OutputReceiver<RequestParameters> out,
                               ProcessContext context) throws Exception {
        if (lookupKeys.contains(input.getKey())) {
            if (input.getValue().split(splitter).length < 2) {
                LOG.warn("Cannot build read raw table request. Could not find db name and table name from "
                        + "input with key [{}], value [{}] and splitter [{}]",
                        input.getKey(), input.getValue(), splitter);
                return;
            }
            LOG.info("Building read raw table request for key [{}] with value [{}]",
                    input.getKey(), input.getValue());

            RequestParameters request = RequestParameters.create()
                    .withDbName(input.getValue().split(splitter)[0])
                    .withTableName(input.getValue().split(splitter)[1]);

            // Check if we should add a column name specification.
            if (!columns.isEmpty()) {
                String columnSpec = columns.stream().collect(Collectors.joining(","));
                request = request
                        .withRootParameter("columns", columnSpec);
            }
            LOG.debug("Request: {}", request.toString());

            out.output(request);
        }
    }
}
