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

package com.cognite.beam.io.fn.read;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.RawRow;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.RawParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.Hints;

/**
 * This function reads a set of rows from raw based on the input RequestParameters.
 *
 *
 */
public class ReadRawRow extends DoFn<RequestParameters, RawRow> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ConnectorServiceV1 connector;
    private final boolean isStreaming;
    private final boolean isMetricsEnabled;

    private final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    private final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");
    private final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    public ReadRawRow(Hints hints, ReaderConfig readerConfig) {
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();

        isStreaming = readerConfig.isStreamingEnabled();
        isMetricsEnabled = readerConfig.isMetricsEnabled();
    }

    @Setup
    public void setup() {
        LOG.info("Setting up ReadRawRow.");
    }

    @ProcessElement
    public void processElement(@Element RequestParameters query, OutputReceiver<RawRow> outputReceiver,
                               ProcessContext context) throws Exception {
        Preconditions.checkArgument(query.getRequestParameters().containsKey("dbName")
                        && query.getRequestParameters().get("dbName") instanceof String,
                "Request parameters must include dnName with a string value");
        Preconditions.checkArgument(query.getRequestParameters().containsKey("tableName")
                        && query.getRequestParameters().get("tableName") instanceof String,
                "Request parameters must include tableName");

        final String batchIdentifierPrefix = "Request id: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        LOG.info(batchIdentifierPrefix + "Sending query to the Cognite data platform: {}", query.toString());

        try {
            Iterator<CompletableFuture<ResponseItems<String>>> results = connector.readRawRows(query);
            CompletableFuture<ResponseItems<String>> responseItemsFuture;
            ResponseItems<String> responseItems;

            while (results.hasNext()) {
                responseItemsFuture = results.next();
                responseItems = responseItemsFuture.join();
                Optional<Long> lastUpdatedTime = Optional.empty();

                if (!responseItems.isSuccessful()) {
                    // something went wrong with the request
                    String message = batchIdentifierPrefix + "Error while iterating through the results from Fusion: "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }

                if (isMetricsEnabled) {
                    MetricsUtil.recordApiLatency(responseItems, apiLatency);
                    MetricsUtil.recordApiBatchSize(responseItems, apiBatchSize);
                    MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                }

                if (isStreaming) {
                    for (String item : responseItems.getResultsItems()) {
                        RawRow row = RawParser.parseRawRow(
                                (String) query.getRequestParameters().get("dbName"),
                                (String) query.getRequestParameters().get("tableName"),
                                item);

                        outputReceiver.outputWithTimestamp(row,
                                org.joda.time.Instant.ofEpochMilli(row.getLastUpdatedTime().getValue()));
                    }
                } else {
                    for (String item : responseItems.getResultsItems()) {
                        outputReceiver.output(RawParser.parseRawRow(
                                (String) query.getRequestParameters().get("dbName"),
                                (String) query.getRequestParameters().get("tableName"),
                                item));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(batchIdentifierPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
