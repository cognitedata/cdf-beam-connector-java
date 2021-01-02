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

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.TimeseriesPoint;
import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.cognite.beam.io.servicesV1.parser.ItemParser;
import com.cognite.beam.io.servicesV1.parser.TimeseriesParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.cognite.v1.timeseries.proto.DataPointListItem;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;


/**
 * This function reads time series data points based on the input RequestParameters.
 *
 *
 */
public class ReadTsPointProto extends DoFn<RequestParameters, Iterable<TimeseriesPoint>> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ConnectorServiceV1 connector;
    private final boolean isMetricsEnabled;
    private final boolean isStreaming;
    private final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    private final Distribution tsPointsBatch = Metrics.distribution("cognite", "tsPointsBatchSize");
    private final Distribution tsBatch = Metrics.distribution("cognite", "numberOfTS");
    private final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    public ReadTsPointProto(Hints hints, ReaderConfig readerConfig) {
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();

        isMetricsEnabled = readerConfig.isMetricsEnabled();
        isStreaming = readerConfig.isStreamingEnabled();
    }

    @Setup
    public void setup() {
        LOG.info("Setting up TS point proto reader.");
    }

    @ProcessElement
    public void processElement(@Element RequestParameters query,
                               OutputReceiver<Iterable<TimeseriesPoint>> out) throws Exception {
        final String batchIdentifierPrefix = "ReadTsPointProto - Request id: "
                + RandomStringUtils.randomAlphanumeric(6) + " - ";

        LOG.info(batchIdentifierPrefix + "Streaming mode: {}", isStreaming);
        LOG.debug(batchIdentifierPrefix + "Sending query to the Cognite data platform: {}", query.toString());

        try {
            Iterator<CompletableFuture<ResponseItems<DataPointListItem>>> results =
                    connector.readTsDatapointsProto(query);

            CompletableFuture<ResponseItems<DataPointListItem>> responseItemsFuture;
            ResponseItems<DataPointListItem> responseItems;

            while (results.hasNext()) {
                responseItemsFuture = results.next();
                responseItems = responseItemsFuture.join();
                int tsPointsCounter = 0;
                List<TimeseriesPoint> pointsOutput = new ArrayList<>(20000);
                long minTimestampMs = Long.MAX_VALUE;

                if (!responseItems.isSuccessful()) {
                    // something went wrong with the request
                    String message = batchIdentifierPrefix + "Error while iterating through the results from Fusion: "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }

                // Gather all TS points
                for (DataPointListItem item : responseItems.getResultsItems()) {
                    List<TimeseriesPoint> points = TimeseriesParser.parseDataPointListItem(item);
                    for (TimeseriesPoint point : points) {
                        pointsOutput.add(point);
                        if (minTimestampMs > point.getTimestamp()) {
                            minTimestampMs = point.getTimestamp();
                        }
                        tsPointsCounter++;
                    }
                }

                if (isStreaming) {
                    // output with timestamps in streaming mode--need that for windowing
                    out.outputWithTimestamp(pointsOutput, org.joda.time.Instant.ofEpochMilli(minTimestampMs));
                } else {
                    // no timestamping in batch mode--just leads to lots of complications
                    out.output(pointsOutput);
                }
                LOG.info(batchIdentifierPrefix + "Received {} TS and {} datapoints in the response.",
                        responseItems.getResultsItems().size(),
                        tsPointsCounter);

                if (isMetricsEnabled) {
                    MetricsUtil.recordApiLatency(responseItems, apiLatency);
                    MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                    tsBatch.update(responseItems.getResultsItems().size());
                    tsPointsBatch.update(tsPointsCounter);
                }
            }
        } catch (Exception e) {
            LOG.error(batchIdentifierPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
