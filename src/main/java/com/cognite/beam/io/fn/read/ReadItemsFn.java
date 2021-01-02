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
import com.cognite.beam.io.fn.ResourceType;
import com.cognite.beam.io.servicesV1.*;
import com.cognite.beam.io.util.internal.MetricsUtil;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the results items from a read request to Cognite. This function will read the response results from
 * a single request and send them to the output. It will not iterate over large results collections
 * that require multiple request. If you need that functionality, please refer to {@link ReadItemsIteratorFn}.
 *
 * This function is normally used to read results from the various read endpoints that provide all results
 * in a single response, such as {@code aggregates} endpoints.
 */
public class ReadItemsFn extends DoFn<RequestParameters, String> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ResourceType resourceType;
    private final ConnectorServiceV1 connector;
    private final boolean isMetricsEnabled;

    private final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    private final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");
    private final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    public ReadItemsFn(Hints hints, ResourceType resourceType, ReaderConfig readerConfig) {
        this.resourceType = resourceType;
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();

        isMetricsEnabled = readerConfig.isMetricsEnabled();
    }

    @Setup
    public void setup() {
        LOG.info("Setting up ReadItemsFn.");
    }

    @ProcessElement
    public void processElement(@Element RequestParameters query,
                               OutputReceiver<String> outputReceiver) throws Exception {
        final String batchLogPrefix = "ReadItemsFn - batch: "
                + RandomStringUtils.randomAlphanumeric(6) + " - ";
        LOG.debug(batchLogPrefix + "Sending query to the Cognite data platform: {}", query.toString());

        try {
            ItemReader<String> itemReader;
            switch (resourceType) {
                case ASSETS_AGGREGATES:
                    itemReader = connector.readAssetsAggregates();
                    break;
                case EVENT_AGGREGATES:
                    itemReader = connector.readEventsAggregates();
                    break;
                case SEQUENCE_AGGREGATES:
                    itemReader = connector.readSequencesAggregates();
                    break;
                case FILE_AGGREGATES:
                    itemReader = connector.readFilesAggregates();
                    break;
                case TIMESERIES_AGGREGATES:
                    itemReader = connector.readTsAggregates();
                    break;
                default:
                    LOG.error(batchLogPrefix + "Not a supported resource type: " + resourceType);
                    throw new Exception(batchLogPrefix + "Not a supported resource type: " + resourceType);
            }
            ResponseItems<String> responseItems = itemReader.getItems(query);

            if (!responseItems.isSuccessful()) {
                // something went wrong with the request
                String message = batchLogPrefix + "Error while iterating through the results from Fusion: "
                        + responseItems.getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }

            if (isMetricsEnabled) {
                MetricsUtil.recordApiLatency(responseItems, apiLatency);
                MetricsUtil.recordApiBatchSize(responseItems, apiBatchSize);
                MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
            }

            for (String item : responseItems.getResultsItems()) {
                outputReceiver.output(item);
            }
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
