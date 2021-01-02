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
import java.util.concurrent.CompletableFuture;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.servicesV1.ItemReader;
import com.cognite.beam.io.servicesV1.ResponseItems;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.fn.ResourceType;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Reads cursors for assets and events, and adds them to the request parameters.
 */
public class ReadCursorsFn extends DoFn<RequestParameters, RequestParameters> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final int MAX_RESULTS_SET_SIZE_LIMIT = 20000;

    private final Hints hints;
    private final ResourceType resourceType;
    private final ConnectorServiceV1 connector;

    public ReadCursorsFn(Hints hints, ResourceType resourceType, ReaderConfig readerConfig) {
        this.hints = hints;
        this.resourceType = resourceType;
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();
    }

    @Setup
    public void setup() {
        LOG.info("Setting up ReadCursorsFn.");
        LOG.debug("Validating the hints");
        hints.validate();
    }

    @ProcessElement
    public void processElement(@Element RequestParameters query,
                               OutputReceiver<RequestParameters> outputReceiver) throws Exception {
        final String batchIdentifierPrefix = "Batch id: " + RandomStringUtils.randomAlphanumeric(6) + " - ";

        // if readShards = 1, then skip fetching cursors
        LOG.debug(batchIdentifierPrefix + "Checking hints for readShards: {}", hints.getReadShards().get());

        if (hints.getReadShards().get() < 2) {
            LOG.debug(batchIdentifierPrefix + "readShards < 2, skipping cursors");
            outputReceiver.output(query);
            return;
        }

        LOG.debug("Received query to process {}", query.toString());

        // the filter parameters cannot contain a cursor parameter
        LOG.debug(batchIdentifierPrefix + "Checking if request parameters already contains a cursor.");
        checkArgument(!query.getRequestParameters().containsKey("cursor"),
                "Filter parameters cannot contain a cursor");

        // set a reasonably low prefetch limit.
        int prefetchResultsSetSizeLimit = Math.min(MAX_RESULTS_SET_SIZE_LIMIT, (int) Math.round(Math.pow(hints.getReadShards().get(), 2)));
        LOG.debug(batchIdentifierPrefix + "cursor prefetch results set size target set to {}", prefetchResultsSetSizeLimit);

        // based on the prefetch limit, adjust the results set batch size so that we don't ask for more results than we need.
        int prefetchResultsSetPageLimit = Math.min(prefetchResultsSetSizeLimit + 1, 1000);
        if (query.getRequestParameters().containsKey("limit")
                && query.getRequestParameters().get("limit") instanceof Integer) {
            prefetchResultsSetPageLimit = Math.min(prefetchResultsSetSizeLimit, (Integer) query.getRequestParameters().get("limit"));
        }
        LOG.debug(batchIdentifierPrefix + "cursor prefetch request batch/page limit set to {}", prefetchResultsSetPageLimit);
        RequestParameters cursorQuery = query.withRootParameter("limit", prefetchResultsSetPageLimit);

        LOG.debug(batchIdentifierPrefix + "Sending query to the Cognite data platform: {}", cursorQuery.toString());
        // Check that the results set is large enough for it to warrant the use of cursors.
        Iterator<CompletableFuture<ResponseItems<String>>> results;
        switch (resourceType) {
            case RAW_ROW:
                results = connector.readRawRows(cursorQuery);
                break;
            default:
                LOG.error(batchIdentifierPrefix + "Not a supported resource type: " + resourceType);
                throw new Exception(batchIdentifierPrefix + "Not a supported resource type: " + resourceType);
        }

        try {
            LOG.info(batchIdentifierPrefix + "Probing the results set size of {}.", resourceType);
            int resultsSetCounter = 0;
            ResponseItems<String> responseItems;
            while (results.hasNext() && resultsSetCounter <= prefetchResultsSetSizeLimit) {
                responseItems = results.next().join();
                if (!responseItems.isSuccessful()) {
                    throw new Exception(batchIdentifierPrefix + "An error occurred while prefetching rows from Raw.");
                }
                resultsSetCounter += responseItems.getResultsItems().size();
            }
            LOG.debug(batchIdentifierPrefix + "Results set size: {}.", resultsSetCounter);

            if (resultsSetCounter >= prefetchResultsSetSizeLimit && hints.getReadShards().get() > 1) {
                LOG.info(batchIdentifierPrefix + "Fetching cursors for {}.", resourceType);
                ResponseItems<String> cursors;
                switch (resourceType) {
                    case RAW_ROW:
                        cursors = connector.readCursorsRawRows().getItems(query
                                .withRootParameter("numberOfCursors", hints.getReadShards().get()));
                        break;
                    default:
                        LOG.error(batchIdentifierPrefix + "Not a supported resource type: " + resourceType);
                        throw new Exception(batchIdentifierPrefix + "Not a supported resource type: " + resourceType);
                }
                if (!cursors.isSuccessful()) {
                    throw new Exception(batchIdentifierPrefix + "An error occurred while reading cursors.");
                }
                for (String item : cursors.getResultsItems()) {
                    outputReceiver.output(query.withRootParameter("cursor", item));
                }
            } else {
                LOG.info(batchIdentifierPrefix + "Skipping cursors--results set size too small or hints specify split = 1.");
                outputReceiver.output(query);
            }

        } catch (Exception e) {
            LOG.error(batchIdentifierPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
