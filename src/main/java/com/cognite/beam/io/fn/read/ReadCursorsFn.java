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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.servicesV1.ResponseItems;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.Hints;
import com.cognite.client.config.ResourceType;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Reads cursors for assets and events, and adds them to the request parameters.
 */
public class ReadCursorsFn extends IOBaseFn<RequestParameters, RequestParameters> {
    private final static Logger LOG = LoggerFactory.getLogger(ReadCursorsFn.class);
    private final int MAX_RESULTS_SET_SIZE_LIMIT = 20000;

    private final ReaderConfig readerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public ReadCursorsFn(Hints hints,
                         ReaderConfig readerConfig,
                         PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(readerConfig, "Reader config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");

        this.projectConfigView = projectConfigView;
        this.readerConfig = readerConfig;
    }

    @ProcessElement
    public void processElement(@Element RequestParameters requestParameters,
                               OutputReceiver<RequestParameters> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "Batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        final Instant batchStartInstant = Instant.now();

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchLogPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // if readShards = 1, then skip fetching cursors
        LOG.debug(batchLogPrefix + "Checking hints for readShards: {}", hints.getReadShards().get());

        if (hints.getReadShards().get() < 2) {
            LOG.debug(batchLogPrefix + "readShards < 2, skipping cursors");
            outputReceiver.output(requestParameters);
            return;
        }

        // the filter parameters cannot contain a cursor parameter
        LOG.debug(batchLogPrefix + "Checking if request parameters already contains a cursor.");
        checkArgument(!requestParameters.getRequestParameters().containsKey("cursor"),
                "Filter parameters cannot contain a cursor");

        Preconditions.checkArgument(requestParameters.getRequestParameters().containsKey("dbName")
                        && requestParameters.getRequestParameters().get("dbName") instanceof String,
                "Request parameters must include dnName with a string value");
        Preconditions.checkArgument(requestParameters.getRequestParameters().containsKey("tableName")
                        && requestParameters.getRequestParameters().get("tableName") instanceof String,
                "Request parameters must include tableName");

        String dbName = (String) requestParameters.getRequestParameters().get("dbName");
        String tableName = (String) requestParameters.getRequestParameters().get("tableName");

        try {
            int totalNoPartitions = hints.getReadShards().get();
            LOG.info(batchLogPrefix + "Will split into {} total partitions / cursors "
                            + "with {} (parallel) partitions per worker. Duration: {}",
                    totalNoPartitions,
                    hints.getReadShardsPerWorker(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            List<String> cursors = getClient(projectConfig, readerConfig).raw().rows()
                    .retrieveCursors(dbName, tableName, totalNoPartitions, requestParameters.getRequest());

            List<String> partitions = new ArrayList<>();
            for (String cursor : cursors) {
                // Build the partitions list
                partitions.add(cursor);
                if (partitions.size() >= hints.getReadShardsPerWorker()) {
                    outputReceiver.output(requestParameters.withPartitions(partitions));
                    partitions = new ArrayList<>();
                }
            }
            if (partitions.size() > 0) {
                outputReceiver.output(requestParameters.withPartitions(partitions));
            }

        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
