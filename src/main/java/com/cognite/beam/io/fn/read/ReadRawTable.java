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

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.dto.RawTable;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This function reads all table names from a given raw database.
 *
 *
 */
public class ReadRawTable extends IOBaseFn<String, RawTable> {
    private static final Logger LOG = LoggerFactory.getLogger(ReadRawTable.class);

    private final ReaderConfig readerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public ReadRawTable(Hints hints,
                        ReaderConfig readerConfig,
                        PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(readerConfig, "Reader config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");
        this.readerConfig = readerConfig;
        this.projectConfigView = projectConfigView;
    }

    @ProcessElement
    public void processElement(@Element String dbName,
                               OutputReceiver<RawTable> outputReceiver,
                               ProcessContext context) throws Exception {
        Preconditions.checkNotNull(dbName, "Database name cannot be null.");
        Preconditions.checkArgument(!dbName.isEmpty(), "Database name cannot be empty.");
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

        try {
            Iterator<List<String>> resultsIterator = getClient(projectConfig, readerConfig).raw().tables().list(dbName);
            Instant pageStartInstant = Instant.now();
            int totalNoItems = 0;

            while (resultsIterator.hasNext()) {
                List<String> results = resultsIterator.next();
                if (readerConfig.isMetricsEnabled()) {
                    apiBatchSize.update(results.size());
                    apiLatency.update(Duration.between(pageStartInstant, Instant.now()).toMillis());
                }
                results.forEach(item -> outputReceiver.output(
                        RawTable.newBuilder()
                                .setDbName(dbName)
                                .setTableName(item)
                                .build()));

                totalNoItems += results.size();
                pageStartInstant = Instant.now();
            }

            LOG.info(batchLogPrefix + "Retrieved {} items in {}}.",
                    totalNoItems,
                    Duration.between(batchStartInstant, Instant.now()).toString());
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
