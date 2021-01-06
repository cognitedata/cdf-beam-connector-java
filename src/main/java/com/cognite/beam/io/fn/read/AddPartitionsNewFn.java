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

import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Aggregate;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits a list / read items request into multiple partitioned requests.
 *
 * This function will first query Cognite Data Fusion for a total count of items via
 * the aggregates endpoints. Based on the total count, it will partition the request.
 */
public class AddPartitionsNewFn extends IOBaseFn<RequestParameters, RequestParameters> {
    private static final long ITEMS_PER_PARTITION = 10000;
    private static final long MAX_PARTITIONS = 100;

    private final ReaderConfig readerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;
    private final ResourceType resourceType;

    public AddPartitionsNewFn(Hints hints,
                              ReaderConfig readerConfig,
                              ResourceType resourceType,
                              PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(readerConfig, "Reader config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");
        Preconditions.checkNotNull(resourceType, "Resource type cannot be null");

        this.projectConfigView = projectConfigView;
        this.readerConfig = readerConfig;
        this.resourceType = resourceType;
    }

    @Setup
    public void setup() {
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

        // Count the expected number of results.
        try {
            Aggregate aggregateResult;
            switch (resourceType) {
                case ASSET:
                    aggregateResult = getClient(projectConfig, readerConfig).events().aggregate(requestParameters);
                    break;
                case EVENT:
                    aggregateResult = getClient(projectConfig, readerConfig).events().aggregate(requestParameters);
                    break;
                case TIMESERIES_HEADER:
                    aggregateResult = getClient(projectConfig, readerConfig).events().aggregate(requestParameters);
                    break;
                default:
                    LOG.error(batchLogPrefix + "Not a supported resource type: " + resourceType);
                    throw new Exception(batchLogPrefix + "Not a supported resource type: " + resourceType);
            }

            // Check the aggregate result
            if (aggregateResult.getAggregatesCount() != 1
                    || aggregateResult.getAggregates(0).hasValue()) {
                String message = String.format(batchLogPrefix + "Could not find item count in aggregate: %s",
                        aggregateResult.toString());
                LOG.error(message);
                throw new Exception(message);
            }

            long itemCount = aggregateResult.getAggregates(0).getCount();
            long totalNoPartitions = Math.min(
                    Math.max(Math.floorDiv(itemCount, ITEMS_PER_PARTITION), 1), // Must have min 1 partition
                    MAX_PARTITIONS);


            LOG.info(batchLogPrefix + "Counted {} items in total. Will split into {} total partitions "
                    + "with {} (parallel) partitions per worker. Duration: {}",
                    itemCount,
                    totalNoPartitions,
                    hints.getReadShardsPerWorker(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            List<String> partitions = new ArrayList<>(hints.getReadShardsPerWorker());
            for (int i = 1; i <= totalNoPartitions; i++) {
                // Build the partitions list in the format "m/n" where m = partition no and n = total no partitions
                partitions.add(String.format("%1$d/%2$d", i, totalNoPartitions));
                if (partitions.size() >= hints.getReadShardsPerWorker()) {
                    outputReceiver.output(requestParameters.withPartitions(partitions));
                    partitions = new ArrayList<>(hints.getReadShardsPerWorker());
                }
            }
            if (partitions.size() > 0) {
                outputReceiver.output(requestParameters.withPartitions(partitions));
            }

        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when reading from Cognite Data Fusion: {}",
                    e.toString());
            throw new Exception(batchLogPrefix + "Error when reading from Cognite Data Fusion.", e);
        }

    }
}
