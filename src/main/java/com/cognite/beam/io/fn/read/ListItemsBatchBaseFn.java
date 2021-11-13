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
import com.cognite.client.CogniteClient;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

/**
 * Base class for listing items from CDF and outputting them as batches.
 *
 * Specific resource types (Event, Asset, etc.) extend this class with
 * custom parsing logic.
 */
public abstract class ListItemsBatchBaseFn<T> extends IOBaseFn<RequestParameters, Iterable<T>> {
    long maxNumResults = Long.MAX_VALUE;
    final ReaderConfig readerConfig;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public ListItemsBatchBaseFn(Hints hints,
                                ReaderConfig readerConfig,
                                PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(readerConfig, "Reader config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");

        this.projectConfigView = projectConfigView;
        this.readerConfig = readerConfig;
    }

    @Setup
    public void setup() {
    }

    @ProcessElement
    public void processElement(@Element RequestParameters requestParameters,
                               OutputReceiver<Iterable<T>> outputReceiver,
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

        // If firstN is enabled, then set a max num of results to fetch
        LOG.debug(batchLogPrefix + "Checking config for firstN count: {}", readerConfig.getFirstNCount());
        if (readerConfig.getFirstNCount() > 0) {
            LOG.info(batchLogPrefix + "FirstN count specified at {}. Will limit the number of results objects.",
                    readerConfig.getFirstNCount());
            maxNumResults = readerConfig.getFirstNCount();
        }

        // Read the items
        try {
            Iterator<List<T>> resultsIterator = listItems(getClient(projectConfig, readerConfig),
                    requestParameters,
                    requestParameters.getPartitions().toArray(new String[requestParameters.getPartitions().size()]));
            Instant pageStartInstant = Instant.now();
            long totalNoItems = 0;
            while (resultsIterator.hasNext() && totalNoItems < maxNumResults) {
                List<T> results = resultsIterator.next();
                if (readerConfig.isMetricsEnabled()) {
                    apiBatchSize.update(results.size());
                    apiLatency.update(Duration.between(pageStartInstant, Instant.now()).toMillis());
                }
                if (readerConfig.isStreamingEnabled()) {
                    // output with timestamps in streaming mode--need that for windowing
                    long minTimestampMs = results.stream()
                            .mapToLong(item -> getTimestamp(item))
                            .min()
                            .orElse(1L);

                     outputReceiver.outputWithTimestamp(results, org.joda.time.Instant.ofEpochMilli(minTimestampMs));
                } else {
                    // no timestamping in batch mode--just leads to lots of complications
                    outputReceiver.output(results);
                }

                totalNoItems += results.size();
                pageStartInstant = Instant.now();
            }

            LOG.info(batchLogPrefix + "Retrieved {} items in {}}.",
                    totalNoItems,
                    Duration.between(batchStartInstant, Instant.now()).toString());
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when reading from Cognite Data Fusion: {}",
                    e.toString());
            throw new Exception(batchLogPrefix + "Error when reading from Cognite Data Fusion.", e);
        }
    }

    /**
     * Reads / lists items from Cognite Data Fusion.
     *
     * @param client The {@link CogniteClient} to use for writing the items.
     * @param requestParameters The request with optional filter parameters.
     * @param partitions The partitions to read.
     * @return An {@link Iterator} for paging through the results.
     * @throws Exception
     */
    protected abstract Iterator<List<T>> listItems(CogniteClient client,
                                                   RequestParameters requestParameters,
                                                   String... partitions) throws Exception;

    /**
     * Gets the timestamp associated with a data item. This timestamp will be used in streaming mode as the
     * basis for time windows. It would normally be the object's create or updated timestamp.
     *
     * @param item The item to get the timestamp for.
     * @return The timestamp.
     */
    protected abstract long getTimestamp(T item);
}
