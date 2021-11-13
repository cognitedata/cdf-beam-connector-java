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
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.dto.TimeseriesPoint;
import com.cognite.beam.io.RequestParameters;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;


/**
 * This function reads time series data points based on the input RequestParameters.
 *
 */
public class ReadTsPointProto extends IOBaseFn<RequestParameters, List<TimeseriesPoint>> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    final ReaderConfig readerConfig;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public ReadTsPointProto(Hints hints,
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
                               OutputReceiver<List<TimeseriesPoint>> outputReceiver,
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

        // Read the items
        try {
            Iterator<List<TimeseriesPoint>> resultsIterator = getClient(projectConfig, readerConfig)
                    .timeseries()
                    .dataPoints()
                    .retrieve(requestParameters.getRequest());
            Instant pageStartInstant = Instant.now();
            int totalNoItems = 0;
            while (resultsIterator.hasNext()) {
                List<TimeseriesPoint> results = resultsIterator.next();
                if (readerConfig.isMetricsEnabled()) {
                    apiBatchSize.update(results.size());
                    apiLatency.update(Duration.between(pageStartInstant, Instant.now()).toMillis());
                }
                if (readerConfig.isStreamingEnabled()) {
                    // output with timestamps in streaming mode--need that for windowing
                    long minTimestampMs = results.stream()
                            .mapToLong(point -> point.getTimestamp())
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

    /*
    protected Iterator<List<TimeseriesPoint>> listItems(CogniteClient client,
                                                     RequestParameters requestParameters,
                                                     String... partitions) throws Exception {
        Preconditions.checkArgument(partitions.length == 0,
                "Partitions is not supported for data points");
        return client.timeseries().dataPoints().retrieve(requestParameters.getRequest());
    }

     */
}
