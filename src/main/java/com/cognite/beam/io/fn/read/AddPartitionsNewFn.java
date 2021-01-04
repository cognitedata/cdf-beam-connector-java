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
import com.cognite.beam.io.fn.ResourceType;
import com.cognite.client.CogniteClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

/**
 * Splits a list / read items request into multiple partitioned requests.
 *
 * This function will first query Cognite Data Fusion for a total count of items via
 * the aggregates endpoints. Based on the total count, it will partition the request.
 */
public abstract class AddPartitionsNewFn extends IOBaseFn<RequestParameters, RequestParameters> {
    private final List<ResourceType> supportedResourceTypes = ImmutableList.of(ResourceType.ASSET, ResourceType.EVENT,
            ResourceType.TIMESERIES_HEADER);

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
        Preconditions.checkArgument(supportedResourceTypes.contains(resourceType),
                "Resource type is not supported: " + resourceType);

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

        // Read the items
        /*
        try {
            Iterator<List<T>> resultsIterator = listItems(getClient(projectConfig, readerConfig),
                    requestParameters,
                    requestParameters.getPartitions().toArray(new String[requestParameters.getPartitions().size()]));
            Instant pageStartInstant = Instant.now();
            int totalNoItems = 0;
            while (resultsIterator.hasNext()) {
                List<T> results = resultsIterator.next();
                if (readerConfig.isMetricsEnabled()) {
                    apiBatchSize.update(results.size());
                    apiLatency.update(Duration.between(pageStartInstant, Instant.now()).toMillis());
                }
                results.forEach(item -> outputReceiver.output(item));
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

         */
    }
}
