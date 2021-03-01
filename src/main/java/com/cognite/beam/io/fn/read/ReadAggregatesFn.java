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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Reads generic aggregates from Cognite resources like assets, events, files. etc.
 *
 * Specialized, per resource aggregates (for example, time series data points aggregates) use
 * the dedicated resource reader / iterator.
 */
public class ReadAggregatesFn extends IOBaseFn<RequestParameters, Aggregate> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ReaderConfig readerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;
    private final ResourceType resourceType;

    public ReadAggregatesFn(Hints hints,
                            ReaderConfig readerConfig,
                            PCollectionView<List<ProjectConfig>> projectConfigView,
                            ResourceType resourceType) {
        super(hints);
        this.readerConfig = readerConfig;
        this.projectConfigView = projectConfigView;
        this.resourceType = resourceType;
    }

    @ProcessElement
    public void processElement(@Element RequestParameters query,
                               OutputReceiver<Aggregate> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "ReadAggregatesFn - batch: "
                + RandomStringUtils.randomAlphanumeric(6) + " - ";
        final Instant batchStartInstant = Instant.now();
        LOG.debug(batchLogPrefix + "Sending query to the Cognite data platform: {}", query.toString());

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
            Aggregate aggregateResult;
            switch (resourceType) {
                case ASSET:
                    aggregateResult =
                            getClient(projectConfig, readerConfig).assets().aggregate(query.getRequest());
                    break;
                case EVENT:
                    aggregateResult =
                            getClient(projectConfig, readerConfig).events().aggregate(query.getRequest());
                    break;
                case TIMESERIES_HEADER:
                    aggregateResult =
                            getClient(projectConfig, readerConfig).timeseries().aggregate(query.getRequest());
                    break;
                case FILE_HEADER:
                    aggregateResult =
                            getClient(projectConfig, readerConfig).files().aggregate(query.getRequest());
                    break;
                case SEQUENCE_HEADER:
                    aggregateResult =
                            getClient(projectConfig, readerConfig).sequences().aggregate(query.getRequest());
                    break;
                default:
                    LOG.error(batchLogPrefix + "Not a supported resource type: " + resourceType);
                    throw new Exception(batchLogPrefix + "Not a supported resource type: " + resourceType);
            }

            if (readerConfig.isMetricsEnabled()) {
                apiBatchSize.update(aggregateResult.getAggregatesCount());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }

            LOG.info(batchLogPrefix + "Retrieved {} aggregates in {}}.",
                    aggregateResult.getAggregatesCount(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            outputReceiver.output(aggregateResult);

        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
