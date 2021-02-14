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
import com.cognite.client.CogniteClient;
import com.cognite.client.dto.Item;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for retrieving items from CDF.
 *
 * Specific resource types (Event, Asset, etc.) extend this class with
 * custom parsing logic.
 */
public abstract class RetrieveItemsBaseFn<T> extends IOBaseFn<Iterable<Item>, T> {
    final ReaderConfig readerConfig;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public RetrieveItemsBaseFn(Hints hints,
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
    public void processElement(@Element Iterable<Item> items,
                               OutputReceiver<T> outputReceiver,
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
        List<Item> itemsList = new ArrayList<>();
        items.forEach(item -> itemsList.add(item));
        try {
            List<T> resultsItems = retrieveItems(getClient(projectConfig, readerConfig), itemsList);
            if (readerConfig.isMetricsEnabled()) {
                apiBatchSize.update(resultsItems.size());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }
            LOG.info(batchLogPrefix + "Retrieved {} items in {}}.",
                    resultsItems.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            resultsItems.forEach(item -> outputReceiver.output(item));
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when reading from Cognite Data Fusion: {}",
                    e.toString());
            throw new Exception(batchLogPrefix + "Error when reading from Cognite Data Fusion.", e);
        }
    }

    /**
     * Retrieve items from Cognite Data Fusion.
     *
     * @param client The {@link CogniteClient} to use for writing the items.
     * @param items The items to retrieve.
     * @return The retrieved items.
     * @throws Exception
     */
    protected abstract List<T> retrieveItems(CogniteClient client,
                                            List<Item> items) throws Exception;
}
