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

package com.cognite.beam.io.fn.write;

import com.cognite.beam.io.config.ConfigBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.CogniteClient;
import com.cognite.client.config.ClientConfig;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for writing items to CDF.Clean. The items will be upserted.
 *
 * Specific resource types (Event, Asset, etc.) extend this class with
 * custom parsing logic.
 */
public abstract class UpsertItemBaseFn<T> extends IOBaseFn<Iterable<T>, T> {
    final WriterConfig writerConfig;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public UpsertItemBaseFn(Hints hints,
                            WriterConfig writerConfig,
                            PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(writerConfig, "Writer config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");

        this.projectConfigView = projectConfigView;
        this.writerConfig = writerConfig;
    }

    @Setup
    public void setup() {
    }

    @ProcessElement
    public void processElement(@Element Iterable<T> items,
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

        // Prep the input items
        List<T> upsertItems = new ArrayList<>();
        items.forEach(item -> upsertItems.add(item));

        // Write the items
        try {
            List<T> results = upsertItems(getClient(projectConfig, writerConfig), upsertItems);

            if (writerConfig.isMetricsEnabled()) {
                apiBatchSize.update(results.size());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }
            LOG.info(batchLogPrefix + "Upserted {} items in {}}.",
                    results.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            results.forEach(item -> outputReceiver.output(item));
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when writing to Cognite Data Fusion", e);
            throw new Exception(batchLogPrefix + "Error when writing to Cognite Data Fusion.", e);
        }
    }

    /**
     * Adds the upsert mode to the client's configuration.
     *
     * @param projectConfig The {@link ProjectConfig} to configure auth credentials.
     * @param configBase Carries the app and session identifiers.
     * @return The {@link CogniteClient} with upsert mode configured
     * @throws Exception
     */
    @Override
    protected CogniteClient getClient(ProjectConfig projectConfig, ConfigBase configBase) throws Exception {
        CogniteClient baseClient = super.getClient(projectConfig, configBase);

        return baseClient.withClientConfig(baseClient.getClientConfig()
                .withUpsertMode(writerConfig.getUpsertMode()));
    }

    /**
     * Writes the collection of items to Cognite Data Fusion.
     *
     * @param client The {@link CogniteClient} to use for writing the items.
     * @param inputItems The items to write / upsert.
     * @return The upserted items.
     */
    protected abstract List<T> upsertItems(CogniteClient client, List<T> inputItems) throws Exception;
}
