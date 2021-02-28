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

package com.cognite.beam.io.fn.delete;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.dto.RawRow;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Deletes rows from CDF.Raw.
 *
 * This function deletes rows from a raw table.
 *
 * The input collection of rows must all belong to the same db and table. That is, each {@code Iterable<RawRow>}
 * must contain rows for the same raw destination table.
 */
public class DeleteRawRowFn extends IOBaseFn<Iterable<RawRow>, RawRow> {
    private final static Logger LOG = LoggerFactory.getLogger(DeleteRawRowFn.class);

    private final WriterConfig writerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public DeleteRawRowFn(Hints hints,
                          WriterConfig writerConfig,
                          PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(writerConfig, "Writer config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");

        this.writerConfig = writerConfig;
        this.projectConfigView = projectConfigView;
    }

    @ProcessElement
    public void processElement(@Element Iterable<RawRow> items,
                               OutputReceiver<RawRow> outputReceiver,
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

        // Delete the items
        List<RawRow> itemsList = new ArrayList<>();
        items.forEach(item -> itemsList.add(item));
        try {
            List<RawRow> resultsItems = getClient(projectConfig, writerConfig).raw().rows().delete(itemsList);

            if (writerConfig.isMetricsEnabled()) {
                apiBatchSize.update(resultsItems.size());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }
            LOG.info(batchLogPrefix + "Deleted {} items in {}}.",
                    resultsItems.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            resultsItems.forEach(item -> outputReceiver.output(item));
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when deleting Raw rows in Cognite Data Fusion: {}",
                    e.toString());
            throw new Exception(batchLogPrefix + "Error when deleting Raw rows in Cognite Data Fusion.", e);
        }
    }
}
