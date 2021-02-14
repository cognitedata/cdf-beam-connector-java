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
import com.cognite.client.dto.Item;
import com.cognite.client.dto.RawRow;
import com.cognite.client.dto.SequenceBody;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.client.servicesV1.parser.SequenceParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Deletes rows from {@code Sequences}.
 *
 * This function will delete all row numbers referenced in the {@link SequenceBody} input object.
 */
public class DeleteSequenceRowsFn extends IOBaseFn<Iterable<SequenceBody>, SequenceBody> {
    private final static Logger LOG = LoggerFactory.getLogger(DeleteSequenceRowsFn.class);

    private final WriterConfig writerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public DeleteSequenceRowsFn(Hints hints,
                                WriterConfig writerConfig,
                                PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(writerConfig, "Writer config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");

        this.writerConfig = writerConfig;
        this.projectConfigView = projectConfigView;
    }

    @ProcessElement
    public void processElement(@Element Iterable<SequenceBody> items,
                               OutputReceiver<SequenceBody> outputReceiver,
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
        List<SequenceBody> itemsList = new ArrayList<>();
        items.forEach(item -> itemsList.add(item));
        try {
            List<SequenceBody> resultsItems =
                    getClient(projectConfig, writerConfig).sequences().rows().delete(itemsList);

            if (writerConfig.isMetricsEnabled()) {
                apiBatchSize.update(resultsItems.size());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }
            LOG.info(batchLogPrefix + "Deleted {} sequence body items in {}}.",
                    resultsItems.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            resultsItems.forEach(item -> outputReceiver.output(item));
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when deleting sequence rows in Cognite Data Fusion: {}",
                    e.toString());
            throw new Exception(batchLogPrefix + "Error when deleting sequence rows in Cognite Data Fusion.", e);
        }
    }
}
