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

import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.dto.Item;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.client.config.ResourceType;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class DeleteItemsFn extends IOBaseFn<Iterable<Item>, Item> {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteItemsFn.class);

    private static final ImmutableList<ResourceType> validResourceType = ImmutableList.of(
            ResourceType.ASSET, ResourceType.EVENT, ResourceType.TIMESERIES_HEADER, ResourceType.FILE,
            ResourceType.FILE_HEADER, ResourceType.RELATIONSHIP, ResourceType.LABEL, ResourceType.SEQUENCE_HEADER);

    private final WriterConfig writerConfig;
    private final ResourceType resourceType;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public DeleteItemsFn(Hints hints,
                         WriterConfig writerConfig,
                         ResourceType resourceType,
                         PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(writerConfig, "Writer config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");
        Preconditions.checkArgument(validResourceType.contains(resourceType), "Not a valid resource type");

        this.resourceType = resourceType;
        this.writerConfig = writerConfig;
        this.projectConfigView = projectConfigView;
    }

    @ProcessElement
    public void processElement(@Element Iterable<Item> items,
                               OutputReceiver<Item> outputReceiver,
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
        List<Item> itemsList = new ArrayList<>();
        items.forEach(item -> itemsList.add(item));
        try {
            List<Item> resultsItems;
            switch (resourceType) {
                case ASSET:
                    resultsItems = getClient(projectConfig, writerConfig).assets().delete(itemsList);
                    break;
                case EVENT:
                    resultsItems = getClient(projectConfig, writerConfig).events().delete(itemsList);
                    break;
                case TIMESERIES_HEADER:
                    resultsItems = getClient(projectConfig, writerConfig).timeseries().delete(itemsList);
                    break;
                case FILE_HEADER:
                case FILE:
                    resultsItems = getClient(projectConfig, writerConfig).files().delete(itemsList);
                    break;
                case RELATIONSHIP:
                    resultsItems = getClient(projectConfig, writerConfig).relationships().delete(itemsList);
                    break;
                case LABEL:
                    resultsItems = getClient(projectConfig, writerConfig).labels().delete(itemsList);
                    break;
                case SEQUENCE_HEADER:
                    resultsItems = getClient(projectConfig, writerConfig).sequences().delete(itemsList);
                    break;
                default:
                    LOG.error("Not a supported resource type: " + resourceType);
                    throw new RuntimeException("Not a supported resource type: " + resourceType);
            }

            if (writerConfig.isMetricsEnabled()) {
                apiBatchSize.update(resultsItems.size());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }
            LOG.info(batchLogPrefix + "Deleted {} items in {}}.",
                    resultsItems.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            resultsItems.forEach(item -> outputReceiver.output(item));
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when deleting items in Cognite Data Fusion: {}",
                    e.toString());
            throw new Exception(batchLogPrefix + "Error when deleting items in Cognite Data Fusion.", e);
        }
    }
}
