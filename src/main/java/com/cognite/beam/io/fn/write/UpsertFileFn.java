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

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.dto.FileContainer;
import com.cognite.client.dto.FileMetadata;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes files to CDF.Clean. If the file exists from before (based on {@code id}/{@code externalId}), it will
 * be overwritten.
 *
 */
public class UpsertFileFn extends IOBaseFn<Iterable<FileContainer>, KV<FileContainer, FileMetadata>> {
    private final static Logger LOG = LoggerFactory.getLogger(UpsertFileFn.class);

    final WriterConfig writerConfig;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public UpsertFileFn(Hints hints,
                        WriterConfig writerConfig,
                        PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkNotNull(writerConfig, "Writer config cannot be null.");
        Preconditions.checkNotNull(projectConfigView, "Project config view cannot be null");

        this.projectConfigView = projectConfigView;
        this.writerConfig = writerConfig;
     }

    /**
     * Converts input to file upload request for files metadata/header. If the file exists from before, it will
     * be overwritten.
     *
     * @param items
     * @param outputReceiver
     * @param context
     * @throws Exception
     */
    @ProcessElement
    public void processElement(@Element Iterable<FileContainer> items,
                               OutputReceiver<KV<FileContainer, FileMetadata>> outputReceiver,
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
        List<FileContainer> upsertItems = new ArrayList<>();
        items.forEach(item -> upsertItems.add(item));

        // Write the items
        try {
            List<FileMetadata> results = getClient(projectConfig, writerConfig).files().upload(upsertItems, false);

            if (writerConfig.isMetricsEnabled()) {
                apiBatchSize.update(results.size());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }
            LOG.info(batchLogPrefix + "Upserted {} items in {}}.",
                    results.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            // Join the input file container with the output file metadata object
            for (FileContainer container : upsertItems) {
                if (container.getFileMetadata().hasExternalId()) {
                    FileMetadata fileMetadata = results.stream()
                            .filter(metadata -> container.getFileMetadata()
                                    .getExternalId()
                                    .equals(metadata.getExternalId()))
                            .findAny()
                            .orElseThrow();

                    outputReceiver.output(KV.of(container, fileMetadata));
                } else if (container.getFileMetadata().hasId()) {
                    FileMetadata fileMetadata = results.stream()
                            .filter(metadata -> container.getFileMetadata().getId() == metadata.getId())
                            .findAny()
                            .orElseThrow();

                    outputReceiver.output(KV.of(container, fileMetadata));
                } else {
                    throw new Exception("Input file container does not contain externalId / id.");
                }
            }
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when writing to Cognite Data Fusion: {}",
                    e.toString());
            throw new Exception(batchLogPrefix + "Error when writing to Cognite Data Fusion.", e);
        }
    }
}
