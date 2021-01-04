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
import com.cognite.beam.io.dto.FileContainer;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Writes files to CDF.Clean. If the file exists from before (based on {@code id}/{@code externalId}), it will
 * be overwritten.
 *
 */
public class UpsertFileFn extends DoFn<Iterable<FileContainer>, String> {
    private final static String globalLoggingPrefix = "UpsertFileFn - ";
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    final ConnectorServiceV1 connector;
    ConnectorServiceV1.FileWriter fileWriter;
    final WriterConfig writerConfig;
    final boolean deleteTempFile;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public UpsertFileFn(Hints hints,
                        WriterConfig writerConfig,
                        boolean deleteTempFile,
                        PCollectionView<List<ProjectConfig>> projectConfig) {
        Preconditions.checkNotNull(writerConfig, "Writer config cannot be null.");
        Preconditions.checkNotNull(hints, "Hints cannot be null");

        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(writerConfig.getAppIdentifier())
                .setSessionIdentifier(writerConfig.getSessionIdentifier())
                .build();
        this.projectConfigView = projectConfig;
        this.writerConfig = writerConfig;
        this.deleteTempFile = deleteTempFile;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up UpsertFileFn.");
        if (null == fileWriter) {
            fileWriter = connector.writeFileProto()
                    .enableDeleteTempFile(deleteTempFile);
        }
    }

    /**
     * Converts input to file upload request for files metadata/header. If the file exists from before, it will
     * be overwritten.
     *
     * @param element
     * @param outputReceiver
     * @param context
     * @throws Exception
     */
    @ProcessElement
    public void processElement(@Element Iterable<FileContainer> element, OutputReceiver<String> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = globalLoggingPrefix + "Batch: "
                + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Instant startInstant = Instant.now();

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchLogPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // naive de-duplication based on ids
        Map<Long, FileContainer> internalIdInsertMap = new HashMap<>(10);
        Map<String, FileContainer> externalIdInsertMap = new HashMap<>(10);

        try {
            populateMaps(element, internalIdInsertMap, externalIdInsertMap);
        } catch (Exception e) {
            LOG.error(batchLogPrefix + e.getMessage());
            throw e;
        }

        LOG.info(batchLogPrefix + "Received files to write: {}", internalIdInsertMap.size() + externalIdInsertMap.size());
        LOG.debug(batchLogPrefix + "Input file ids: [{}], file external ids: [{}]",
                ImmutableList.copyOf(internalIdInsertMap.keySet()).toString(),
                ImmutableList.copyOf(externalIdInsertMap.keySet()).toString());

        // Combine into list
        List<FileContainer> fileContainerList = new ArrayList<>(10);
        fileContainerList.addAll(externalIdInsertMap.values());
        fileContainerList.addAll(internalIdInsertMap.values());

        // Should not happen--but need to guard against empty input
        if (fileContainerList.isEmpty()) {
            LOG.warn(batchLogPrefix + "No input files received.");
            return;
        }

        // Results set container
        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>(10);

        // Write files async
        for (FileContainer file : fileContainerList) {
            CompletableFuture<ResponseItems<String>> future = fileWriter.writeFileAsync(
                    RequestParameters.create()
                            .withProjectConfig(projectConfig)
                            .withProtoRequestBody(file)
            );
            resultFutures.add(future);
        }
        LOG.info(batchLogPrefix + "Dispatched {} files for upload.", fileContainerList.size());

        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(resultFutures.toArray(
                new CompletableFuture[resultFutures.size()]));
        // Wait until the uber future completes.
        allFutures.join();

        for (CompletableFuture<ResponseItems<String>> future : resultFutures) {
            ResponseItems<String> responseItems = future.join();
            if (responseItems.isSuccessful()) {
                if (writerConfig.isMetricsEnabled()) {
                    MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                    MetricsUtil.recordApiLatency(responseItems, apiLatency);
                }
                for (String resultItem : responseItems.getResultsItems()) {
                    outputReceiver.output(resultItem);
                }
            } else {
                LOG.error(batchLogPrefix + "Failed to upload file: {}", responseItems.getResponseBodyAsString());
                throw new Exception(batchLogPrefix + "Failed to upload file: " + responseItems.getResponseBodyAsString());
            }
        }
        LOG.info(batchLogPrefix + "Completed upload of {} files within a duration of {}.",
                fileContainerList.size(),
                Duration.between(startInstant, Instant.now()).toString());
    }

    void populateMaps(Iterable<FileContainer> element, Map<Long, FileContainer> internalIdUpdateMap,
                            Map<String, FileContainer> externalIdUpdateMap) throws Exception {
        for (FileContainer value : element) {
            if (value.getFileMetadata().hasExternalId()) {
                externalIdUpdateMap.put(value.getFileMetadata().getExternalId().getValue(), value);
            } else if (value.getFileMetadata().hasId()) {
                internalIdUpdateMap.put(value.getFileMetadata().getId().getValue(), value);
            } else {
                throw new Exception("File item does not contain id nor externalId: " + value.getFileMetadata().toString());
            }
        }
    }
}
