/*
 * Copyright (c) 2021 Cognite AS
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

package com.cognite.beam.io.fn.context;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.dto.FileBinary;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.PnIDResponse;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.PnIDResponseParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Struct;
import com.google.protobuf.util.Values;
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
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Detects annotations and builds interactive P&ID (svg and png) from single-page PDF.
 *
 * The function detects entities (for example, assets) in the P&ID and highlights them in the (optional)
 * SVG/PNG. The detected entities can be used to enrich the viewer experience in an application displaying
 * the P&ID.
 *
 * Annotations are detected based on a side input of {@link List<Struct>}. {@code Struct.name} is used
 * for annotation matching.
 *
 * The input specifies which file (id) to process.
 *
 */
public class CreateInteractivePnIDFn extends DoFn<Iterable<Item>, PnIDResponse> {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ConnectorServiceV1 connector;
    private final ReaderConfig readerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;
    private final PCollectionView<List<Struct>> matchToView;
    private final boolean convertFile;
    private final boolean partialMatch;
    private final int minTokens;

    private final Distribution annotationQueueDuration =
            Metrics.distribution("cognite", "annotationQueueDuration");
    private final Distribution annotationExecutionDuration =
            Metrics.distribution("cognite", "annotationExecutionDuration");
    private final Distribution convertQueueDuration =
            Metrics.distribution("cognite", "convertQueueDuration");
    private final Distribution convertExecutionDuration =
            Metrics.distribution("cognite", "convertExecutionDuration");

    public CreateInteractivePnIDFn(Hints hints,
                                   ReaderConfig readerConfig,
                                   PCollectionView<List<ProjectConfig>> projectConfigView,
                                   PCollectionView<List<Struct>> matchToView,
                                   boolean convertFile) {
        this(hints, readerConfig, projectConfigView, matchToView, convertFile,
                false, 2);
    }

    public CreateInteractivePnIDFn(Hints hints,
                                   ReaderConfig readerConfig,
                                   PCollectionView<List<ProjectConfig>> projectConfigView,
                                   PCollectionView<List<Struct>> matchToView,
                                   boolean convertFile,
                                   boolean partialMatch,
                                   int minTokens) {
        this.projectConfigView = projectConfigView;
        this.matchToView = matchToView;
        this.convertFile = convertFile;
        this.partialMatch = partialMatch;
        this.minTokens = minTokens;
        this.readerConfig = readerConfig;
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();
    }

    @Setup
    public void setup() {
        LOG.info("Setting up CreateInteractivePnIDFn.");
    }

    @ProcessElement
    public void processElement(@Element Iterable<Item> element,
                               OutputReceiver<PnIDResponse> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "CreateInteractivePnIDFn - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        LOG.info(batchLogPrefix + "Received a batch of files to process.");
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
        LOG.debug(batchLogPrefix + "Input items: {}",
                ImmutableList.copyOf(element).toString());

        List<PnIDResponse> results = new ArrayList<>();
        try {
            /*
            Start the detect annotation section.
             */
            Map<CompletableFuture<ResponseItems<String>>, Item> annotationFutureMap = new HashMap<>();
            int counter = 0;
            for (Item item : element) {
                Preconditions.checkState(item.getId() > 0,
                        batchLogPrefix + "The input item must contain an id.");
                annotationFutureMap.put(detectAnnotations(item, projectConfig, context, batchLogPrefix), item);
                counter++;
            }
            // just being too ... careful
            if (counter != annotationFutureMap.size()) {
                String message = batchLogPrefix + "Something went wrong when collecting annotation futures. "
                        + "The total count is off. Counter: " + counter + ", Map size: " + annotationFutureMap.size();
                LOG.error(message);
                throw new Exception(message);
            }
            LOG.info(batchLogPrefix + "Submitted {} files to the detect annotations service",
                    annotationFutureMap.size());

            // wait for all futures to complete and parse the results
            CompletableFuture<Void> allAnnotationsDoneFuture = CompletableFuture
                    .allOf(annotationFutureMap.keySet().toArray(new CompletableFuture[annotationFutureMap.size()]));
            allAnnotationsDoneFuture.join(); // wait for completion of all futures
            Map<ResponseItems<String>, Item> annotationsMap = new HashMap<>(annotationFutureMap.size());
            annotationFutureMap.entrySet().stream()
                    .forEach(entry -> annotationsMap.put(entry.getKey().join(), entry.getValue()));

            for (Map.Entry<ResponseItems<String>, Item> entry : annotationsMap.entrySet()) {
                    if (readerConfig.isMetricsEnabled()) {
                        MetricsUtil.recordApiJobQueueDuration(entry.getKey(), annotationQueueDuration);
                        MetricsUtil.recordApiJobExecutionDuration(entry.getKey(), annotationExecutionDuration);
                    }
                    PnIDResponse annotationsResponse =
                            PnIDResponseParser.ParsePnIDAnnotationResponse(entry.getKey().getResultsItems().get(0));
                    // must add the fileId via a lookup since it isn't a part of the response payload
                    annotationsResponse = annotationsResponse.toBuilder()
                            .setFileId(Int64Value.of(entry.getValue().getId()))
                            .build();

                    results.add(annotationsResponse);
            }
            LOG.info(batchLogPrefix + "Completed annotations for {} files within a duration of {}.",
                    results.size(),
                    Duration.between(startInstant, Instant.now()).toString());

            /*
            If the user has specified to include interactive Svg & Png files, we add them in the below section.
            First we run the conversion service and then download the resulting files.
             */
            if (convertFile) {
                List<PnIDResponse> annotations = ImmutableList.copyOf(results);
                results.clear(); // clear the old annotation results
                List<PnIDResponse> convertedFiles = new ArrayList<>(annotations.size());

                // run the files through the conversion service
                Map<CompletableFuture<ResponseItems<String>>, PnIDResponse> convertFutureMap =
                        new HashMap<>(annotations.size());
                for (PnIDResponse annotation : annotations) {
                    convertFutureMap.put(convertPnid(annotation, projectConfig, batchLogPrefix), annotation);
                }
                // wait for all futures to complete
                CompletableFuture<Void> allConversionsDoneFuture = CompletableFuture
                        .allOf(convertFutureMap.keySet().toArray(new CompletableFuture[convertFutureMap.size()]));
                allConversionsDoneFuture.join();
                Map<ResponseItems<String>, PnIDResponse> conversionsMap = new HashMap<>(convertFutureMap.size());
                convertFutureMap.entrySet().stream()
                        .forEach(entry -> conversionsMap.put(entry.getKey().join(), entry.getValue()));

                for (Map.Entry<ResponseItems<String>, PnIDResponse> entry : conversionsMap.entrySet()) {
                    if (readerConfig.isMetricsEnabled()) {
                        MetricsUtil.recordApiJobQueueDuration(entry.getKey(), convertQueueDuration);
                        MetricsUtil.recordApiJobExecutionDuration(entry.getKey(), convertExecutionDuration);
                    }
                    // merge the convert response into the annotation response
                    PnIDResponse convertResponse = entry.getValue().toBuilder()
                            .mergeFrom(PnIDResponseParser
                                    .ParsePnIDConvertResponse(entry.getKey().getResultsItems().get(0)))
                            .build();

                    convertedFiles.add(convertResponse);
                }
                LOG.info(batchLogPrefix + "Completed converting pdfs for {} files within a duration of {}.",
                        convertedFiles.size(),
                        Duration.between(startInstant, Instant.now()).toString());

                // download the converted binaries
                List<CompletableFuture<PnIDResponse>> downloadFutures = new ArrayList<>(convertedFiles.size());
                for (PnIDResponse convertedFile : convertedFiles) {
                    downloadFutures.add(downloadConvertedFiles(convertedFile, batchLogPrefix));
                }
                // wait for all futures to complete
                CompletableFuture<Void> allBinariesDoneFuture = CompletableFuture
                        .allOf(downloadFutures.toArray(new CompletableFuture[downloadFutures.size()]));
                allBinariesDoneFuture.join();
                downloadFutures.stream()
                        .forEach(future -> results.add(future.join()));
                LOG.info(batchLogPrefix + "Completed downloading interactive binaries for {} files within a duration of {}.",
                        results.size(),
                        Duration.between(startInstant, Instant.now()).toString());

            }
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
        for (PnIDResponse result : results) {
            outputReceiver.output(result);
        }
        LOG.info(batchLogPrefix + "Completed processing of {} files within a duration of {}.",
                results.size(),
                Duration.between(startInstant, Instant.now()).toString());
    }

    /**
     * Detect annotations on a file.
     *
     * Annotations are detected based on a side input of {@link List<Struct>}. {@code Struct.name} is used
     * for annotation matching.
     *
     * @param element
     * @param config
     * @param context
     * @param batchLogPrefix
     * @return
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> detectAnnotations(Item element,
                                                                       ProjectConfig config,
                                                                       ProcessContext context,
                                                                       String batchLogPrefix) throws Exception {
        List<Struct> matchToList = context.sideInput(matchToView);
        ImmutableList<String> matchToEntities = ImmutableList.copyOf(
                matchToList.stream()
                        .map(struct -> struct
                                .getFieldsOrDefault("name", Values.of(""))
                                .getStringValue())
                        .collect(Collectors.toList()));

        RequestParameters detectAnnotations = RequestParameters.create()
                .withProjectConfig(config)
                .withRootParameter("fileId", element.getId())
                .withRootParameter("entities", matchToEntities)
                .withRootParameter("partialMatch", partialMatch)
                .withRootParameter("minTokens", minTokens);

        return connector.detectAnnotationsPnid()
                .getItemsAsync(detectAnnotations)
                .thenApply(responseItems -> {
                    if (!responseItems.isSuccessful()) {
                        LOG.error(batchLogPrefix + "Api job did not complete successfully. Response body: {}",
                                responseItems.getResponseBodyAsString());
                        throw new CompletionException(
                                new Throwable("Api job did not complete successfully. Response body: "
                                        + responseItems.getResponseBodyAsString()));
                    }

                    return responseItems;
                });
    }

    private CompletableFuture<ResponseItems<String>> convertPnid(PnIDResponse annotations,
                                                                 ProjectConfig config,
                                                                 String batchLogPrefix) throws Exception {
        RequestParameters interactiveFilesRequest = RequestParameters.create()
                .withProjectConfig(config)
                .withRootParameter("fileId", annotations.getFileId())
                .withRootParameter("items", annotations.getItemsList());

        return connector.convertPnid()
                .getItemsAsync(interactiveFilesRequest)
                .thenApply(stringResponseItems -> {
                    if (!stringResponseItems.isSuccessful()) {
                        LOG.error(batchLogPrefix + "Api job did not complete successfully. Response body: {}",
                                stringResponseItems.getResponseBodyAsString());
                        throw new CompletionException(
                                new Throwable("Api job did not complete successfully. Response body: "
                                        + stringResponseItems.getResponseBodyAsString()));
                    }

                    return stringResponseItems;
                });
    }

    private CompletableFuture<PnIDResponse> downloadConvertedFiles(PnIDResponse converted,
                                                                   String batchLogPrefix) throws Exception {
        return CompletableFuture.<PnIDResponse>supplyAsync(() -> {
            // download the binaries
            CompletableFuture<FileBinary> svgResponse = null;
            CompletableFuture<FileBinary> pngResponse = null;
            if (converted.hasSvgUrl()) {
                svgResponse = ConnectorServiceV1.DownloadFileBinary
                        .downloadFileBinaryFromURL(converted.getSvgUrl().getValue());
            }
            if (converted.hasPngUrl()) {
                pngResponse = ConnectorServiceV1.DownloadFileBinary
                        .downloadFileBinaryFromURL(converted.getPngUrl().getValue());
            }

            // add the binaries to the response object
            PnIDResponse.Builder responseBuilder = converted.toBuilder();
            if (null != svgResponse) {
                LOG.debug(batchLogPrefix + "Found SVG. Adding to response object");
                responseBuilder.setSvgBinary(BytesValue.of(svgResponse.join().getBinary()));
            }
            if (null != pngResponse) {
                LOG.debug(batchLogPrefix + "Found PNG. Adding to response object");
                responseBuilder.setPngBinary(BytesValue.of(pngResponse.join().getBinary()));
            }
            return responseBuilder.build();
        });
    }
}
