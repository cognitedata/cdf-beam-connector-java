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
import com.cognite.beam.io.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ItemParser;
import com.cognite.client.servicesV1.parser.SequenceParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.StringValue;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.client.servicesV1.ConnectorConstants.*;

/**
 * Writes sequences body (rows and columns) to CDF.Clean.
 *
 * This function writes sequences rows to Cognite. In case the rows exists from before (based on the externalId/id
 * and row number), the existing value will be overwritten.
 *
 * In case the sequence metadata / header does not exists, this module will create a minimum header.
 * The created header depends on the columns information included in the sequences body object.
 *
 * In any case, we recommend that you create the sequence header before starting to write sequence rows to it.
 */
public class UpsertSeqBodyFn extends DoFn<Iterable<SequenceBody>, SequenceBody> {
    // This transform uses constants from ConnectorConstants to control request batch sizes

    private static final SequenceMetadata DEFAULT_SEQ_METADATA = SequenceMetadata.newBuilder()
            .setExternalId(StringValue.of("beam_writer_default"))
            .setName(StringValue.of("beam_writer_default"))
            .setDescription(StringValue.of("Default Sequence metadata created by the Beam writer."))
            .build();

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    final Distribution seqRowsBatch = Metrics.distribution("cognite", "sequenceRowsPerBatch");
    final Distribution seqBatch = Metrics.distribution("cognite", "sequencesPerBatch");
    final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");
    final Counter metadataRetryCounter = Metrics.counter("cognite", "metadataRetries");

    private final ConnectorServiceV1 connector;
    private final WriterConfig writerConfig;
    private ConnectorServiceV1.ItemWriter seqBodyWriterInsert;
    private ConnectorServiceV1.ItemWriter seqHeaderWriterInsert;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    private final boolean isMetricsEnabled;

    public UpsertSeqBodyFn(Hints hints,
                           WriterConfig writerConfig,
                           PCollectionView<List<ProjectConfig>> projectConfigView) {
        Preconditions.checkNotNull(writerConfig, "WriterConfig cannot be null.");
        Preconditions.checkNotNull(hints, "Hints cannot be null");

        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(writerConfig.getAppIdentifier())
                .setSessionIdentifier(writerConfig.getSessionIdentifier())
                .build();
        this.writerConfig = writerConfig;
        this.projectConfigView = projectConfigView;

        isMetricsEnabled = writerConfig.isMetricsEnabled();
    }

    @Setup
    public void setup() {
        LOG.info("Setting up UpsertSeqBodyFn.");
        seqBodyWriterInsert = connector.writeSequencesRows();
        seqHeaderWriterInsert = connector.writeSequencesHeaders();
    }

    @ProcessElement
    public void processElement(@Element Iterable<SequenceBody> batch,
                               OutputReceiver<SequenceBody> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "Batch identifier: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        final Instant startInstant = Instant.now();

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchLogPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // Check all elements for id / externalId
        List<SequenceBody> itemList = new ArrayList<>(100);
        int seqRowCounter = 0;
        int seqCellCounter = 0;

        for (SequenceBody value : batch) {
            if (value.hasExternalId() || value.hasId()) {
                seqRowCounter += value.getRowsCount();
                seqCellCounter += value.getColumnsCount() * value.getRowsCount();
                itemList.add(value);
            } else {
                String message = batchLogPrefix + "Sequence does not contain id nor externalId: " + value.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }

        LOG.info(batchLogPrefix + "Received {} sequence ids with {} cells across {} rows to write.",
                itemList.size(),
                seqCellCounter,
                seqRowCounter);

        // Should not happen--but need to guard against empty input
        if (itemList.isEmpty()) {
            LOG.warn(batchLogPrefix + "No input elements received.");
            return;
        }

        /*
        Start the upsert:
        1. Write all sequences to the Cognite API.
        2. If one (or more) of the sequences fail, it is most likely because of missing headers. Add temp headers.
        3. Retry the failed sequences
         */
        Map<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> responseMap =
                splitAndUpsertSeqBody(itemList, projectConfig, batchLogPrefix);
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(future -> futureList.add(future));
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // wait for all requests to finish

        // Check for unsuccessful request
        List<Item> missingItems = new ArrayList<>();
        List<SequenceBody> retrySequenceBodyList = new ArrayList<>(itemList.size());
        List<CompletableFuture<ResponseItems<String>>> successfulFutures = new ArrayList<>(itemList.size());
        boolean requestsAreSuccessful = true;
        for (CompletableFuture<ResponseItems<String>> future : futureList) {
            ResponseItems<String> responseItems = future.join();
            requestsAreSuccessful = requestsAreSuccessful && responseItems.isSuccessful();
            if (!responseItems.isSuccessful()) {
                // Check for duplicates. Duplicates should not happen, so fire off an exception.
                if (!responseItems.getDuplicateItems().isEmpty()) {
                    String message = String.format(batchLogPrefix + "Duplicates reported: %d %n "
                            + "Response body: %s",
                            responseItems.getDuplicateItems().size(),
                            responseItems.getResponseBodyAsString()
                                    .substring(0, Math.min(1000, responseItems.getResponseBodyAsString().length())));
                    LOG.error(message);
                    throw new Exception(message);
                }

                // Get the missing items and add the original sequence bodies to the retry list
                missingItems.addAll(parseItems(responseItems.getMissingItems(), batchLogPrefix));
                retrySequenceBodyList.addAll(responseMap.get(future));
            } else {
                successfulFutures.add(future);
            }
        }
        recordMetrics(successfulFutures, responseMap);

        if (requestsAreSuccessful) {
            // All requests completed successfully
            LOG.info(batchLogPrefix + "Writing sequences completed successfully. Inserted {} sequences items "
                    + "and {} rows",
                    itemList.size(),
                    seqRowCounter);
        } else {
            LOG.warn(batchLogPrefix + "Write sequence rows failed. Most likely due to missing sequence header / metadata. "
                    + "Will add minimum sequence metadata and retry the sequence rows insert.");
            LOG.info(batchLogPrefix + "Number of missing entries reported by CDF: {}", missingItems.size());

            // check if the missing items are based on internal id--not supported
            List<SequenceBody> missingSequences = new ArrayList<>(missingItems.size());
            for (Item item : missingItems) {
                if (item.getIdTypeCase() != Item.IdTypeCase.EXTERNAL_ID) {
                    String message = batchLogPrefix + "Sequence with internal id refers to a non-existing time series. "
                            + "Only externalId is supported. Item specification: " + item.toString();
                    LOG.error(message);
                    throw new Exception(message);
                }
                // add the corresponding sequence body to a list for later processing
                itemList.stream()
                        .filter(sequence -> sequence.getExternalId().getValue().equals(item.getExternalId()))
                        .forEach(missingSequences::add);
            }
            LOG.debug(batchLogPrefix + "All missing items are based on externalId");

            // If we have missing items, add default sequence header
            if (missingSequences.isEmpty()) {
                LOG.warn(batchLogPrefix + "Write sequences rows failed, but cannot identify missing sequences headers");
            } else {
                LOG.info(batchLogPrefix + "Start writing default sequence headers for {} items",
                        missingSequences.size());
                metadataRetryCounter.inc();
                writeSeqHeaderForRows(missingSequences, projectConfig, batchLogPrefix);
            }

            // Retry the failed sequence body upsert
            LOG.info(batchLogPrefix + "Finished writing default headers. Will retry {} sequence body items.",
                    retrySequenceBodyList.size());
            Map<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> retryResponseMap =
                    splitAndUpsertSeqBody(retrySequenceBodyList, projectConfig, batchLogPrefix);
            List<CompletableFuture<ResponseItems<String>>> retryFutureList = new ArrayList<>();
            retryResponseMap.keySet().forEach(future -> retryFutureList.add(future));
            CompletableFuture<Void> allRetryFutures =
                    CompletableFuture.allOf(retryFutureList.toArray(new CompletableFuture[retryFutureList.size()]));
            allRetryFutures.join(); // wait for all requests to finish

            // Check status of the requests
            requestsAreSuccessful = true;
            for (CompletableFuture<ResponseItems<String>> future : retryFutureList) {
                requestsAreSuccessful = requestsAreSuccessful && future.join().isSuccessful();
            }
            if (requestsAreSuccessful) {
                // All requests completed successfully
                recordMetrics(retryFutureList, retryResponseMap);
                LOG.info(batchLogPrefix + "Writing sequences completed successfully. Inserted {} sequences items "
                                + "and {} rows",
                        itemList.size(),
                        seqRowCounter);
            }
        }

        if (!requestsAreSuccessful) {
            String message = batchLogPrefix + "Failed to write sequences rows.";
            LOG.error(message);
            throw new Exception(message);
        }
        LOG.info(batchLogPrefix + "Completed writing {} sequence items with {} total rows and {} cells "
                + "across {} requests within a duration of {}.",
                itemList.size(),
                seqRowCounter,
                seqCellCounter,
                responseMap.size(),
                Duration.between(startInstant, Instant.now()).toString());
        // output the committed items
        itemList.forEach(item -> outputReceiver.output(item));
    }

    /**
     * Record api metrics based on request response futures.
     * @param futureList
     * @param responseMap
     */
    private void recordMetrics(List<CompletableFuture<ResponseItems<String>>> futureList,
                               Map<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> responseMap) {
        if (writerConfig.isMetricsEnabled()) {
            futureList.forEach(future -> {
                MetricsUtil.recordApiRetryCounter(future.join(), apiRetryCounter);
                MetricsUtil.recordApiLatency(future.join(), apiLatency);
                List<SequenceBody> request = responseMap.getOrDefault(future, new ArrayList<>());
                seqBatch.update(request.size());
                seqRowsBatch.update(request.stream()
                        .mapToInt(sequenceBody -> sequenceBody.getRowsCount())
                        .sum());
            });
        }

    }

    /**
     * Post a (large) batch of {@link SequenceBody} by splitting it up into multiple parallel requests.
     *
     * The response from each individual request is returned along with its part of the input.
     *
     * @param itemList
     * @param projectConfig
     * @param batchLogPrefix
     * @return
     * @throws Exception
     */
    private Map<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> splitAndUpsertSeqBody(List<SequenceBody> itemList,
                                                                                                    ProjectConfig projectConfig,
                                                                                                    String batchLogPrefix) throws Exception {

        Map<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> responseMap = new HashMap<>();
        List<SequenceBody> batch = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH);
        int itemCounter = 0;
        int batchRowCounter = 0;
        int totalRowCounter = 0;
        int batchColumnsCounter = 0;
        int totalCellsCounter = 0;
        for (SequenceBody sequence : itemList)  {
            totalCellsCounter += sequence.getColumnsCount() * sequence.getRowsCount();
            batchColumnsCounter = Math.max(batchColumnsCounter, sequence.getColumnsCount());
            List<SequenceRow> rowList = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ROWS_PER_ITEM);
            for (SequenceRow row : sequence.getRowsList()) {
                rowList.add(row);
                batchRowCounter++;
                totalRowCounter++;
                if ((rowList.size() >= DEFAULT_SEQUENCE_WRITE_MAX_ROWS_PER_ITEM)
                        || ((batchRowCounter * batchColumnsCounter) >= DEFAULT_SEQUENCE_WRITE_MAX_CELLS_PER_BATCH))  {
                    batch.add(sequence.toBuilder()
                            .clearRows()
                            .addAllRows(rowList)
                            .build());
                    rowList = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ROWS_PER_ITEM);
                    itemCounter++;
                    if ((batch.size() >= DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH)
                            || ((batchRowCounter * batchColumnsCounter) >= DEFAULT_SEQUENCE_WRITE_MAX_CELLS_PER_BATCH)) {
                        responseMap.put(upsertSeqBody(batch, projectConfig, batchLogPrefix), batch);
                        batch = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH);
                        batchRowCounter = 0;
                        batchColumnsCounter = 0;
                    }
                }
            }
            if (rowList.size() > 0) {
                batch.add(sequence.toBuilder()
                        .clearRows()
                        .addAllRows(rowList)
                        .build());
                itemCounter++;
            }

            if (batch.size() >= DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH) {
                responseMap.put(upsertSeqBody(batch, projectConfig, batchLogPrefix), batch);
                batch = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH);
                batchRowCounter = 0;
                batchColumnsCounter = 0;
            }
        }
        if (batch.size() > 0) {
            responseMap.put(upsertSeqBody(batch, projectConfig, batchLogPrefix), batch);
        }

        LOG.debug(batchLogPrefix + "Finished submitting {} cells by {} rows across {} sequence items in {} requests batches.",
                totalCellsCounter,
                totalRowCounter,
                itemCounter,
                responseMap.size());

        return responseMap;
    }

    /**
     * Post a {@link SequenceBody} upsert request on a separate thread. The response is wrapped in a
     * {@link CompletableFuture} that is returned immediately to the caller.
     *
     * This method will send the entire input {@link List<SequenceBody>} in a single request. It does not
     * split the input into multiple batches. If you have a large batch of {@link SequenceBody} that
     * you would like to split across multiple requests, use the {@code splitAndUpsertSeqBody} method.
     *
     * @param itemList
     * @param projectConfig
     * @param batchLogPrefix
     * @return
     */
    private CompletableFuture<ResponseItems<String>> upsertSeqBody(List<SequenceBody> itemList,
                                                                    ProjectConfig projectConfig,
                                                                    String batchLogPrefix) throws Exception {
        // Check that all sequences carry an id/externalId + build items list
        ImmutableList.Builder<Map<String, Object>> insertItemsBuilder = ImmutableList.builder();
        int rowCounter = 0;
        int maxColumnCounter = 0;
        int cellCounter = 0;
        for (SequenceBody item : itemList) {
            rowCounter += item.getRowsCount();
            maxColumnCounter = Math.max(maxColumnCounter, item.getColumnsCount());
            cellCounter += (item.getColumnsCount() * item.getRowsCount());
            if (!(item.hasExternalId() || item.hasId())) {
                throw new Exception(batchLogPrefix + "Sequence body does not contain externalId nor id");
            }
            insertItemsBuilder.add(SequenceParser.toRequestInsertItem(item));
        }

        LOG.debug(batchLogPrefix + "Starting the upsert sequence body request. "
                + "No sequences: {}, Max no columns: {}, Total no rows: {}, Total no cells: {} ",
                itemList.size(),
                maxColumnCounter,
                rowCounter,
                cellCounter);

        // build request object
        RequestParameters postSeqBody = RequestParameters.create()
                .withItems(insertItemsBuilder.build())
                .withProjectConfig(projectConfig);

        // post write request
        return seqBodyWriterInsert.writeItemsAsync(postSeqBody);
    }

    private List<Item> parseItems(List<String> input, String batchLogPrefix) throws Exception {
        ImmutableList.Builder<Item> listBuilder = ImmutableList.builder();
        for (String item : input) {
            listBuilder.add(ItemParser.parseItem(item));
        }
        return listBuilder.build();
    }

    /**
     * Inserts default sequence headers for the input sequence list. Uses {@code writeSeqHeader} for
     * executing the write/insert batch.
     */
    private void writeSeqHeaderForRows(List<SequenceBody> sequenceList,
                                        ProjectConfig config,
                                        String batchLogPrefix) throws Exception {
        Map<String, Map<String, Object>> insertMap = new HashMap<>(100);
        for (SequenceBody sequence : sequenceList) {
            insertMap.put(sequence.getExternalId().getValue(), generateDefaultSequenceMetadataInsertItem(sequence));
            if (insertMap.size() >= 1000) {
                writeSeqHeaders(insertMap, config, batchLogPrefix);
                insertMap = new HashMap<>(100);
            }
        }

        if (!insertMap.isEmpty()) {
            writeSeqHeaders(insertMap, config, batchLogPrefix);
        }
    }

    /**
     * Executes the write sequence header operation for a batch of insert items.
     */
    private void writeSeqHeaders(Map<String, Map<String, Object>> insertMap,
                                ProjectConfig config,
                                String batchLogPrefix) throws Exception {
        Preconditions.checkArgument(!insertMap.isEmpty(), batchLogPrefix
                + "The insert item list cannot be empty.");
        int numRetries = 3;
        boolean isWriteSuccessful = false;
        List<Map<String, Object>> insertItems = new ArrayList<>(1000);
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < numRetries && !isWriteSuccessful; i++,
                Thread.sleep(Math.min(1000L, (10L * (long) Math.exp(i)) + random.nextLong(20)))) {
            insertItems.clear();
            insertItems.addAll(insertMap.values());

            // build request object
            RequestParameters postSeqHeaders = RequestParameters.create()
                    .withItems(insertItems)
                    .withProjectConfig(config);
            LOG.debug(batchLogPrefix + "Built upsert sequences metadata request for {} elements", insertItems.size());

            // post write request and monitor for duplicates
            ResponseItems<String> responseItems = seqHeaderWriterInsert.writeItems(postSeqHeaders);
            isWriteSuccessful = responseItems.isSuccessful();
            LOG.info(batchLogPrefix + "Write sequence metadata request sent. Result returned: {}", isWriteSuccessful);

            if (!isWriteSuccessful) {
                // we have duplicates. Remove them and try again.
                LOG.warn(batchLogPrefix + "Write sequence metadata failed. Most likely due to duplicates. "
                        + "Will remove them and retry.");
                List<Item> duplicates = parseItems(responseItems.getDuplicateItems(), batchLogPrefix);
                LOG.warn(batchLogPrefix + "Number of duplicate sequence header entries reported by CDF: {}", duplicates.size());
                LOG.warn(batchLogPrefix + "Reply payload: {}", responseItems.getResponseBodyAsString());

                // Remove the duplicates from the insert list
                for (Item value : duplicates) {
                    if (value.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                        insertItems.remove(value.getExternalId());
                    }
                }
            }
        }
        if (!isWriteSuccessful) {
            String message = batchLogPrefix + "Writing default sequence metadata for sequence rows failed. ";
            LOG.error(message);
            throw new Exception(message);
        }
    }

    /**
     * Builds a single sequence header with default values. It relies on information completeness
     * related to the columns as these cannot be updated at a later time.
     */
    private Map<String, Object> generateDefaultSequenceMetadataInsertItem(SequenceBody body) {
        Preconditions.checkArgument(body.hasExternalId(),
                "Sequence body is not based on externalId: " + body.toString());

        List<Map<String, Object>> columnList = new ArrayList<>();
        for (SequenceColumn column : body.getColumnsList()) {
            columnList.add(SequenceParser.toRequestInsertItem(column));
        }

        return ImmutableMap.<String, Object>builder()
                .put("externalId", body.getExternalId().getValue())
                .put("name", DEFAULT_SEQ_METADATA.getName().getValue())
                .put("description", DEFAULT_SEQ_METADATA.getDescription().getValue())
                .put("columns", columnList)
                .build();
    }
}
