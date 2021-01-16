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

package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.SequenceParser;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.StringValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.cognite.client.servicesV1.ConnectorConstants.*;
import static com.cognite.client.servicesV1.ConnectorConstants.DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH;

/**
 * This class represents the Cognite sequence body/rows api endpoint.
 *
 * It provides methods for reading and writing {@link SequenceBody}.
 */
@AutoValue
public abstract class SequenceRows extends ApiBase {
    private static final SequenceMetadata DEFAULT_SEQ_METADATA = SequenceMetadata.newBuilder()
            .setExternalId(StringValue.of("SDK_default"))
            .setName(StringValue.of("SDK_default"))
            .setDescription(StringValue.of("Default Sequence metadata created by the Java SDK."))
            .build();

    private static Builder builder() {
        return new AutoValue_SequenceRows.Builder();
    }

    /**
     * Construct a new {@link SequenceRows} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static SequenceRows of(CogniteClient client) {
        return SequenceRows.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link SequenceBody} object that matches the filters set in the {@link RequestParameters}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The timeseries are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving timeseries.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<SequenceBody>> list(RequestParameters requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link SequenceBody} objects that matches the filters set in the {@link RequestParameters} for
     * the specified partitions. This method is intended for advanced use cases where you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the timeseries.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<SequenceBody>> list(RequestParameters requestParameters, String... partitions) throws Exception {
        // todo: implement
        return new Iterator<List<SequenceBody>>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public List<SequenceBody> next() {
                return null;
            }
        };
    }

    /**
     * Retrieves timeseries by id.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved timeseries.
     * @throws Exception
     */
    public List<SequenceBody> retrieve(List<Item> items) throws Exception {
        // todo: implement
        return Collections.emptyList();
    }

    /**
     * Creates or updates a set of {@link SequenceBody} objects.
     *
     * @param sequenceBodies The sequences rows to upsert
     * @return The upserted sequences rows
     * @throws Exception
     */
    public List<SequenceBody> upsert(List<SequenceBody> sequenceBodies) throws Exception {
        Instant startInstant = Instant.now();
        String batchLogPrefix =
                "upsert() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Preconditions.checkArgument(sequenceBodies.stream().allMatch(sequenceBody -> getSequenceId(sequenceBody).isPresent()),
                batchLogPrefix + "All items must have externalId or id.");

        int inputRowsCounter = 0;
        int inputCellsCounter = 0;
        for (SequenceBody sequenceBody : sequenceBodies) {
            inputRowsCounter += sequenceBody.getRowsCount();
            inputCellsCounter += sequenceBody.getRowsCount() * sequenceBody.getColumnsCount();
        }
        LOG.debug(batchLogPrefix + "Received {} sequence body objects with {} cells across {} rows to upsert",
                sequenceBodies.size(),
                inputCellsCounter,
                inputRowsCounter);

        // Should not happen--but need to guard against empty input
        if (sequenceBodies.isEmpty()) {
            LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
            return Collections.<SequenceBody>emptyList();
        }

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeSequencesRows()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        /*
        Start the upsert:
        1. Write all sequences to the Cognite API.
        2. If one (or more) of the sequences fail, it is most likely because of missing headers. Add temp headers.
        3. Retry the failed sequences
        */
        Map<ResponseItems<String>, List<SequenceBody>> responseMap = splitAndUpsertSeqBody(sequenceBodies, createItemWriter);
        LOG.debug(batchLogPrefix + "Completed create items requests for {} input items across {} batches at duration {}",
                sequenceBodies.size(),
                responseMap.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Check for unsuccessful request
        List<Item> missingItems = new ArrayList<>();
        List<SequenceBody> retrySequenceBodyList = new ArrayList<>(sequenceBodies);
        List<ResponseItems<String>> successfulBatches = new ArrayList<>(sequenceBodies.size());
        boolean requestsAreSuccessful = true;
        for (ResponseItems<String> responseItems : responseMap.keySet()) {
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
                missingItems.addAll(parseItems(responseItems.getMissingItems()));
                retrySequenceBodyList.addAll(responseMap.get(responseItems));
            } else {
                successfulBatches.add(responseItems);
            }
        }

        if (!requestsAreSuccessful) {
            LOG.warn(batchLogPrefix + "Write sequence rows failed. Most likely due to missing sequence header / metadata. "
                    + "Will add minimum sequence metadata and retry the sequence rows insert.");
            LOG.info(batchLogPrefix + "Number of missing entries reported by CDF: {}", missingItems.size());

            // check if the missing items are based on internal id--not supported
            List<SequenceBody> missingSequences = new ArrayList<>(missingItems.size());
            for (Item item : missingItems) {
                if (item.getIdTypeCase() != Item.IdTypeCase.EXTERNAL_ID) {
                    String message = batchLogPrefix + "Sequence with internal id refers to a non-existing sequence. "
                            + "Only externalId is supported. Item specification: " + item.toString();
                    LOG.error(message);
                    throw new Exception(message);
                }
                // add the corresponding sequence body to a list for later processing
                sequenceBodies.stream()
                        .filter(sequence -> sequence.getExternalId().getValue().equals(item.getExternalId()))
                        .forEach(missingSequences::add);
            }
            LOG.debug(batchLogPrefix + "All missing items are based on externalId");

            // If we have missing items, add default sequence header
            if (missingSequences.isEmpty()) {
                LOG.warn(batchLogPrefix + "Write sequences rows failed, but cannot identify missing sequences headers");
            } else {
                LOG.debug(batchLogPrefix + "Start writing default sequence headers for {} items",
                        missingSequences.size());
                writeSeqHeaderForRows(missingSequences);
            }

            // Retry the failed sequence body upsert
            LOG.debug(batchLogPrefix + "Finished writing default headers. Will retry {} sequence body items.",
                    retrySequenceBodyList.size());
            Map<ResponseItems<String>, List<SequenceBody>> retryResponseMap =
                    splitAndUpsertSeqBody(retrySequenceBodyList, createItemWriter);

            // Check status of the requests
            requestsAreSuccessful = true;
            for (ResponseItems<String> responseItems : retryResponseMap.keySet()) {
                requestsAreSuccessful = requestsAreSuccessful && responseItems.isSuccessful();
            }
        }

        if (!requestsAreSuccessful) {
            String message = batchLogPrefix + "Failed to write sequences rows.";
            LOG.error(message);
            throw new Exception(message);
        }
        LOG.info(batchLogPrefix + "Completed writing {} sequence items with {} total rows and {} cells "
                        + "across {} requests within a duration of {}.",
                sequenceBodies.size(),
                inputRowsCounter,
                inputCellsCounter,
                responseMap.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return ImmutableList.copyOf(sequenceBodies);
    }

    public List<SequenceBody> delete(List<SequenceBody> sequenceRows) throws Exception {
        // todo: implement

        return Collections.emptyList();
    }

    /**
     * Post a (large) batch of {@link SequenceBody} by splitting it up into multiple parallel requests.
     *
     * The response from each individual request is returned along with its part of the input.
     *
     * @param itemList
     * @param seqBodyCreateWriter
     * @return
     * @throws Exception
     */
    private Map<ResponseItems<String>, List<SequenceBody>> splitAndUpsertSeqBody(List<SequenceBody> itemList,
                                                                                 ConnectorServiceV1.ItemWriter seqBodyCreateWriter) throws Exception {
        String loggingPrefix = "splitAndUpsertSeqBody() - ";
        Map<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> responseMap = new HashMap<>();
        List<SequenceBody> batch = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH);
        List<String> sequenceIds = new ArrayList<>(); // To check for existing / duplicate item ids
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
                    // Check for duplicate items in the same batch
                    if (getSequenceId(sequence).isPresent()) {
                        if (sequenceIds.contains(getSequenceId(sequence).get())) {
                            // The externalId / id already exists in the batch, submit it
                            responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
                            batch = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH);
                            batchRowCounter = 0;
                            batchColumnsCounter = 0;
                            sequenceIds.clear();
                        }
                        sequenceIds.add(getSequenceId(sequence).get());
                    }
                    batch.add(sequence.toBuilder()
                            .clearRows()
                            .addAllRows(rowList)
                            .build());
                    rowList = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ROWS_PER_ITEM);
                    itemCounter++;
                    if ((batch.size() >= DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH)
                            || ((batchRowCounter * batchColumnsCounter) >= DEFAULT_SEQUENCE_WRITE_MAX_CELLS_PER_BATCH)) {
                        responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
                        batch = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH);
                        batchRowCounter = 0;
                        batchColumnsCounter = 0;
                        sequenceIds.clear();
                    }
                }
            }
            if (rowList.size() > 0) {
                // Check for duplicate items in the same batch
                if (getSequenceId(sequence).isPresent()) {
                    if (sequenceIds.contains(getSequenceId(sequence).get())) {
                        // The externalId / id already exists in the batch, submit it
                        responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
                        batch = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH);
                        batchRowCounter = 0;
                        batchColumnsCounter = 0;
                        sequenceIds.clear();
                    }
                    sequenceIds.add(getSequenceId(sequence).get());
                }
                batch.add(sequence.toBuilder()
                        .clearRows()
                        .addAllRows(rowList)
                        .build());
                itemCounter++;
            }

            if (batch.size() >= DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH) {
                responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
                batch = new ArrayList<>(DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH);
                batchRowCounter = 0;
                batchColumnsCounter = 0;
                sequenceIds.clear();
            }
        }
        if (batch.size() > 0) {
            responseMap.put(upsertSeqBody(batch, seqBodyCreateWriter), batch);
        }

        LOG.debug(loggingPrefix + "Finished submitting {} cells by {} rows across {} sequence items in {} requests batches.",
                totalCellsCounter,
                totalRowCounter,
                itemCounter,
                responseMap.size());

        // Wait for all requests futures to complete
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(future -> futureList.add(future));
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the responses from the futures
        Map<ResponseItems<String>, List<SequenceBody>> resultsMap = new HashMap<>(responseMap.size());
        for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<SequenceBody>> entry : responseMap.entrySet()) {
            resultsMap.put(entry.getKey().join(), entry.getValue());
        }

        return resultsMap;
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
     * @param seqBodyCreateWriter
     * @return
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> upsertSeqBody(List<SequenceBody> itemList,
                                                                   ConnectorServiceV1.ItemWriter seqBodyCreateWriter) throws Exception {
        String loggingPrefix = "upsertSeqBody() - ";
        // Check that all sequences carry an id/externalId + no duplicates + build items list
        ImmutableList.Builder<Map<String, Object>> insertItemsBuilder = ImmutableList.builder();
        int rowCounter = 0;
        int maxColumnCounter = 0;
        int cellCounter = 0;
        List<String> sequenceIds = new ArrayList<>();
        for (SequenceBody item : itemList) {
            rowCounter += item.getRowsCount();
            maxColumnCounter = Math.max(maxColumnCounter, item.getColumnsCount());
            cellCounter += (item.getColumnsCount() * item.getRowsCount());
            if (!(item.hasExternalId() || item.hasId())) {
                throw new Exception(loggingPrefix + "Sequence body does not contain externalId nor id");
            }
            if (getSequenceId(item).isPresent()) {
                if (sequenceIds.contains(getSequenceId(item).get())) {
                    throw new Exception(String.format(loggingPrefix + "Duplicate sequence body items detected. ExternalId: %s",
                            getSequenceId(item).get()));
                }
                sequenceIds.add(getSequenceId(item).get());
            }
            insertItemsBuilder.add(SequenceParser.toRequestInsertItem(item));
        }

        LOG.debug(loggingPrefix + "Starting the upsert sequence body request. "
                        + "No sequences: {}, Max no columns: {}, Total no rows: {}, Total no cells: {} ",
                itemList.size(),
                maxColumnCounter,
                rowCounter,
                cellCounter);

        // build request object
        RequestParameters postSeqBody = addAuthInfo(RequestParameters.create()
                .withItems(insertItemsBuilder.build()));

        // post write request
        return seqBodyCreateWriter.writeItemsAsync(postSeqBody);
    }

    /**
     * Inserts default sequence headers for the input sequence list.
     */
    private void writeSeqHeaderForRows(List<SequenceBody> sequenceList) throws Exception {
        List<SequenceMetadata> sequenceMetadataList = new ArrayList<>(sequenceList.size());
        sequenceList.forEach(sequenceBody -> sequenceMetadataList.add(generateDefaultSequenceMetadataInsertItem(sequenceBody)));

        if (!sequenceMetadataList.isEmpty()) {
            getClient().sequences().upsert(sequenceMetadataList);
        }
    }

    /**
     * Builds a single sequence header with default values. It relies on information completeness
     * related to the columns as these cannot be updated at a later time.
     */
    private SequenceMetadata generateDefaultSequenceMetadataInsertItem(SequenceBody body) {
        Preconditions.checkArgument(body.hasExternalId(),
                "Sequence body is not based on externalId: " + body.toString());

        return DEFAULT_SEQ_METADATA.toBuilder()
                .setExternalId(body.getExternalId())
                .addAllColumns(body.getColumnsList())
                .build();
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private SequenceBody parseSequenceBody(String json) {
        try {
            return SequenceParser.parseSequenceBody(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(SequenceBody item) {
        try {
            return SequenceParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of a sequence. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getSequenceId(SequenceBody item) {
        if (item.hasExternalId()) {
          return Optional.of(item.getExternalId().getValue());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId().getValue()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract SequenceRows build();
    }
}
