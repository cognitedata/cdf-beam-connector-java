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
import com.cognite.v1.timeseries.proto.*;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.StringValue;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Cognite timeseries api endpoint.
 *
 * It provides methods for reading and writing {@link TimeseriesMetadata}.
 */
@AutoValue
public abstract class DataPoints extends ApiBase {
    private static final int DATA_POINTS_WRITE_MAX_ITEMS_PER_BATCH = 10_000;
    private static final int DATA_POINTS_WRITE_MAX_POINTS_PER_BATCH = 100_000;
    private static final int DATA_POINTS_WRITE_MAX_CHARS_PER_BATCH = 1_000_000;

    private static final TimeseriesMetadata DEFAULT_TS_METADATA = TimeseriesMetadata.newBuilder()
            .setExternalId(StringValue.of("java_sdk_default"))
            .setName(StringValue.of("java_sdk_default"))
            .setDescription(StringValue.of("Default TS metadata created by the Java SDK."))
            .setIsStep(false)
            .setIsString(false)
            .build();

    private static Builder builder() {
        return new com.cognite.client.AutoValue_DataPoints.Builder();
    }

    /**
     * Construct a new {@link DataPoints} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static DataPoints of(CogniteClient client) {
        return DataPoints.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link TimeseriesPoint} object that matches the filters set in the {@link RequestParameters}.
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
    public Iterator<List<TimeseriesPoint>> list(RequestParameters requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link TimeseriesPoint} objects that matches the filters set in the {@link RequestParameters} for
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
    public Iterator<List<TimeseriesPoint>> list(RequestParameters requestParameters, String... partitions) throws Exception {
        // todo: implement
        return new Iterator<List<TimeseriesPoint>>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public List<TimeseriesPoint> next() {
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
    public List<TimeseriesPoint> retrieve(List<Item> items) throws Exception {
        // todo: implement
        return Collections.emptyList();
    }

    /**
     * Creates or update a set of {@link TimeseriesPoint} objects.
     *
     * If it is a new {@link TimeseriesPoint} object (based on the {@code id / externalId}, then it will be created.
     *
     * If an {@link TimeseriesPoint} object already exists in Cognite Data Fusion, it will be updated. The update
     * behaviour is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * The algorithm runs as follows:
     * 1. Write all {@link TimeseriesPointPost} objects to the Cognite API.
     * 2. If one (or more) of the objects fail, check if it is because of missing time series objects--create temp headers.
     * 3. Retry the failed {@link TimeseriesPointPost} objects.
     *
     * @param dataPoints The data points to upsert
     * @return The upserted data points
     * @throws Exception
     */
    public List<TimeseriesPointPost> upsert(List<TimeseriesPointPost> dataPoints) throws Exception {
        Instant startInstant = Instant.now();
        String batchLogPrefix =
                "upsert() - batch " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Preconditions.checkArgument(dataPoints.stream().allMatch(point -> getTimeseriesId(point).isPresent()),
                batchLogPrefix + "All items must have externalId or id.");

        LOG.debug(batchLogPrefix + "Received {} data points to upsert",
                dataPoints.size());

        // Should not happen--but need to guard against empty input
        if (dataPoints.isEmpty()) {
            LOG.debug(batchLogPrefix + "Received an empty input list. Will just output an empty list.");
            return Collections.<TimeseriesPointPost>emptyList();
        }

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeTsDatapointsProto()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        /*
        Start the upsert:
        1. Write all sequences to the Cognite API.
        2. If one (or more) of the sequences fail, it is most likely because of missing headers. Add temp headers.
        3. Retry the failed sequences
        */
        Map<ResponseItems<String>, List<List<TimeseriesPointPost>>> responseMap = splitAndUpsertDataPoints(dataPoints, createItemWriter);
        LOG.debug(batchLogPrefix + "Completed create items requests for {} data points across {} batches at duration {}",
                dataPoints.size(),
                responseMap.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Check for unsuccessful request
        List<Item> missingItems = new ArrayList<>();
        List<List<TimeseriesPointPost>> retryDataPointsGroups = new ArrayList<>();
        List<ResponseItems<String>> successfulBatches = new ArrayList<>();
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

                // Get the missing items and add the original data points to the retry list
                missingItems.addAll(parseItems(responseItems.getMissingItems()));
                retryDataPointsGroups.addAll(responseMap.get(responseItems));
            } else {
                successfulBatches.add(responseItems);
            }
        }

        if (!requestsAreSuccessful) {
            LOG.warn(batchLogPrefix + "Write data points failed. Most likely due to missing header / metadata. "
                    + "Will add minimum time series metadata and retry the data points insert.");
            LOG.info(batchLogPrefix + "Number of missing entries reported by CDF: {}", missingItems.size());

            // check if the missing items are based on internal id--not supported
            List<TimeseriesPointPost> missingTimeSeries = new ArrayList<>(missingItems.size());
            for (Item item : missingItems) {
                if (item.getIdTypeCase() != Item.IdTypeCase.EXTERNAL_ID) {
                    String message = batchLogPrefix + "Sequence with internal id refers to a non-existing sequence. "
                            + "Only externalId is supported. Item specification: " + item.toString();
                    LOG.error(message);
                    throw new Exception(message);
                }
                // add a data point representing the item (via id) so we can create a header for it later.
                retryDataPointsGroups.stream()
                        .filter((List<TimeseriesPointPost> collection) ->
                                getTimeseriesId(collection.get(0)).get().equalsIgnoreCase(item.getExternalId()))
                        .forEach(collection -> missingTimeSeries.add(collection.get(0)));
            }
            LOG.debug(batchLogPrefix + "All missing items are based on externalId");

            // If we have missing items, add default time series header
            if (missingTimeSeries.isEmpty()) {
                LOG.warn(batchLogPrefix + "Write data points failed, but cannot identify missing headers");
            } else {
                LOG.debug(batchLogPrefix + "Start writing default time series headers for {} items",
                        missingTimeSeries.size());
                writeTsHeaderForPoints(missingTimeSeries);
            }

            // Retry the failed data points upsert
            List<TimeseriesPointPost> retryPointsList = new ArrayList<>();
            retryDataPointsGroups.stream()
                    .forEach(group -> retryPointsList.addAll(group));
            LOG.info(batchLogPrefix + "Finished writing default headers. Will retry {} data points. Duration {}",
                    retryDataPointsGroups.size(),
                    Duration.between(startInstant, Instant.now()));
            if (retryPointsList.isEmpty()) {
                LOG.warn(batchLogPrefix + "Write data points failed, but cannot identify data points to retry.");
            } else {
                Map<ResponseItems<String>, List<List<TimeseriesPointPost>>> retryResponseMap =
                        splitAndUpsertDataPoints(retryPointsList, createItemWriter);

                // Check status of the requests
                requestsAreSuccessful = true;
                for (ResponseItems<String> responseItems : retryResponseMap.keySet()) {
                    requestsAreSuccessful = requestsAreSuccessful && responseItems.isSuccessful();
                }
            }
        }

        if (!requestsAreSuccessful) {
            String message = batchLogPrefix + "Failed to write data points.";
            LOG.error(message);
            throw new Exception(message);
        }
        LOG.info(batchLogPrefix + "Completed writing {} data points "
                        + "across {} requests within a duration of {}.",
                dataPoints.size(),
                responseMap.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return dataPoints;
    }

    public List<Item> delete(List<Item> dataPoints) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteDatapoints()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildProjectConfig())
                .withDeleteItemMappingFunction(this::toRequestDeleteItem);

        return deleteItems.deleteItems(dataPoints);
    }

    /**
     * Writes a (large) batch of {@link TimeseriesPointPost} by splitting it up into multiple, parallel requests.
     *
     * The response from each individual request is returned along with its part of the input.
     * @param dataPoints
     * @param dataPointsWriter
     * @return
     * @throws Exception
     */
    private Map<ResponseItems<String>, List<List<TimeseriesPointPost>>> splitAndUpsertDataPoints(Collection<TimeseriesPointPost> dataPoints,
                                                                      ConnectorServiceV1.ItemWriter dataPointsWriter) throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "splitAndUpsertDataPoints() - ";
        Map<String, List<TimeseriesPointPost>> groupedPoints = sortAndGroupById(dataPoints);

        Map<CompletableFuture<ResponseItems<String>>, List<List<TimeseriesPointPost>>> responseMap = new HashMap<>();
        List<List<TimeseriesPointPost>> batch = new ArrayList<>();
        int totalItemCounter = 0;
        int totalPointsCounter = 0;
        int totalCharacterCounter = 0;
        int batchItemsCounter = 0;
        int batchPointsCounter = 0;
        int batchCharacterCounter = 0;
        for (Map.Entry<String, List<TimeseriesPointPost>> entry : groupedPoints.entrySet()) {
            List<TimeseriesPointPost> pointsList = new ArrayList<>();
            for (TimeseriesPointPost dataPoint : entry.getValue()) {
                // Check if the new data point will make the current batch too large.
                // If yes, submit the batch before continuing the iteration.
                if (batchPointsCounter + 1 >= DATA_POINTS_WRITE_MAX_POINTS_PER_BATCH
                        || batchCharacterCounter + getCharacterCount(dataPoint) >= DATA_POINTS_WRITE_MAX_CHARS_PER_BATCH) {
                    if (pointsList.size() > 0) {
                        // We have some points to add to the batch before submitting
                        batch.add(pointsList);
                        pointsList = new ArrayList<>();
                    }
                    responseMap.put(upsertDataPoints(batch, dataPointsWriter), batch);
                    batch = new ArrayList<>();
                    batchCharacterCounter = 0;
                    batchItemsCounter = 0;
                    batchPointsCounter = 0;
                }

                // Add the point to the points list
                pointsList.add(dataPoint);
                batchPointsCounter++;
                totalPointsCounter++;
                batchCharacterCounter += getCharacterCount(dataPoint);
                totalCharacterCounter += getCharacterCount(dataPoint);
            }
            if (pointsList.size() > 0) {
                batch.add(pointsList);
                totalItemCounter++;
            }

            if (batchItemsCounter >= DATA_POINTS_WRITE_MAX_ITEMS_PER_BATCH) {
                responseMap.put(upsertDataPoints(batch, dataPointsWriter), batch);
                batch = new ArrayList<>();
                batchCharacterCounter = 0;
                batchItemsCounter = 0;
                batchPointsCounter = 0;
            }
        }
        if (batch.size() > 0) {
            responseMap.put(upsertDataPoints(batch, dataPointsWriter), batch);
        }

        LOG.debug(loggingPrefix + "Finished submitting {} data points with {} characters across {} TS items "
                        + "in {} requests batches. Duration: {}",
                totalPointsCounter,
                totalCharacterCounter,
                totalItemCounter,
                responseMap.size(),
                Duration.between(startInstant, Instant.now()));

        // Wait for all requests futures to complete
        List<CompletableFuture<ResponseItems<String>>> futureList = new ArrayList<>();
        responseMap.keySet().forEach(future -> futureList.add(future));
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Collect the responses from the futures
        Map<ResponseItems<String>, List<List<TimeseriesPointPost>>> resultsMap = new HashMap<>(responseMap.size());
        for (Map.Entry<CompletableFuture<ResponseItems<String>>, List<List<TimeseriesPointPost>>> entry : responseMap.entrySet()) {
            resultsMap.put(entry.getKey().join(), entry.getValue());
        }

        return resultsMap;
    }

    /**
     * Post a collection of {@link TimeseriesPointPost} upsert request on a separate thread. The response is wrapped in a
     * {@link CompletableFuture} that is returned immediately to the caller.
     *
     * The data points must be grouped by id. That is, the inner list of data points must all belong to the same
     * time series. Multiple time series (max 10k) can be handled in a single collection.
     *
     *  This method will send the entire input in a single request. It does not
     *  split the input into multiple batches. If you have a large batch of {@link TimeseriesPointPost} that
     *  you would like to split across multiple requests, use the {@code splitAndUpsertDataPoints} method.
     *
     * @param dataPointsBatch
     * @param dataPointsWriter
     * @return
     * @throws Exception
     */
    private CompletableFuture<ResponseItems<String>> upsertDataPoints(Collection<List<TimeseriesPointPost>> dataPointsBatch,
                                                                      ConnectorServiceV1.ItemWriter dataPointsWriter) throws Exception {
        DataPointInsertionRequest requestPayload = toRequestProto(dataPointsBatch);

        // build request object
        RequestParameters postSeqBody = addAuthInfo(RequestParameters.create()
                .withProtoRequestBody(requestPayload));

        // post write request
        return dataPointsWriter.writeItemsAsync(postSeqBody);
    }

    /**
     * Builds a proto request object for upserting a collection of time series data points.
     *
     * @param dataPoints Data points to build request object for.
     * @return The proto request object.
     * @throws Exception
     */
    private DataPointInsertionRequest toRequestProto(Collection<List<TimeseriesPointPost>> dataPoints) {
        DataPointInsertionRequest.Builder requestBuilder = DataPointInsertionRequest.newBuilder();
        for (List<TimeseriesPointPost> points : dataPoints) {
            requestBuilder.addItems(this.toRequestProtoItem(points));
        }

        return requestBuilder.build();
    }

    /**
     * Convert a collection of time series point post object to a Cognite API request proto object.
     * All data points in the input collection must belong to the same time series (externalId / id).
     *
     * @param elements The time series point to build insert object for.
     * @return The proto insert object.
     */
    private DataPointInsertionItem toRequestProtoItem(Collection<TimeseriesPointPost> elements) {
        TimeseriesPointPost[] points = elements.toArray(new TimeseriesPointPost[0]);
        DataPointInsertionItem.Builder itemBuilder = DataPointInsertionItem.newBuilder();

        // set ids, identify points type
        if (points[0].getIdTypeCase() == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID) {
            itemBuilder.setExternalId(points[0].getExternalId());
        } else {
            itemBuilder.setId(points[0].getId());
        }

        if(points[0].getValueTypeCase() == TimeseriesPointPost.ValueTypeCase.VALUE_NUM) {
            NumericDatapoints.Builder numPointsBuilder = NumericDatapoints.newBuilder();
            for (TimeseriesPointPost point : points) {
                numPointsBuilder.addDatapoints(NumericDatapoint.newBuilder()
                        .setTimestamp(point.getTimestamp())
                        .setValue(point.getValueNum())
                        .build());
            }
            itemBuilder.setNumericDatapoints(numPointsBuilder.build());
        } else {
            StringDatapoints.Builder stringPointsBuilder = StringDatapoints.newBuilder();
            for (TimeseriesPointPost point : points) {
                stringPointsBuilder.addDatapoints(StringDatapoint.newBuilder()
                        .setTimestamp(point.getTimestamp())
                        .setValue(point.getValueString())
                        .build());
            }
            itemBuilder.setStringDatapoints(stringPointsBuilder.build());
        }
        return itemBuilder.build();
    }

    /**
     * Sorts and groups the data points into sub-collections per externalId / id. The data points are sorted
     * by timestamp, ascending order, before being grouped by time series id.
     *
     * This method will also de-duplicate the data points based on id and timestamp.
     *
     * @param dataPoints The data points to organize into sub-collections
     * @return The data points partitioned into sub-collections by externalId / id.
     */
    private Map<String, List<TimeseriesPointPost>> sortAndGroupById(Collection<TimeseriesPointPost> dataPoints) throws Exception {
        String loggingPrefix = "collectById() - ";

        // Sort the data points by timestamp
        List<TimeseriesPointPost> sortedPoints = new ArrayList<>(dataPoints);
        sortedPoints.sort(Comparator.comparingLong(point -> point.getTimestamp()));

        // Check all elements for id / externalId + naive deduplication
        Map<String, Map<Long, TimeseriesPointPost>> externalIdInsertMap = new HashMap<>(100);
        Map<Long, Map<Long, TimeseriesPointPost>> internalIdInsertMap = new HashMap<>(100);
        for (TimeseriesPointPost value : dataPoints) {
            if (value.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.IDTYPE_NOT_SET) {
                String message = loggingPrefix + "Neither externalId nor id found. "
                        + "Time series point must specify either externalId or id";
                LOG.error(message);
                throw new Exception(message);
            }
            if (value.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID) {
                if (!externalIdInsertMap.containsKey(value.getExternalId())) {
                    externalIdInsertMap.put(value.getExternalId(), new HashMap<Long, TimeseriesPointPost>(20000));
                }
                externalIdInsertMap.get(value.getExternalId()).put(value.getTimestamp(), value);
            } else {
                if (!internalIdInsertMap.containsKey(value.getId())) {
                    internalIdInsertMap.put(value.getId(), new HashMap<Long, TimeseriesPointPost>(10000));
                }
                internalIdInsertMap.get(value.getId()).put(value.getTimestamp(), value);
            }
        }

        // Collect the groups
        Map<String, List<TimeseriesPointPost>> result = new HashMap<>();
        externalIdInsertMap.forEach((key, value) -> {
            List<TimeseriesPointPost> points = new ArrayList<>(value.size());
            dataPoints.addAll(value.values());
            result.put(key, points);
        });
        internalIdInsertMap.forEach((key, value) -> {
            List<TimeseriesPointPost> points = new ArrayList<>(value.size());
            dataPoints.addAll(value.values());
            result.put(String.valueOf(key), points);
        });

        return result;
    }

    /**
     * Returns the total character count. If it is a numeric data point, the count will be 0.
     *
     * @param point The data point to check for character count.
     * @return The number of string characters.
     */
    private int getCharacterCount(TimeseriesPointPost point) {
        int count = 0;
        if (point.getValueTypeCase() == TimeseriesPointPost.ValueTypeCase.VALUE_STRING) {
            count = point.getValueString().length();
        }
        return count;
    }

    /**
     * Inserts default time series headers for the input data points list.
     */
    private void writeTsHeaderForPoints(List<TimeseriesPointPost> dataPoints) throws Exception {
        List<TimeseriesMetadata> tsMetadataList = new ArrayList<>();
        dataPoints.forEach(point -> tsMetadataList.add(generateDefaultTimeseriesMetadata(point)));

        if (!tsMetadataList.isEmpty()) {
            getClient().timeseries().upsert(tsMetadataList);
        }
    }

    /**
     * Builds a single sequence header with default values. It relies on information completeness
     * related to the columns as these cannot be updated at a later time.
     */
    private TimeseriesMetadata generateDefaultTimeseriesMetadata(TimeseriesPointPost dataPoint) {
        Preconditions.checkArgument(dataPoint.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID,
                "Data point is not based on externalId: " + dataPoint.toString());

        return DEFAULT_TS_METADATA.toBuilder()
                .setExternalId(StringValue.of(dataPoint.getExternalId()))
                .setIsStep(dataPoint.getIsStep())
                .setIsString(dataPoint.getValueTypeCase() == TimeseriesPointPost.ValueTypeCase.VALUE_STRING)
                .build();
    }


    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestDeleteItem(Item item) {
        Map<String, Object> deleteItem = new HashMap<>();

        // Add the id
        if (item.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
            deleteItem.put("externalId", item.getExternalId());
        } else if (item.getIdTypeCase() == Item.IdTypeCase.ID) {
            deleteItem.put("id", item.getId());
        } else {
            throw new RuntimeException("Item contains neither externalId nor id.");
        }

        // Add the time window
        if (item.hasInclusiveBegin()) {
            deleteItem.put("inclusiveBegin", item.getInclusiveBegin().getValue());
        } else {
            // Add a default start value
            deleteItem.put("inclusiveBegin", 0L);
        }

        if (item.hasExclusiveEnd()) {
            deleteItem.put("exclusiveEnd", item.getExclusiveEnd().getValue());
        }

        return deleteItem;
    }

    /*
    Returns the id of a time series header. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getTimeseriesId(TimeseriesMetadata item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId().getValue());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId().getValue()));
        } else {
            return Optional.<String>empty();
        }
    }

    /*
    Returns the id of a time series header. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getTimeseriesId(TimeseriesPointPost item) {
        if (item.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID) {
            return Optional.of(item.getExternalId());
        } else if (item.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.ID) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract DataPoints build();
    }
}
