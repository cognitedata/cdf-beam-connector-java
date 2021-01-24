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
import com.cognite.client.servicesV1.ConnectorConstants;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.TimeseriesParser;
import com.cognite.client.servicesV1.util.TSIterationUtilities;
import com.cognite.v1.timeseries.proto.*;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * This class represents the Cognite timeseries api endpoint.
 *
 * It provides methods for reading {@link TimeseriesPoint} and writing {@link TimeseriesPointPost}.
 */
@AutoValue
public abstract class DataPoints extends ApiBase {
    // Write request batch limits
    private static final int DATA_POINTS_WRITE_MAX_ITEMS_PER_REQUEST = 10_000;
    private static final int DATA_POINTS_WRITE_MAX_POINTS_PER_REQUEST = 100_000;
    private static final int DATA_POINTS_WRITE_MAX_CHARS_PER_REQUEST = 1_000_000;

    // Read request limits
    private static final int MAX_RAW_POINTS = 100000;
    private static final int MAX_AGG_POINTS = 10000;
    //private static final int PARALLELIZATION = 4;
    private static final int MAX_ITEMS_PER_REQUEST = 20;

    // Request parameter keys
    private static final String START_KEY = "start";
    private static final String END_KEY = "end";
    private static final String GRANULARITY_KEY = "granularity";
    private static final String AGGREGATES_KEY = "aggregates";

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

    protected static final Logger LOG = LoggerFactory.getLogger(DataPoints.class);

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
     * Returns all {@link TimeseriesPoint} objects that matches the filters set in the {@link RequestParameters}.
     *
     * Please note that only root-level filter and aggregate specifications are supported. That is, per-item
     * specifications of time filters and/or aggregations are not supported. If you need to apply different time
     * and/or aggregation specifications, then these should be submitted in separate requests--each using
     * root-level specifications.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the timeseries.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<TimeseriesPoint>> retrieve(RequestParameters requestParameters) throws Exception {
        String loggingPrefix = "retrieve() -";
        if (requestParameters.getItems().isEmpty()) {
            LOG.warn(loggingPrefix + "No items specified in the request. Will skip the read request.");
            return Collections.emptyIterator();
        }

        // Check that we have item ids and don't have per-item filter specifications
        for (Map<String, Object> item : requestParameters.getItems()) {
            Preconditions.checkArgument(itemHasId(item),
                    loggingPrefix + "All items must contain externalId or id.");
            Preconditions.checkArgument(!itemHasQuerySpecification(item),
                    loggingPrefix + "Per item query specification is not supported.");
        }

        // Build the api iterators.
        List<Iterator<CompletableFuture<ResponseItems<DataPointListItem>>>> iterators = new ArrayList<>();
        for (RequestParameters request : splitRetrieveRequest(requestParameters)) {
            iterators.add(getClient().getConnectorService().readTsDatapointsProto(addAuthInfo(request))
                    .withExecutorService(getClient().getExecutorService())
                    .withHttpClient(getClient().getHttpClient()));
            // todo: move executor service and client spec to connector service config
        }

        // The iterator that will collect results across multiple results streams
        FanOutIterator fanOutIterator = FanOutIterator.of(iterators);

        // Add results object parsing
        AdapterIterator adapterIterator = AdapterIterator.of(fanOutIterator, this::parseDataPointListItem);

        // Un-nest the nested results lists
        return FlatMapIterator.of(adapterIterator);
    }

    /**
     * Returns all {@link TimeseriesPoint} objects that matches the item specifications (externalId / id).
     *
     * the results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     * @param items
     * @return
     * @throws Exception
     */
    public Iterator<List<TimeseriesPoint>> retrieveComplete(List<Item> items) throws Exception {
        String loggingPrefix = "retrieveComplete() - ";
        List<Map<String, Object>> itemsList = new ArrayList<>();
        long endTimestamp = Instant.now().toEpochMilli();
        for (Item item : items) {
            if (item.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                itemsList.add(ImmutableMap.of("externalId", item.getExternalId()));
            } else if (item.getIdTypeCase() == Item.IdTypeCase.ID) {
                itemsList.add(ImmutableMap.of("id", item.getId()));
            } else {
                throw new Exception(String.format(loggingPrefix + "Item does not contain externalId nor id: %s"
                        , item.toString()));
            }
        }

        return this.retrieve(RequestParameters.create()
                .withItems(itemsList)
                .withRootParameter(START_KEY, 0L)
                .withRootParameter(END_KEY, endTimestamp)
                .withRootParameter("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE_TS_DATAPOINTS));
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
    public List<TimeseriesPointPost> upsert(@NotNull List<TimeseriesPointPost> dataPoints) throws Exception {
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
     * Split a retrieve data points request into multiple, smaller request for parallel retrieval.
     *
     * The splitting performed along two dimensions: 1) the time window and 2) time series items.
     *
     * First the algorithm looks at the total number of items and splits them based on a target
     * of 20 items per request. Depending on the effect of this split, the algorithm looks at
     * further splitting per time window.
     *
     *
     *
     * @param requestParameters
     * @return
     * @throws Exception
     */
    private List<RequestParameters> splitRetrieveRequest(RequestParameters requestParameters) throws Exception {
        String loggingPrefix = "splitRetrieveRequest - ";
        List<RequestParameters> splitsByItems = new ArrayList<>();

        // First, perform a split by items.
        if (requestParameters.getItems().size() > MAX_ITEMS_PER_REQUEST) {
            List<Map<String, Object>> itemsBatch = new ArrayList();
            int batchCounter = 0;
            for (ImmutableMap<String, Object> item : requestParameters.getItems()) {
                itemsBatch.add(item);
                batchCounter++;

                if (batchCounter >= MAX_ITEMS_PER_REQUEST) {
                    splitsByItems.add(requestParameters.withItems(itemsBatch));
                    itemsBatch = new ArrayList<>();
                    batchCounter = 0;
                }
            }
            if (itemsBatch.size() > 0) {
                splitsByItems.add(requestParameters.withItems(itemsBatch));
            }
            LOG.info(loggingPrefix + "Split the original {} request items across {} requests.",
                    requestParameters.getItems().size(),
                    splitsByItems.size());
        } else {
            // No need to split by items. Just replicate the original request.
            splitsByItems.add(requestParameters);
        }

        // If the split by items will utilize min 50% of available resources (read partitions and workers)
        // then we don't need to split further by time window.
        int capacity = Math.min(getClient().getClientConfig().getNoWorkers(),
                getClient().getClientConfig().getNoListPartitions());
        if (splitsByItems.size() / (long) capacity > 0.5) {
            LOG.info(loggingPrefix + "Splitting by items into {} requests offers good utilization of the available {} "
                    + "capacity units. Will not split further (by time window).",
                    splitsByItems.size(),
                    capacity);
            return splitsByItems;
        }

        // Split further by time windows.
        // Establish the request time window
        long startTimestamp = 0L;
        long endTimestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS).toEpochMilli();

        LOG.debug(loggingPrefix + "Get end time from request attribute {}: [{}]",
                END_KEY,
                requestParameters.getRequestParameters().get(END_KEY));
        Optional<Long> requestEndTime = TSIterationUtilities.getEndAsMillis(requestParameters);
        if (requestEndTime.isPresent()) {
            endTimestamp = requestEndTime.get();
        }

        LOG.debug(loggingPrefix + "Get start time from request attribute {}: [{}]",
                START_KEY,
                requestParameters.getRequestParameters().get(START_KEY));
        Optional<Long> requestStartTime = TSIterationUtilities.getStartAsMillis(requestParameters);
        if (requestStartTime.isPresent()) {
            startTimestamp = requestStartTime.get();
        }

        if (startTimestamp >= endTimestamp) {
            LOG.error(loggingPrefix + "Request start time > end time. Request parameters: {}", requestParameters);
            throw new Exception(loggingPrefix + "Request start time >= end time.");
        }

        //
        int noTsItems = splitsByItems.get(0).getItems().size();  // get the no items after the item split
        Duration duration = Duration.ofMillis(endTimestamp - startTimestamp);
        // Minimum duration is set based on a TS with 1Hz frequency and 20 iterations.
        final Duration SPLIT_LOWER_LIMIT = Duration.ofHours(Math.max(12, (240 / noTsItems)));

        LOG.debug(loggingPrefix + "Splitting request with {} items, a duration of {} and a min time window of {}.",
                noTsItems,
                duration.toString(),
                SPLIT_LOWER_LIMIT.toString());

        if (duration.compareTo(SPLIT_LOWER_LIMIT) < 0) {
            // The restriction range is too small to split.
            LOG.info(loggingPrefix + "The request's time window is too small to split. Will just keep it as it is.");
            return splitsByItems;
        }

        List<RequestParameters> splitByTimeWindow = new ArrayList<>();

        if (requestParameters.getRequestParameters().containsKey(GRANULARITY_KEY)) {
            // Run the aggregate split
            return splitsByItems; // no splits
        } else {
            // We have raw data points. Check the max frequency
            double maxFrequency = getMaxFrequency(requestParameters,
                    Instant.ofEpochMilli(startTimestamp),
                    Instant.ofEpochMilli(endTimestamp));
            if (maxFrequency == 0d) {
                // no datapoints in the range--don't split it
                LOG.warn(loggingPrefix + "Unable to build statistics for the restriction / range. No counts. "
                        + "Will keep the original range/restriction.");
                return splitsByItems;
            }

            // Calculate the number of splits by time window.
            long maxSplitsByCapacity = Math.floorDiv(capacity, splitsByItems.size()); // may result in zero
            long estimatedNoDataPoints = (long) maxFrequency * noTsItems * Duration.ofMillis(endTimestamp - startTimestamp).getSeconds();
            long minDataPointsPerRequest = 100_000 * 10L;
            long maxSplitsByFrequency = Math.floorDiv(estimatedNoDataPoints, minDataPointsPerRequest); // may result in zero
            long targetNoSplits = Math.min(maxSplitsByCapacity, maxSplitsByFrequency);

            if (targetNoSplits <= 1) {
                // no need to split further
                return splitsByItems;
            }
            long splitDelta = Math.floorDiv(endTimestamp - startTimestamp, targetNoSplits);
            long previousEnd = startTimestamp;
            for (int i = 0; i < targetNoSplits; i++) {
                long deltaStart = previousEnd;
                long deltaEnd = deltaStart + splitDelta;
                if (i == targetNoSplits - 1) {
                    // We are on the final iteration, so make sure we include the rest of the time range.
                    deltaEnd = endTimestamp;
                }
                for (RequestParameters request : splitsByItems) {
                    LOG.debug(loggingPrefix + "Adding time based split with start {} and end {}",
                            deltaStart,
                            deltaEnd);
                    splitByTimeWindow.add(request
                            .withRootParameter(START_KEY, deltaStart)
                            .withRootParameter(END_KEY, deltaEnd));
                }
            }
        }

        return splitByTimeWindow;
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
        LOG.debug(loggingPrefix + "Received {} data points to split and upsert.",
                dataPoints.size());
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
                if (batchPointsCounter + 1 >= DATA_POINTS_WRITE_MAX_POINTS_PER_REQUEST
                        || batchCharacterCounter + getCharacterCount(dataPoint) >= DATA_POINTS_WRITE_MAX_CHARS_PER_REQUEST) {
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

            if (batchItemsCounter >= DATA_POINTS_WRITE_MAX_ITEMS_PER_REQUEST) {
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
        LOG.debug(loggingPrefix + "Received {} data points to sort and group.",
                dataPoints.size());

        // Sort the data points by timestamp
        List<TimeseriesPointPost> sortedPoints = new ArrayList<>(dataPoints);
        sortedPoints.sort(Comparator.comparingLong(point -> point.getTimestamp()));

        // Check all elements for id / externalId + naive deduplication
        Map<String, Map<Long, TimeseriesPointPost>> externalIdInsertMap = new HashMap<>(100);
        Map<Long, Map<Long, TimeseriesPointPost>> internalIdInsertMap = new HashMap<>(100);
        for (TimeseriesPointPost value : sortedPoints) {
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
            points.addAll(value.values());
            result.put(key, points);
        });
        internalIdInsertMap.forEach((key, value) -> {
            List<TimeseriesPointPost> points = new ArrayList<>(value.size());
            points.addAll(value.values());
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

    /**
     * Check if a time series request item contains query specifications other than {@code externalId / id}.
     *
     * Per-item query specification is not supported for retrieve / read requests.
     * @param item The item to check for query specification.
     * @return true if a specification is detected, false if the item does not carry a specification.
     */
    private boolean itemHasQuerySpecification(Map<String, Object> item) {
        boolean hasSpecification = false;
        if (item.containsKey("granularity")
                || item.containsKey("aggregates")
                || item.containsKey("start")
                || item.containsKey("end")
                || item.containsKey("limit")
                || item.containsKey("includeOutsidePoints")) {
            hasSpecification = true;
        }
        return hasSpecification;
    }

    /**
     * Check if a time series request item contains an id specification ({@code externalId / id}).
     *
     * @param item The item to check for id.
     * @return true if an id is found, false if not.
     */
    private boolean itemHasId(Map<String, Object> item) {
        boolean hasId = false;
        if (item.containsKey("id")
                || item.containsKey("externalId")) {
            hasId = true;
        }
        return hasId;
    }

    /**
     * Calculate the max frequency of the TS items in the query.
     *
     * @param requestParameters
     * @param startOfWindow
     * @param endOfWindow
     * @return
     * @throws Exception
     */
    private double getMaxFrequency(RequestParameters requestParameters,
                                   Instant startOfWindow,
                                   Instant endOfWindow) throws Exception {
        final String loggingPrefix = "getMaxFrequency() - ";
        final Duration MAX_STATS_DURATION = Duration.ofDays(10);
        long from = startOfWindow.toEpochMilli();
        long to = endOfWindow.toEpochMilli();

        double frequency = 0d;

        Duration duration = Duration.ofMillis(to - from);
        if (duration.compareTo(MAX_STATS_DURATION) > 0) {
            // we have a really long range, shorten it for the statistics request.
            from = to - MAX_STATS_DURATION.toMillis();
            duration = Duration.ofMillis(to - from);
        }

        if (duration.compareTo(Duration.ofDays(1)) > 0) {
            // build stats from days granularity
            LOG.info(loggingPrefix + "Calculating TS stats based on day granularity, using a time window "
                            + "from [{}] to [{}]",
                    Instant.ofEpochMilli(from).toString(),
                    Instant.ofEpochMilli(to).toString());

            RequestParameters statsQuery = requestParameters
                    .withRootParameter(START_KEY, from)
                    .withRootParameter(END_KEY, to)
                    .withRootParameter(GRANULARITY_KEY, "d")
                    .withRootParameter(AGGREGATES_KEY, ImmutableList.of("count"));

            double averageCount = this.getMaxAverageCount(statsQuery);
            frequency = averageCount / (Duration.ofDays(1).toMinutes() * 60);
            LOG.info(loggingPrefix + "Average TS count per day: {}, frequency: {}", averageCount, frequency);

        } else {
            // build stats from hour granularity
            LOG.info(loggingPrefix + "Calculating TS stats based on hour granularity, using a time window "
                            + "from [{}] to [{}]",
                    Instant.ofEpochMilli(from).toString(),
                    Instant.ofEpochMilli(to).toString());

            RequestParameters statsQuery = requestParameters
                    .withRootParameter(START_KEY, from)
                    .withRootParameter(END_KEY, to)
                    .withRootParameter(GRANULARITY_KEY, "h")
                    .withRootParameter(AGGREGATES_KEY, ImmutableList.of("count"));

            double averageCount = this.getMaxAverageCount(statsQuery);
            frequency = averageCount / (Duration.ofHours(1).toMinutes() * 60);
            LOG.info(loggingPrefix + "Average TS count per hour: {}, frequency: {}", averageCount, frequency);
        }

        return frequency;
    }

    /**
     * Gets the max average TS count from a query.
     * @param query
     * @return
     * @throws Exception
     */
    private double getMaxAverageCount(RequestParameters query) throws Exception {
        String loggingPrefix = "getMaxAverageCount() - ";
        Preconditions.checkArgument(query.getRequestParameters().containsKey(AGGREGATES_KEY)
                        && query.getRequestParameters().get(AGGREGATES_KEY) instanceof List
                        && ((List) query.getRequestParameters().get(AGGREGATES_KEY)).contains("count"),
                "The query must specify the count aggregate.");

        double average = 0d;
        try {
            Iterator<CompletableFuture<ResponseItems<DataPointListItem>>> results =
                    getClient().getConnectorService().readTsDatapointsProto(addAuthInfo(query));
            CompletableFuture<ResponseItems<DataPointListItem>> responseItemsFuture;
            ResponseItems<DataPointListItem> responseItems;

            while (results.hasNext()) {
                responseItemsFuture = results.next();
                responseItems = responseItemsFuture.join();

                if (!responseItems.isSuccessful()) {
                    // something went wrong with the request
                    String message = loggingPrefix + "Error while iterating through the results from Fusion: "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }

                for (DataPointListItem item : responseItems.getResultsItems()) {
                    LOG.debug(loggingPrefix + "Item in results list, Ts id: {}", item.getId());
                    List<TimeseriesPoint> points = TimeseriesParser.parseDataPointListItem(item);
                    LOG.info(loggingPrefix + "Number of datapoints in TS list item: {}", points.size());

                    double candidate = points.stream()
                            .map(TimeseriesPoint::getValueAggregates)
                            .map(TimeseriesPoint.Aggregates::getCount)
                            .mapToDouble(Int64Value::getValue)
                            .average()
                            .orElse(0d);

                    if (candidate > average) average = candidate;
                }
            }
        } catch (Exception e) {
            LOG.error(loggingPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
        return average;
    }

    /**
     * Builds a request object with the specified start and end times.
     *
     * @param requestParameters
     * @param start
     * @param end
     * @return
     */
    private RequestParameters buildRequestParameters(RequestParameters requestParameters,
                                                     long start,
                                                     long end) {
        String loggingPrefix = "buildRequestParameters() - ";
        Preconditions.checkArgument(start < end, "Trying to build request with start >= end.");
        Preconditions.checkArgument(
                ((Long) requestParameters.getRequestParameters().getOrDefault(START_KEY, 0L)) <= start,
                "Trying to build request with start < original start time.");
        Preconditions.checkArgument(
                ((Long) requestParameters.getRequestParameters().getOrDefault(END_KEY, Long.MAX_VALUE)) >= end,
                "Trying to build request with end > original end time.");
        LOG.debug(loggingPrefix + "Building RequestParameters with start = {} and end = {}", start, end);
        return requestParameters
                .withRootParameter(START_KEY, start)
                .withRootParameter(END_KEY, end);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<TimeseriesPoint> parseDataPointListItem(DataPointListItem listItem) {
        try {
            return TimeseriesParser.parseDataPointListItem(listItem);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    /**
     * This {@link Iterator} takes the input from an input {@link Iterator} and maps the output to a new
     * type via a mapping {@link Function}.
     *
     * The input {@link Iterator} must provide a nested list ({@code List<List<T>>}) as its output.
     * I.e. it iterates over a potentially large collection via a set of batches.
     *
     * @param <T> The element type of the input iterator's nested list.
     */
    @AutoValue
    public abstract static class FlatMapIterator<T> implements Iterator<List<T>> {

        private static <T> DataPoints.FlatMapIterator.Builder<T> builder() {
            return new AutoValue_DataPoints_FlatMapIterator.Builder<T>();
        }

        /**
         * Creates a new {@link DataPoints.FlatMapIterator} translating the input {@link Iterator} elements by
         * unwrapping the nested list objects.
         *
         * @param inputIterator The iterator who's elements should be un-nested.
         * @param <T> The object type of the input iterator's list.
         * @return The iterator producing the mapped objects.
         */
        public static <T> DataPoints.FlatMapIterator<T> of(Iterator<List<List<T>>> inputIterator) {
            return DataPoints.FlatMapIterator.<T>builder()
                    .setInputIterator(inputIterator)
                    .build();
        }

        abstract Iterator<List<List<T>>> getInputIterator();

        @Override
        public boolean hasNext() {
            return getInputIterator().hasNext();
        }

        @Override
        public List<T> next() throws NoSuchElementException {
            if (!this.hasNext()) {
                throw new NoSuchElementException("No more elements to iterate over.");
            }

            List<T> results = new ArrayList<>();
            getInputIterator().next().stream()
                    .forEach(list -> results.addAll(list));

            return results;
        }

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract DataPoints.FlatMapIterator.Builder<T> setInputIterator(Iterator<List<List<T>>> value);

            abstract DataPoints.FlatMapIterator<T> build();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract DataPoints build();
    }
}
