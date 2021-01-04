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

package com.cognite.beam.io.fn.read;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.dto.TimeseriesPoint;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.TimeseriesParser;
import com.cognite.client.servicesV1.util.TSIterationUtilities;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.cognite.v1.timeseries.proto.DataPointListItem;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Int64Value;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;


/**
 * This function reads time series data points based on the input RequestParameters. It is based on a splittable
 * DoFn to fan out the reads of a request.
 *
 */
@DoFn.BoundedPerElement
public class ReadTsPointProtoSdf extends DoFn<RequestParameters, Iterable<TimeseriesPoint>> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
    private final String loggingPrefix = "Read TS points sdf [" + randomIdString + "] -";

    private static final String START_KEY = "start";
    private static final String END_KEY = "end";
    private static final String GRANULARITY_KEY = "granularity";
    private static final String AGGREGATES_KEY = "aggregates";

    private static final int MAX_RAW_POINTS = 100000;
    private static final int MAX_AGG_POINTS = 10000;
    private static final int PARALLELIZATION = 4;

    private final ConnectorServiceV1 connector;
    private final boolean isMetricsEnabled;
    private final boolean isStreaming;

    private final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    private final Distribution tsPointsBatch = Metrics.distribution("cognite", "tsPointsBatchSize");
    private final Distribution tsBatch = Metrics.distribution("cognite", "numberOfTS");
    private final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    public ReadTsPointProtoSdf(Hints hints, ReaderConfig readerConfig) {
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(readerConfig.getAppIdentifier())
                .setSessionIdentifier(readerConfig.getSessionIdentifier())
                .build();

        isMetricsEnabled = readerConfig.isMetricsEnabled();
        isStreaming = readerConfig.isStreamingEnabled();
    }

    @Setup
    public void setup() {
        LOG.info("Setting up TS point proto reader.");
    }

    @ProcessElement
    public void processElement(@Element RequestParameters query,
                               RestrictionTracker<OffsetRange, Long> tracker,
                               OutputReceiver<Iterable<TimeseriesPoint>> out,
                               ManualWatermarkEstimator watermarkEstimator) throws Exception {
        final String batchIdentifierPrefix = "Request batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        final String localLoggingPrefix = loggingPrefix + batchIdentifierPrefix;
        LOG.info(localLoggingPrefix + "Streaming mode: {}", isStreaming);
        LOG.debug(localLoggingPrefix + "Input query: {}", query.toString());
        LOG.info(localLoggingPrefix + "Reading TS based on input restriction {}", tracker.currentRestriction());

        long startRange = tracker.currentRestriction().getFrom();
        long endRange = tracker.currentRestriction().getTo();
        LOG.info(localLoggingPrefix + "Start of range: [{}], end of range: [{}]",
                Instant.ofEpochMilli(startRange).toString(),
                Instant.ofEpochMilli(endRange).toString());

        if (isStreaming) {
            watermarkEstimator.setWatermark(org.joda.time.Instant.ofEpochMilli(startRange));
        }

        // Check if this instance can process this item based on the current restriction
        if (!tracker.tryClaim(endRange - 1)) {
            return;
        }
        // Build query so it conforms with the current restriction
        RequestParameters queryRestricted = buildRequestParameters(query, startRange, endRange, localLoggingPrefix);
        try {
            Iterator<CompletableFuture<ResponseItems<DataPointListItem>>> results =
                    connector.readTsDatapointsProto(queryRestricted);
            CompletableFuture<ResponseItems<DataPointListItem>> responseItemsFuture;
            ResponseItems<DataPointListItem> responseItems;

            while (results.hasNext()) {
                responseItemsFuture = results.next();
                responseItems = responseItemsFuture.join();
                int tsPointsCounter = 0;
                List<TimeseriesPoint> pointsOutput = new ArrayList<>(20000);
                long minTimestampMs = Long.MAX_VALUE;

                if (!responseItems.isSuccessful()) {
                    // something went wrong with the request
                    String message = batchIdentifierPrefix + "Error while iterating through the results from Fusion: "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }

                // Gather all TS points
                for (DataPointListItem item : responseItems.getResultsItems()) {
                    List<TimeseriesPoint> points = TimeseriesParser.parseDataPointListItem(item);
                    for (TimeseriesPoint point : points) {
                        pointsOutput.add(point);
                        if (minTimestampMs > point.getTimestamp()) {
                            minTimestampMs = point.getTimestamp();
                        }
                        tsPointsCounter++;
                    }
                }

                if (isStreaming) {
                    // output with timestamps in streaming mode--need that for windowing
                    out.outputWithTimestamp(pointsOutput, org.joda.time.Instant.ofEpochMilli(minTimestampMs));
                } else {
                    // no timestamping in batch mode--just leads to lots of complications
                    out.output(pointsOutput);
                }
                LOG.info(batchIdentifierPrefix + "Received {} TS and {} datapoints in the response.",
                        responseItems.getResultsItems().size(),
                        tsPointsCounter);

                if (isMetricsEnabled) {
                    MetricsUtil.recordApiLatency(responseItems, apiLatency);
                    MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                    tsBatch.update(responseItems.getResultsItems().size());
                    tsPointsBatch.update(tsPointsCounter);
                }
            }
        } catch (Exception e) {
            LOG.error(localLoggingPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }

    @GetInitialWatermarkEstimatorState
    public org.joda.time.Instant getInitialWatermarkEstimatorState(@Restriction OffsetRange restriction) {
        return org.joda.time.Instant.ofEpochMilli(restriction.getFrom());
    }

    @NewWatermarkEstimator
    public ManualWatermarkEstimator<org.joda.time.Instant> newWatermarkEstimator(@WatermarkEstimatorState org.joda.time.Instant state) {
        return new WatermarkEstimators.Manual(state);
    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(RequestParameters requestParameters) throws Exception {
        long startTimestamp = 0L;
        long endTimestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS).toEpochMilli();

        // check if we have an end time
        LOG.debug(loggingPrefix + "GetInitialRestriction, get end time from request attribute {}: [{}]",
                END_KEY,
                requestParameters.getRequestParameters().get(END_KEY));
        Optional<Long> requestEndTime = TSIterationUtilities.getEndAsMillis(requestParameters);
        if (requestEndTime.isPresent()) {
            endTimestamp = requestEndTime.get();
        }

        // check for start time
        LOG.debug(loggingPrefix + "GetInitialRestriction, get start time from request attribute {}: [{}]",
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

        return new OffsetRange(startTimestamp, endTimestamp);
    }

    @SplitRestriction
    public void splitRestriction(RequestParameters requestParameters,
                                 OffsetRange offsetRange,
                                 OutputReceiver<OffsetRange> out) throws Exception {
        int noTsItems = requestParameters.getItems().size();
        Duration duration = Duration.ofMillis(offsetRange.getTo() - offsetRange.getFrom());
        final Duration SPLIT_LOWER_LIMIT = Duration.ofMinutes(Math.max(60, (1440 / noTsItems)));

        LOG.info(loggingPrefix + "Splitting restriction for request with {} items, a duration of {} and a split limit of {}.",
                noTsItems,
                duration.toString(),
                SPLIT_LOWER_LIMIT.toString());

        if (duration.compareTo(SPLIT_LOWER_LIMIT) < 0) {
            // The restriction range is too small to split.
            LOG.info(loggingPrefix + "The restriction / range is too small to split. Will just keep it as it is.");
            out.output(offsetRange);
            return;
        }

        List<OffsetRange> offsetRanges;

        if (requestParameters.getRequestParameters().containsKey(GRANULARITY_KEY)) {
            // Run the aggregate split
            offsetRanges = ImmutableList.of(offsetRange); // no splits
        } else {
            // We have raw data points. Check the max frequency
            double maxFrequency = getMaxFrequency(requestParameters, offsetRange);
            if (maxFrequency == 0d) {
                // no datapoints in the range--don't split it
                LOG.warn(loggingPrefix + "Unable to build statistics for the restriction / range. No counts. "
                        + "Will keep the original range/restriction.");
                out.output(offsetRange);
                return;
            }
            int maxPointsPerTS = MAX_RAW_POINTS / Math.max(1, (noTsItems / PARALLELIZATION));
            Duration targetDuration = Duration.ofSeconds(Double.valueOf(maxPointsPerTS / maxFrequency).longValue());
            if (targetDuration.compareTo(duration) < 0) {
                offsetRanges = offsetRange.split(targetDuration.toMillis(), targetDuration.toMillis() / 2);
                LOG.info(loggingPrefix + "Restriction / range will be split into {} parts", offsetRanges.size());
            } else {
                // current duration is less than the target duration, do not split
                LOG.info(loggingPrefix + "The restriction / range is too small to split. Will just keep it as it is.");
                offsetRanges = ImmutableList.of(offsetRange); // no splits
            }
        }

        for (OffsetRange range : offsetRanges) {
            out.output(range);
        }
    }

    /* Calculate the max frequency of the TS items in the query */
    private double getMaxFrequency(RequestParameters requestParameters,
                                   OffsetRange offsetRange) throws Exception {
        final String localLoggingPrefix = loggingPrefix + "Build TS stats. ";
        final Duration MAX_STATS_DURATION = Duration.ofDays(10);
        long from = offsetRange.getFrom();
        long to = offsetRange.getTo();

        double frequency = 0d;

        Duration duration = Duration.ofMillis(to - from);
        if (duration.compareTo(MAX_STATS_DURATION) > 0) {
            // we have a really long range, shorten it
            from = to - MAX_STATS_DURATION.toMillis();
            duration = Duration.ofMillis(to - from);
        }

        if (duration.compareTo(Duration.ofDays(1)) > 0) {
            // build stats from days granularity
            LOG.info(localLoggingPrefix + "Calculating TS stats based on day granularity, using a time window "
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
            LOG.info(localLoggingPrefix + "Average TS count per day: {}, frequency: {}", averageCount, frequency);

        } else {
            // build stats from hour granularity
            LOG.info(localLoggingPrefix + "Calculating TS stats based on hour granularity, using a time window "
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
            LOG.info(localLoggingPrefix + "Average TS count per hour: {}, frequency: {}", averageCount, frequency);
        }

        return frequency;
    }

    /* Gets the max average TS count from a query */
    private double getMaxAverageCount(RequestParameters query) throws Exception {
        String localLoggingPrefix = loggingPrefix + "getMaxAverageCount - ";
        Preconditions.checkArgument(query.getRequestParameters().containsKey(AGGREGATES_KEY)
                && query.getRequestParameters().get(AGGREGATES_KEY) instanceof List
                && ((List) query.getRequestParameters().get(AGGREGATES_KEY)).contains("count"),
                "The query must specify the count aggregate.");

        double average = 0d;
        try {
            Iterator<CompletableFuture<ResponseItems<DataPointListItem>>> results =
                    connector.readTsDatapointsProto(query);
            CompletableFuture<ResponseItems<DataPointListItem>> responseItemsFuture;
            ResponseItems<DataPointListItem> responseItems;

            while (results.hasNext()) {
                responseItemsFuture = results.next();
                responseItems = responseItemsFuture.join();
                int tsPointsCounter = 0;

                if (!responseItems.isSuccessful()) {
                    // something went wrong with the request
                    String message = localLoggingPrefix + "Error while iterating through the results from Fusion: "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }

                for (DataPointListItem item : responseItems.getResultsItems()) {
                    LOG.debug(localLoggingPrefix + "Item in results list, Ts id: {}", item.getId());
                    List<TimeseriesPoint> points = TimeseriesParser.parseDataPointListItem(item);
                    LOG.info(localLoggingPrefix + "Number of datapoints in TS list item: {}", points.size());

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
            LOG.error(localLoggingPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
        return average;
    }

    private RequestParameters buildRequestParameters(RequestParameters requestParameters,
                                                     long start,
                                                     long end,
                                                     String loggingPrefix) {
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

}
