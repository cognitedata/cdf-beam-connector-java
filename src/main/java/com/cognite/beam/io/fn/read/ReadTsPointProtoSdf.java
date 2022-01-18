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
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.CogniteClient;
import com.cognite.client.dto.TimeseriesPoint;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.util.TSIterationUtilities;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.values.PCollectionView;
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
public class ReadTsPointProtoSdf extends IOBaseFn<RequestParameters, List<TimeseriesPoint>> {
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

    final ReaderConfig readerConfig;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    public ReadTsPointProtoSdf(Hints hints,
                               ReaderConfig readerConfig,
                               PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        this.readerConfig = readerConfig;
        this.projectConfigView = projectConfigView;
    }

    @ProcessElement
    public void processElement(@Element RequestParameters query,
                               RestrictionTracker<OffsetRange, Long> tracker,
                               OutputReceiver<List<TimeseriesPoint>> outputReceiver,
                               ManualWatermarkEstimator watermarkEstimator,
                               ProcessContext context) throws Exception {
        final String batchIdentifierPrefix = "Request batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        final String localLoggingPrefix = loggingPrefix + batchIdentifierPrefix;
        final Instant batchStartInstant = Instant.now();
        LOG.info(localLoggingPrefix + "Streaming mode: {}", readerConfig.isStreamingEnabled());
        LOG.debug(localLoggingPrefix + "Input query: {}", query.toString());
        LOG.info(localLoggingPrefix + "Reading TS based on input restriction {}", tracker.currentRestriction());

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = localLoggingPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        long startRange = tracker.currentRestriction().getFrom();
        long endRange = tracker.currentRestriction().getTo();
        LOG.info(localLoggingPrefix + "Start of range: [{}], end of range: [{}]",
                Instant.ofEpochMilli(startRange).toString(),
                Instant.ofEpochMilli(endRange).toString());

        if (readerConfig.isStreamingEnabled()) {
            watermarkEstimator.setWatermark(org.joda.time.Instant.ofEpochMilli(startRange));
        }

        // Check if this instance can process this item based on the current restriction
        if (!tracker.tryClaim(endRange - 1)) {
            return;
        }
        // Build query so it conforms with the current restriction
        RequestParameters queryRestricted = buildRequestParameters(query, startRange, endRange, localLoggingPrefix);
        try {
            Iterator<List<TimeseriesPoint>> resultsIterator = getClient(projectConfig, readerConfig)
                    .timeseries()
                    .dataPoints()
                    .retrieve(queryRestricted.getRequest());

            Instant pageStartInstant = Instant.now();
            int totalNoItems = 0;
            while (resultsIterator.hasNext()) {
                List<TimeseriesPoint> results = resultsIterator.next();
                if (readerConfig.isMetricsEnabled()) {
                    apiBatchSize.update(results.size());
                    apiLatency.update(Duration.between(pageStartInstant, Instant.now()).toMillis());
                }
                if (readerConfig.isStreamingEnabled()) {
                    // output with timestamps in streaming mode--need that for windowing
                    long minTimestampMs = results.stream()
                            .mapToLong(point -> point.getTimestamp())
                            .min()
                            .orElse(1L);

                    outputReceiver.outputWithTimestamp(results, org.joda.time.Instant.ofEpochMilli(minTimestampMs));
                } else {
                    // no timestamping in batch mode--just leads to lots of complications
                    outputReceiver.output(results);
                }

                totalNoItems += results.size();
                pageStartInstant = Instant.now();
            }

            LOG.info(localLoggingPrefix + "Retrieved {} items in {}}.",
                    totalNoItems,
                    Duration.between(batchStartInstant, Instant.now()).toString());
        } catch (Exception e) {
            LOG.error(localLoggingPrefix + "Error reading results from the Cognite connector.", e);
            LOG.warn(localLoggingPrefix + "Failed request: {}", query.getRequestParametersAsJson());
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
        Optional<Long> requestEndTime = TSIterationUtilities.getEndAsMillis(requestParameters.getRequest());
        if (requestEndTime.isPresent()) {
            endTimestamp = requestEndTime.get();
        }

        // check for start time
        LOG.debug(loggingPrefix + "GetInitialRestriction, get start time from request attribute {}: [{}]",
                START_KEY,
                requestParameters.getRequestParameters().get(START_KEY));
        Optional<Long> requestStartTime = TSIterationUtilities.getStartAsMillis(requestParameters.getRequest());
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
                                 OutputReceiver<OffsetRange> out,
                                 ProcessContext context) throws Exception {
        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

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
            double maxFrequency = getMaxFrequency(requestParameters, offsetRange, getClient(projectConfig, readerConfig));
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
                                   OffsetRange offsetRange,
                                   CogniteClient client) throws Exception {
        long from = offsetRange.getFrom();
        long to = offsetRange.getTo();

        return client.timeseries()
                .dataPoints()
                .getMaxFrequency(requestParameters.getRequest(), Instant.ofEpochMilli(from), Instant.ofEpochMilli(to));

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
