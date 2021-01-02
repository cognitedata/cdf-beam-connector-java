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

package com.cognite.beam.io.fn.request;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.ResourceType;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.util.TSIterationUtilities;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * This function generates an unbounded stream of {@code RequestParameter} queries.
 */
@DoFn.UnboundedPerElement
public class GenerateReadRequestsUnboundFn extends DoFn<RequestParameters, RequestParameters> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
    private final String loggingPrefix = "Generate read request unbound [" + randomIdString + "] -";

    private static final String TS_START_KEY = "start";
    private static final String TS_END_KEY = "end";
    private static final String GRANULARITY_KEY = "granularity";

    private static final String RAW_START_KEY = "minLastUpdatedTime";
    private static final String RAW_END_KEY = "maxLastUpdatedTime";

    private static final String RESOURCE_LAST_UPDATED_KEY = "lastUpdatedTime";

    private static final List<ResourceType> validResourceTypes = ImmutableList.<ResourceType>builder()
            .add(ResourceType.ASSET)
            .add(ResourceType.EVENT)
            .add(ResourceType.TIMESERIES_HEADER)
            .add(ResourceType.TIMESERIES_DATAPOINTS)
            .add(ResourceType.FILE_HEADER)
            .add(ResourceType.RAW_ROW)
            .add(ResourceType.SEQUENCE_HEADER)
            .build();

    private final ReaderConfig readerConfig;
    private final ResourceType resourceType;

    public GenerateReadRequestsUnboundFn(ReaderConfig readerConfig,
                                         ResourceType resourceType) {
        Preconditions.checkArgument(validResourceTypes.contains(resourceType),
                "Incompatible resource type: " + resourceType.toString());
        this.readerConfig = readerConfig;
        this.resourceType = resourceType;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up read unbound request generator.");
        readerConfig.validate();
    }

    @ProcessElement
    public ProcessContinuation processElement(@Element RequestParameters query,
                                              RestrictionTracker<OffsetRange, Long> tracker,
                                              OutputReceiver<RequestParameters> out,
                                              ManualWatermarkEstimator watermarkEstimator) throws Exception {
        Preconditions.checkArgument(!query.getRequestParameters().containsKey(GRANULARITY_KEY),
                "The datapoints request is an aggregate query. The streaming reader only supports non-aggregate requests.");
        final String batchIdentifierPrefix = "Request batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        final String localLoggingPrefix = loggingPrefix + batchIdentifierPrefix;
        LOG.info(localLoggingPrefix + "Input query: {}", query.toString());
        LOG.info(localLoggingPrefix + "Input restriction {}", tracker.currentRestriction());

        long startRange = tracker.currentRestriction().getFrom();
        long endRange = tracker.currentRestriction().getTo();

        while (startRange < (System.currentTimeMillis() - readerConfig.getPollOffset().get().toMillis())) {
            // Set the query's max end to current time - offset.
            if (endRange > (System.currentTimeMillis() - readerConfig.getPollOffset().get().toMillis())) {
                endRange = (System.currentTimeMillis() - readerConfig.getPollOffset().get().toMillis());
            }

            if (tracker.tryClaim(endRange - 1)) {
                LOG.info(localLoggingPrefix + "Building RequestParameters with start = {} and end = {}", startRange, endRange);
                watermarkEstimator.setWatermark(org.joda.time.Instant.ofEpochMilli(startRange));
                out.outputWithTimestamp(buildRequestParameters(query, startRange, endRange, localLoggingPrefix),
                        org.joda.time.Instant.ofEpochMilli(startRange));
                // Update the start and end range for the next iteration
                startRange = endRange;
                endRange = tracker.currentRestriction().getTo();
            } else {
                LOG.info(localLoggingPrefix + "Stopping work due to checkpointing or splitting.");
                return ProcessContinuation.stop();
            }

            if (startRange >= tracker.currentRestriction().getTo()) {
                LOG.info(localLoggingPrefix + "Completed the request time range. Will stop watching for new datapoints.");
                return ProcessContinuation.stop();
            }

            LOG.info(localLoggingPrefix + "Pausing for {}", readerConfig.getPollInterval().get().toString());
            return ProcessContinuation.resume().withResumeDelay(org.joda.time.Duration.millis(
                    readerConfig.getPollInterval().get().toMillis()));
        }

        LOG.info(localLoggingPrefix + "Pausing for {}", readerConfig.getPollInterval().get().toString());
        return ProcessContinuation.resume().withResumeDelay(org.joda.time.Duration.millis(
                readerConfig.getPollInterval().get().toMillis()));
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Restriction OffsetRange restriction) {
        return Instant.ofEpochMilli(restriction.getFrom());
    }

    @NewWatermarkEstimator
    public ManualWatermarkEstimator<Instant> newWatermarkEstimator(@WatermarkEstimatorState Instant state) {
        return new WatermarkEstimators.Manual(state);
    }


    /*
    Sets the time range for a read request to CDF. The time range is given in milliseconds since epoch with
    <start> being inclusive and <end> being exclusive.
     */
    private RequestParameters buildRequestParameters(RequestParameters requestParameters,
                                                     long start,
                                                     long end,
                                                     String loggingPrefix) {
        Preconditions.checkArgument(start < end, "Trying to build request with start >= end.");
        Preconditions.checkArgument(
                ((Long) requestParameters.getRequestParameters().getOrDefault(TS_START_KEY, 0L)) <= start,
                "Trying to build request with start < original start time.");
        Preconditions.checkArgument(
                ((Long) requestParameters.getRequestParameters().getOrDefault(TS_END_KEY, Long.MAX_VALUE)) >= end,
                "Trying to build request with end > original end time.");
        LOG.debug(loggingPrefix + "Building RequestParameters with start = {} and end = {}", start, end);

        RequestParameters returnValue = null;
        if (resourceType == ResourceType.TIMESERIES_DATAPOINTS) {
            returnValue = requestParameters
                    .withRootParameter(TS_START_KEY, start)
                    .withRootParameter(TS_END_KEY, end);
        } else if (resourceType == ResourceType.RAW_ROW) {
            returnValue = requestParameters
                    .withRootParameter(RAW_START_KEY, start)
                    .withRootParameter(RAW_END_KEY, (end - 1l));
        } else {
            returnValue = requestParameters
                    .withFilterParameter(RESOURCE_LAST_UPDATED_KEY, ImmutableMap.of(
                            "min", start,
                            "max", (end - 1l)
                    ));
        }

        return returnValue;
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element RequestParameters requestParameters) throws Exception {
        long startTimestamp = 0L;
        long endTimestamp = Long.MAX_VALUE;

        if (resourceType == ResourceType.TIMESERIES_DATAPOINTS) {
            // check if we have an end time
            LOG.debug(loggingPrefix + "GetInitialRestriction, get end time from request attribute {}: [{}]",
                    TS_END_KEY,
                    requestParameters.getRequestParameters().getOrDefault(TS_END_KEY, "null"));
            Optional<Long> requestEndTime = TSIterationUtilities.getEndAsMillis(requestParameters);
            if (requestEndTime.isPresent()) {
                endTimestamp = requestEndTime.get();
            }

            // check for start time
            LOG.debug(loggingPrefix + "GetInitialRestriction, get start time from request attribute {}: [{}]",
                    TS_START_KEY,
                    requestParameters.getRequestParameters().getOrDefault(TS_START_KEY, "null"));
            Optional<Long> requestStartTime = TSIterationUtilities.getStartAsMillis(requestParameters);
            if (requestStartTime.isPresent()) {
                startTimestamp = requestStartTime.get();
            }
        } else if (resourceType == ResourceType.RAW_ROW) {
            // check if we have an end time
            LOG.debug(loggingPrefix + "GetInitialRestriction, get end time from request attribute {}: [{}]",
                    RAW_END_KEY,
                    requestParameters.getRequestParameters().getOrDefault(RAW_END_KEY, "null"));
            if (requestParameters.getRequestParameters().containsKey(RAW_END_KEY)
                        && requestParameters.getRequestParameters().get(RAW_END_KEY) instanceof Long) {
                endTimestamp = (Long) requestParameters.getRequestParameters().get(RAW_END_KEY);
            }

            // check for start time
            LOG.debug(loggingPrefix + "GetInitialRestriction, get start time from request attribute {}: [{}]",
                    RAW_START_KEY,
                    requestParameters.getRequestParameters().getOrDefault(RAW_START_KEY, "null"));
            if (requestParameters.getRequestParameters().containsKey(RAW_START_KEY)
                    && requestParameters.getRequestParameters().get(RAW_START_KEY) instanceof Long) {
                startTimestamp = (Long) requestParameters.getRequestParameters().get(RAW_START_KEY);
            }

        } else {
            // check if we have a time restriction set
            LOG.debug(loggingPrefix + "GetInitialRestriction, get min/max last updated time from request attribute {}: [{}]",
                    RESOURCE_LAST_UPDATED_KEY,
                    requestParameters.getFilterParameters().getOrDefault(RESOURCE_LAST_UPDATED_KEY, "null"));
            if (requestParameters.getFilterParameters().containsKey(RESOURCE_LAST_UPDATED_KEY)
                    && requestParameters.getFilterParameters().get(RESOURCE_LAST_UPDATED_KEY) instanceof Map) {
                Map<?, ?> lastUpdatedTimeMap = (Map) requestParameters.getFilterParameters().get(RESOURCE_LAST_UPDATED_KEY);
                // check for start time
                if (lastUpdatedTimeMap.containsKey("min")
                        && lastUpdatedTimeMap.get("min") instanceof Long) {
                    startTimestamp = (Long) lastUpdatedTimeMap.get("min");
                }

                // check for end time
                if (lastUpdatedTimeMap.containsKey("max")
                        && lastUpdatedTimeMap.get("max") instanceof Long) {
                    endTimestamp = (Long) lastUpdatedTimeMap.get("max");
                }
            }
        }

        if (startTimestamp >= endTimestamp) {
            String message = "Trying to build an initial restriction with start >= end. Start = ["
                    + startTimestamp + "], end = [" + endTimestamp + "]";
            LOG.error(message);
            throw new Exception(message);
        }

        return new OffsetRange(startTimestamp, endTimestamp);
    }
}
