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

package com.cognite.beam.io.util.internal;

import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.cognite.v1.timeseries.proto.DataPointInsertionRequest;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;

/**
 * Internal utility class for recording metrics.
 */
public class MetricsUtil {

    /**
     * Adds the recorded api latency to the provided {@link Distribution}.
     *
     * @param responseItems The response payload containing the recorded statistics.
     * @param apiLatency The destination metric for api latency.
     * @return true if the metric was updated successfully. false if the metric couldn't be updated.
     */
    public static boolean recordApiLatency(ResponseItems<?> responseItems,
                                           Distribution apiLatency) {
        boolean metricUpdated = false;
        if (null != responseItems.getResponseBinary().getApiLatency()) {
            apiLatency.update(responseItems.getResponseBinary().getApiLatency());
            metricUpdated = true;
        }
        return metricUpdated;
    }

    /**
     * Adds the recorded api retries to the provided {@link Counter}.
     *
     * @param responseItems The response payload containing the recorded statistics.
     * @param retries The destination metric for api retries.
     * @return true if the metric was updated successfully. false if the metric couldn't be updated.
     */
    public static boolean recordApiRetryCounter(ResponseItems<?> responseItems,
                                                Counter retries) {
        if (responseItems.getResponseBinary().getApiRetryCounter() > 0) {
            retries.inc(responseItems.getResponseBinary().getApiRetryCounter());
        }
        return true;
    }

    /**
     * Adds the recorded api batch size to the provided {@link Distribution}.
     *
     * @param responseItems The response payload containing the recorded statistics.
     * @param batchSize The destination metric for api batch size.
     * @return true if the metric was updated successfully. false if the metric couldn't be updated.
     */
    public static boolean recordApiBatchSize(ResponseItems<?> responseItems,
                                             Distribution batchSize) {
        boolean metricUpdated = false;
        try {
            batchSize.update(responseItems.getResultsItems().size());
            metricUpdated = true;
        } catch (Exception e) {}

        return metricUpdated;
    }

    /**
     * Adds the number of items in the {@link RequestParameters} to the provided {@link Distribution}.
     *
     * @param requestParameters The request payload containing the items to record.
     * @param batchSize The destination metric for api batch size.
     * @return true if the metric was updated successfully. false if the metric couldn't be updated.
     */
    public static boolean recordApiBatchSize(RequestParameters requestParameters,
                                             Distribution batchSize) {
        if (requestParameters.getProtoRequestBody() instanceof DataPointInsertionRequest) {
            batchSize.update(((DataPointInsertionRequest) requestParameters.getProtoRequestBody()).getItemsCount());
        } else {
            batchSize.update(requestParameters.getItems().size());
        }

        return true;
    }

    /**
     * Adds the recorded api job duration to the provided {@link Distribution}.
     *
     * @param responseItems The response payload containing the recorded statistics.
     * @param jobExecutionDuration The destination metric for api job duration latency.
     * @return true if the metric was updated successfully. false if the metric couldn't be updated.
     */
    public static boolean recordApiJobExecutionDuration(ResponseItems<?> responseItems,
                                                        Distribution jobExecutionDuration) {
        boolean metricUpdated = false;
        if (null != responseItems.getResponseBinary().getApiJobDurationMillies()) {
            jobExecutionDuration.update(responseItems.getResponseBinary().getApiJobDurationMillies());
            metricUpdated = true;
        }
        return metricUpdated;
    }

    /**
     * Adds the recorded api job queue duration to the provided {@link Distribution}.
     *
     * @param responseItems The response payload containing the recorded statistics.
     * @param jobQueueDuration The destination metric for api job duration latency.
     * @return true if the metric was updated successfully. false if the metric couldn't be updated.
     */
    public static boolean recordApiJobQueueDuration(ResponseItems<?> responseItems,
                                                    Distribution jobQueueDuration) {
        boolean metricUpdated = false;
        if (null != responseItems.getResponseBinary().getApiJobQueueDurationMillies()) {
            jobQueueDuration.update(responseItems.getResponseBinary().getApiJobQueueDurationMillies());
            metricUpdated = true;
        }
        return metricUpdated;
    }
}
