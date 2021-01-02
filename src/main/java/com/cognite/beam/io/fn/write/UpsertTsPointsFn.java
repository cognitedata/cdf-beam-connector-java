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
import com.cognite.beam.io.dto.Item;
import com.cognite.beam.io.dto.TimeseriesMetadata;
import com.cognite.beam.io.dto.TimeseriesPointPost;
import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.cognite.beam.io.servicesV1.parser.ItemParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Writes time series data points to CDF.Clean.
 *
 * This function writes TS data points to Cognite. In case the datapoint exists from before (based on the externalId/id
 * and timestamp), the existing value will be overwritten.
 *
 * In case the TS metadata / header does not exists, this module will create a minimum header. The created header supports
 * both numeric and string values, but only non-step (i.e. "isStep = false") time series. In addition, only externalId
 * is supported. That is, if you try to post TS datapoints to an internalId that does not exist the write will fail.
 *
 * In any case, we recommend that you create the TS header before starting to write TS data points to it.
 *
 */
public class UpsertTsPointsFn extends DoFn<Iterable<TimeseriesPointPost>, TimeseriesPointPost> {
    private static final TimeseriesMetadata DEFAULT_TS_METADATA = TimeseriesMetadata.newBuilder()
            .setExternalId(StringValue.of("beam_writer_default"))
            .setName(StringValue.of("beam_writer_default"))
            .setDescription(StringValue.of("Default TS metadata created by the Beam writer."))
            .setIsStep(false)
            .setIsString(false)
            .build();

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    private final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    private final ConnectorServiceV1 connector;
    private final WriterConfig writerConfig;
    private ConnectorServiceV1.ItemWriter tsPointsWriterInsert;
    private ConnectorServiceV1.ItemWriter tsHeaderWriterInsert;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public UpsertTsPointsFn(Hints hints, WriterConfig writerConfig,
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
    }

    @Setup
    public void setup() {
        LOG.info("Setting up UpsertTsPointsFn.");
        LOG.debug("Opening writer");
        tsPointsWriterInsert = connector.writeTsDatapoints();
        tsHeaderWriterInsert = connector.writeTsHeaders();
    }

    @ProcessElement
    public void processElement(@Element Iterable<TimeseriesPointPost> element,
                               OutputReceiver<TimeseriesPointPost> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "Batch identifier: " + RandomStringUtils.randomAlphanumeric(6) + " - ";

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchLogPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // Check all elements for id / externalId + naive deduplication
        Map<String, Map<Long, TimeseriesPointPost>> externalIdInsertMap = new HashMap<>(1000);
        Map<Long, Map<Long, TimeseriesPointPost>> internalIdInsertMap = new HashMap<>(1000);
        //Map<String, Map<Long, TimeseriesPointPost>> missingMap = new HashMap<>(10);
        List<TimeseriesPointPost> missingList = new ArrayList<>(5000);

        int tsPointCounter = 0;

        for (TimeseriesPointPost value : element) {
            if (value.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.IDTYPE_NOT_SET) {
                String message = batchLogPrefix + "Neither externalId nor id found. "
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
            tsPointCounter++;
        }
        LOG.info(batchLogPrefix + "Received {} TS ids to write datapoints to.",
                externalIdInsertMap.size() + internalIdInsertMap.size());
        LOG.info(batchLogPrefix + "Received {} TS points to write.", tsPointCounter);

        // Should not happen--but need to guard against empty input
        if (externalIdInsertMap.isEmpty() && internalIdInsertMap.isEmpty()) {
            LOG.warn(batchLogPrefix + "No input elements received.");
            return;
        }

        // build initial request object
        RequestParameters pointsPostRequest = RequestParameters.create()
                .withItems(toRequestInsertItems(externalIdInsertMap, internalIdInsertMap))
                .withProjectConfig(projectConfig);
        LOG.debug(batchLogPrefix + "Built write request for {} TS and {} data points",
                externalIdInsertMap.size() + internalIdInsertMap.size(), tsPointCounter);

        ResponseItems<String> responseItems = tsPointsWriterInsert.writeItems(pointsPostRequest);
        LOG.info(batchLogPrefix + "Insert elements request sent. Response returned: {}", responseItems.isSuccessful());

        if (responseItems.isSuccessful()) {
            if (writerConfig.isMetricsEnabled()) {
                MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                MetricsUtil.recordApiLatency(responseItems, apiLatency);
            }
            LOG.info(batchLogPrefix + "Writing items completed successfully. Inserted {} TS and {} data points",
                    externalIdInsertMap.size() + internalIdInsertMap.size(), tsPointCounter);
        } else {
            LOG.warn(batchLogPrefix + "Write items failed. Most likely due to missing TS metadata. "
                    + "Will retry the request without the items referring to missing TS metadata.");
            LOG.debug(batchLogPrefix + "Write items failed. Writer returned {}", responseItems.getResponseBodyAsString());

            // check for duplicates. Duplicates should not happen, so fire off an exception.
            if (!responseItems.getDuplicateItems().isEmpty()) {
                String message = batchLogPrefix + "Duplicates reported: "
                        + responseItems.getDuplicateItems().size();
                LOG.error(message);
                throw new Exception(message);
            }

            // Get the missing items and send them to a secondary output (uncommited items).
            List<Item> missingItems = ItemParser.parseItems(responseItems.getMissingItems());
            LOG.debug(batchLogPrefix + "Number of missing entries reported by CDF: {}", missingItems.size());

            // check if the missing items are based on internal id--not supported
            for (Item item : missingItems) {
                if (item.getIdTypeCase() != Item.IdTypeCase.EXTERNAL_ID) {
                    String message = batchLogPrefix + "Datapoint with internal id refers to a non-existing time series. "
                            + "Item specification: " + item.toString();
                    LOG.error(message);
                    throw new Exception(message);
                }
            }
            LOG.debug(batchLogPrefix + "All missing items are based on externalId");

            // Sample a data point from each missing item and create a default TS header for them.
            for (Item value : missingItems) {
                missingList.add(externalIdInsertMap.get(value.getExternalId()).values().iterator().next());
            }
            LOG.info(batchLogPrefix + "Building default TS header for {} \"missing\" time series.", missingList.size());
            writeTsHeaderForPoints(missingList, projectConfig, batchLogPrefix);

            // New write with the TS points.
            LOG.info(batchLogPrefix + "Retrying insert TS data points with {} target time series",
                    internalIdInsertMap.size() + externalIdInsertMap.size());

            responseItems = tsPointsWriterInsert.writeItems(pointsPostRequest);
            if (responseItems.isSuccessful()) {
                if (writerConfig.isMetricsEnabled()) {
                    MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                    MetricsUtil.recordApiLatency(responseItems, apiLatency);
                }
            }
            LOG.info(batchLogPrefix + "Insert elements request sent. Response returned: {}", responseItems.isSuccessful());
        }

        if (!responseItems.isSuccessful()) {
            String message = batchLogPrefix + "Failed to write data points. The Cognite api returned: "
                    + responseItems.getResponseBodyAsString();
            LOG.error(message);
            throw new Exception(message);
        }

        // output the committed items (excluding duplicates)
        externalIdInsertMap.forEach((String extId, Map<Long, TimeseriesPointPost> tsMap) ->
            tsMap.forEach((Long key, TimeseriesPointPost datapoint) -> outputReceiver.output(datapoint))
        );
        internalIdInsertMap.forEach((Long intId, Map<Long, TimeseriesPointPost> tsMap) ->
                tsMap.forEach((Long key, TimeseriesPointPost datapoint) -> outputReceiver.output(datapoint))
        );
    }

    private List<Map<String, Object>> toRequestInsertItems(Map<String, Map<Long, TimeseriesPointPost>> externalIdInsertMap,
                                                           Map<Long, Map<Long, TimeseriesPointPost>> internalIdInsertMap) {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Map.Entry<String, Map<Long, TimeseriesPointPost>> element : externalIdInsertMap.entrySet()) {
            listBuilder.add(this.toRequestInsertItem(element.getValue().values()));
        }
        for (Map.Entry<Long, Map<Long, TimeseriesPointPost>> element : internalIdInsertMap.entrySet()) {
            listBuilder.add(this.toRequestInsertItem(element.getValue().values()));
        }

        return listBuilder.build();
    }

    /**
     * All data points in the input collection must belong to the same time series (externalId / id).
     */
    private Map<String, Object> toRequestInsertItem(Iterable<TimeseriesPointPost> elements) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Map<String, Object>> pointsBuilder = ImmutableList.builder();
        TimeseriesPointPost.IdTypeCase idType = TimeseriesPointPost.IdTypeCase.IDTYPE_NOT_SET;
        TimeseriesPointPost.ValueTypeCase valueType = TimeseriesPointPost.ValueTypeCase.VALUETYPE_NOT_SET;
        String externalId = "";
        long internalId = 0;

        for (TimeseriesPointPost element : elements) {
            if (idType == TimeseriesPointPost.IdTypeCase.IDTYPE_NOT_SET) {
                // First element in the iteration, set id, idType and valueType.
                idType = element.getIdTypeCase();
                valueType = element.getValueTypeCase();
                if (idType == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID) {
                    externalId = element.getExternalId();
                    mapBuilder.put("externalId", externalId);
                } else {
                    internalId = element.getId();
                    mapBuilder.put("id", internalId);
                }
            }

            if (valueType == TimeseriesPointPost.ValueTypeCase.VALUE_NUM) {
                pointsBuilder.add(ImmutableMap.of("timestamp", element.getTimestamp(),
                        "value", element.getValueNum()));
            } else {
                pointsBuilder.add(ImmutableMap.of("timestamp", element.getTimestamp(),
                        "value", element.getValueString()));
            }
        }
        mapBuilder.put("datapoints", pointsBuilder.build());
        return mapBuilder.build();
    }

    private void writeTsHeaderForPoints(List<TimeseriesPointPost> points, ProjectConfig config,
                                        String batchLogPrefix) throws Exception {
        int counter = 0;
        Map<String, Map<String, Object>> insertMap = new HashMap<>(1000);
        for (TimeseriesPointPost point : points) {
            insertMap.put(point.getExternalId(), generateDefaultTsMetadataInsertItem(point));
            counter++;
            if (counter >= 1000) {
                writeTsHeaders(insertMap, config, batchLogPrefix);
                insertMap.clear();
                counter = 0;
            }
        }

        if (!insertMap.isEmpty()) {
            writeTsHeaders(insertMap, config, batchLogPrefix);
        }
    }

    private void writeTsHeaders(Map<String, Map<String, Object>> insertMap, ProjectConfig config,
                                String batchLogPrefix) throws Exception {
        Preconditions.checkArgument(!insertMap.isEmpty(), batchLogPrefix
                + "The insert item list cannot be empty.");
        int numRetries = 3;
        boolean isWriteSuccessful = false;
        List<Map<String, Object>> insertItems = new ArrayList<>(1000);
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < numRetries && !isWriteSuccessful; i++,
                Thread.sleep(Math.min(32000L, (500L * (long) Math.exp(i)) + random.nextLong(1000)))) {
            insertItems.clear();
            insertItems.addAll(insertMap.values());

            // build request object
            RequestParameters postTsHeaders = RequestParameters.create()
                    .withItems(insertItems)
                    .withProjectConfig(config);
            LOG.debug(batchLogPrefix + "Built upsert elements request for {} elements", insertItems.size());

            // post write request and monitor for duplicates
            ResponseItems<String> responseItems = tsHeaderWriterInsert.writeItems(postTsHeaders);
            isWriteSuccessful = responseItems.isSuccessful();
            LOG.info(batchLogPrefix + "Write TS metadata request sent. Result returned: {}", isWriteSuccessful);

            if (!isWriteSuccessful) {
                // we have duplicates. Remove them and try again.
                LOG.warn(batchLogPrefix + "Write TS metadata failed. Most likely due to duplicates. "
                        + "Will remove them and retry.");
                List<Item> duplicates = ItemParser.parseItems(responseItems.getDuplicateItems());
                LOG.debug(batchLogPrefix + "Number of duplicate TS header entries reported by CDF: {}", duplicates.size());

                // Remove the duplicates from the insert list
                for (Item value : duplicates) {
                    insertItems.remove(value.getExternalId());
                }
            }

        }
        if (!isWriteSuccessful) {
            String message = batchLogPrefix + "Writing default TS metadata for TS data points failed. ";
            LOG.error(message);
            throw new Exception(message);
        }
    }

    private Map<String, Object> generateDefaultTsMetadataInsertItem(TimeseriesPointPost datapoint) {
        Preconditions.checkArgument(datapoint.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID,
                "Time series data point is not based on externalId: " + datapoint.toString());

        return ImmutableMap.<String, Object>builder()
                .put("externalId", datapoint.getExternalId())
                .put("legacyName", datapoint.getExternalId())
                .put("name", DEFAULT_TS_METADATA.getName().getValue())
                .put("description", DEFAULT_TS_METADATA.getDescription().getValue())
                .put("isStep", datapoint.getIsStep())
                .put("isString", datapoint.getValueTypeCase() == TimeseriesPointPost.ValueTypeCase.VALUE_STRING)
                .build();
    }
}
