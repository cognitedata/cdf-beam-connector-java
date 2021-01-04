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

package com.cognite.beam.io.fn.delete;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.RawRow;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deletes rows from CDF.Raw.
 *
 * This function deletes rows from a raw table.
 *
 * The input collection of rows must all belong to the same db and table. That is, each {@code Iterable<RawRow>}
 * must contain rows for the same raw destination table.
 */
public class DeleteRawRowFn extends DoFn<Iterable<RawRow>, RawRow> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    final Counter apiRetryCounter = Metrics.counter("cognite", "apiRetries");

    private final ConnectorServiceV1 connector;
    private final WriterConfig writerConfig;
    private ConnectorServiceV1.ItemWriter itemWriterDelete;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public DeleteRawRowFn(Hints hints, WriterConfig writerConfig,
                          PCollectionView<List<ProjectConfig>> projectConfigView) {
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
        LOG.info("Setting up DeleteRawRowFn.");
        LOG.debug("Opening writer");
        itemWriterDelete = connector.deleteRawRows();
    }

    @ProcessElement
    public void processElement(@Element Iterable<RawRow> element, OutputReceiver<RawRow> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchIdentifier = RandomStringUtils.randomAlphanumeric(6);
        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchIdentifier + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // naive de-duplication based on ids
        Map<String, RawRow> deleteMap = new HashMap<>(10000);

        // all rows must reference the same db and table
        String dbName = "";
        String tableName = "";

        for (RawRow value : element) {
            if (value.getDbName().isEmpty() || value.getTableName().isEmpty() || value.getKey().isEmpty()) {
                String message = "Batch identifier: " + batchIdentifier
                        + "- Row must specify dbName, tableName and row key: " + value.toString();
                LOG.error(message);
                throw new Exception(message);
            }
            if (dbName.isEmpty() || tableName.isEmpty()) {
                // Sampling the first element's db and table name
                dbName = value.getDbName();
                tableName = value.getTableName();
            }
            if (!dbName.equals(value.getDbName()) || !tableName.equals(value.getTableName())) {
                String message = "Batch identifier: " + batchIdentifier
                        + "- All rows in the same batch must belong to the same table." + System.lineSeparator()
                        + "This error may be caused by a bug in the SDK.";
                LOG.error(message);
                throw new Exception(message);
            }
            deleteMap.put(value.getKey(), value);
        }
        LOG.info("Batch identifier: " + batchIdentifier + "- Received items to delete:{}", deleteMap.size());

        // Should not happen--but need to guard against empty input
        if (deleteMap.isEmpty()) {
            LOG.warn("Batch identifier: " + batchIdentifier + "- No input elements received.");
            return;
        }

        // build initial request object
        RequestParameters request = RequestParameters.create()
                .withItems(toRequestDeleteItems(deleteMap.values()))
                .withRootParameter("dbName", dbName)
                .withRootParameter("tableName", tableName)
                .withProjectConfig(projectConfig);
        LOG.debug("Batch identifier: " + batchIdentifier + "- Built delete request for {} elements", deleteMap.size());

        ResponseItems<String> responseItems = itemWriterDelete.writeItems(request);
        LOG.info("Batch identifier: " + batchIdentifier + "- Delete request sent. Result returned: {}"
                , responseItems.isSuccessful());

        if (responseItems.isSuccessful()) {
            if (writerConfig.isMetricsEnabled()) {
                MetricsUtil.recordApiRetryCounter(responseItems, apiRetryCounter);
                MetricsUtil.recordApiLatency(responseItems, apiLatency);
            }
        } else {
            String message = "Batch identifier: " + batchIdentifier
                    + "- Failed to delete rows in raw. Writer returned "
                    + responseItems.getResponseBodyAsString();
            LOG.error(message);
            throw new Exception(message);
        }

        // output the deleted items (excluding duplicates)
        for (RawRow outputElement : deleteMap.values()) {
            outputReceiver.output(outputElement);
        }
    }

    private List<Map<String, Object>> toRequestDeleteItems(Iterable<RawRow> input) {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (RawRow element : input) {
            listBuilder.add(this.toRequestDeleteItem(element));
        }
        return listBuilder.build();
    }

    private Map<String, Object> toRequestDeleteItem(RawRow element) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        mapBuilder.put("key", element.getKey());

        return mapBuilder.build();
    }
}
