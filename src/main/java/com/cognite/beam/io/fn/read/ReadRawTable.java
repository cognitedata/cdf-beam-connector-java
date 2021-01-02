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

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.dto.RawTable;
import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This function reads all table names from a given raw database.
 *
 *
 */
public class ReadRawTable extends DoFn<String, RawTable> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String parseErrorDefaultPrefix = "Parsing error. Unable to parse result item. ";

    private final ConnectorServiceV1 connector;

    private final PCollectionView<List<ProjectConfig>> projectConfigView;

    public ReadRawTable(Hints hints, String appIdentifier, String sessionIdentifier,
                        PCollectionView<List<ProjectConfig>> projectConfigView) {
        this.connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(appIdentifier)
                .setSessionIdentifier(sessionIdentifier)
                .build();
        this.projectConfigView = projectConfigView;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up ReadRawTable.");
    }

    @ProcessElement
    public void processElement(@Element String dbName, OutputReceiver<RawTable> outputReceiver,
                               ProcessContext context) throws Exception {
        Preconditions.checkNotNull(dbName, "Database name cannot be null.");
        Preconditions.checkArgument(!dbName.isEmpty(), "Database name cannot be empty.");
        final String batchIdentifier = RandomStringUtils.randomAlphanumeric(6);
        final String loggingPrefix = "ReadRawTable - " + batchIdentifier + " - ";
        LOG.debug(loggingPrefix + "Sending query to the Cognite api. List tables for: {}",
                dbName);

        // Identify the project config to use
        ProjectConfig projectConfig = ProjectConfig.create();
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            LOG.error(loggingPrefix + "Unable to read ProjectConfig from the side input.");
        }

        Preconditions.checkState(projectConfig.isConfigured(),
                loggingPrefix + "ProjectConfig is not configured. ProjectConfig: "
                + projectConfig.toString());

        try {
            Iterator<CompletableFuture<ResponseItems<String>>> results = connector.readRawTableNames(dbName, projectConfig);
            ResponseItems<String> responseItems;

            while (results.hasNext()) {
                responseItems = results.next().join();
                if (!responseItems.isSuccessful()) {
                    // something went wrong with the request
                    String message = loggingPrefix + "Error while iterating through the results from Fusion: "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }

                for (String item : responseItems.getResultsItems()) {
                    JsonNode root = objectMapper.readTree(item);

                    RawTable.Builder rawTableBuilder = RawTable.newBuilder()
                            .setDbName(dbName);

                    // A result item must contain a name node
                    if (root.path("name").isTextual()) {
                        rawTableBuilder.setTableName(root.get("name").textValue());
                    } else {
                        throw new Exception(loggingPrefix + parseErrorDefaultPrefix
                                + "Unable to parse attribute: name. Item exerpt: "
                                + item
                                .substring(0, Math.min(item.length() - 1, CogniteIO.MAX_LOG_ELEMENT_LENGTH)));
                    }
                    outputReceiver.output(rawTableBuilder.build());
                }
            }
        } catch (Exception e) {
            LOG.error(loggingPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
