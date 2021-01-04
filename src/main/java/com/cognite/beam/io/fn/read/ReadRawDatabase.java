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
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * This function reads all database names from raw.
 *
 *
 */
public class ReadRawDatabase extends DoFn<ProjectConfig, String> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String parseErrorDefaultPrefix = "Parsing error. Unable to parse result item. ";

    private final Hints hints;
    private final String appIdentifier;
    private final String sessionIdentifier;

    public ReadRawDatabase(Hints hints, String appIdentifier, String sessionIdentifier) {
        this.hints = hints;
        this.appIdentifier = appIdentifier;
        this.sessionIdentifier = sessionIdentifier;
    }

    @Setup
    public void setup() {
        LOG.debug("Setting up ReadRawDatabase.");
        LOG.debug("Validating the hints");
        hints.validate();
    }

    @ProcessElement
    public void processElement(@Element ProjectConfig config, OutputReceiver<String> outputReceiver) throws Exception {
        final String batchIdentifier = RandomStringUtils.randomAlphanumeric(6);
        final String loggingPrefix = "ReadRawDatabase - " + batchIdentifier + " - ";
        Preconditions.checkArgument(config.isConfigured(),
                loggingPrefix + "ProjectConfig is not configured: " + config.toString());

        LOG.debug(loggingPrefix + "Sending query to the Cognite api: {}",
                "List database names");

        ConnectorServiceV1 connector = ConnectorServiceV1.builder()
                .setMaxRetries(hints.getMaxRetries())
                .setAppIdentifier(appIdentifier)
                .setSessionIdentifier(sessionIdentifier)
                .build();

        try {
            Iterator<CompletableFuture<ResponseItems<String>>> results = connector.readRawDbNames(config);
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

                    // A result item must contain a name node
                    if (root.path("name").isTextual()) {
                        outputReceiver.output(root.get("name").textValue());
                    } else {
                        throw new Exception(loggingPrefix + parseErrorDefaultPrefix
                                + "Unable to parse attribute: name. Item exerpt: "
                                + item
                                .substring(0, Math.min(item.length() - 1, CogniteIO.MAX_LOG_ELEMENT_LENGTH)));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(loggingPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
