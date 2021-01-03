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
import com.cognite.client.CogniteClient;
import com.cognite.client.config.ClientConfig;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for writing items to CDF.Clean. Specific resource types (Event, Asset, etc.) extend this class with
 * custom parsing logic.
 */
public abstract class UpsertItemBaseNewFn<T> extends DoFn<Iterable<T>, T> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");

    private transient CogniteClient client = null; // Mark transient to prevent serialization
    final Hints hints;
    final WriterConfig writerConfig;
    final ProjectConfig projectConfig;

    public UpsertItemBaseNewFn(Hints hints, WriterConfig writerConfig,
                               ProjectConfig projectConfig) {
        Preconditions.checkNotNull(writerConfig, "Writer config cannot be null.");
        Preconditions.checkNotNull(hints, "Hints cannot be null");
        Preconditions.checkNotNull(projectConfig, "Project config cannot be null");

        this.hints = hints;
        this.projectConfig = projectConfig;
        this.writerConfig = writerConfig;
    }

    /**
     * Returns the {@link CogniteClient}. Will instantiate a new client and cache it for further access.
     * @return The {@link CogniteClient}.
     * @throws Exception
     */
    protected CogniteClient getClient(ProjectConfig projectConfig) throws Exception {
        if (null == client) {
            // Client is not configured. Either because this class has been serialized or it is the first method call
            Preconditions.checkState(null != projectConfig.getApiKey() && projectConfig.getApiKey().isAccessible(),
                    "API key cannot be accessed. Please check that it is configured.");
            client = CogniteClient.ofKey(projectConfig.getApiKey().get())
                    .withBaseUrl(projectConfig.getHost().get())
                    .withClientConfig(ClientConfig.create()
                            .withMaxRetries(hints.getMaxRetries().get())
                            .withAppIdentifier(writerConfig.getAppIdentifier())
                            .withSessionIdentifier(writerConfig.getSessionIdentifier()));

            if (null != projectConfig.getProject() && projectConfig.getProject().isAccessible()) {
                client = client.withProject(projectConfig.getProject().get());
            }
        }

        return client;
    }

    @Setup
    public void setup() {
    }


}
