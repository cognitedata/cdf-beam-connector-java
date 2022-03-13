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

package com.cognite.beam.io.fn;

import com.cognite.beam.io.config.ConfigBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.config.ClientConfig;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

/**
 * Base class for DoFns interacting with Cognite Data Fusion.
 *
 * This class will handle the instantiation of the {@link CogniteClient}.
 */
public abstract class IOBaseFn<T, R> extends DoFn<T, R> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected final Distribution apiLatency = Metrics.distribution("cognite", "apiLatency");
    protected final Distribution apiBatchSize = Metrics.distribution("cognite", "apiBatchSize");

    protected final Hints hints;
    private transient CogniteClient client = null; // Mark transient to prevent serialization

    public IOBaseFn(Hints hints) {
        Preconditions.checkNotNull(hints, "Hints cannot be null");
        this.hints = hints;
    }

    /**
     * Returns the {@link CogniteClient}. Will instantiate a new client and cache it for further access.
     * @param projectConfig The {@link ProjectConfig} to configure auth credentials.
     * @param configBase Carries the app and session identifiers.
     * @return The {@link CogniteClient}.
     * @throws Exception
     */
    protected CogniteClient getClient(ProjectConfig projectConfig, ConfigBase configBase) throws Exception {
        if (null == client) {
            // Client is not configured. Either because this class has been serialized or it is the first method call

            if (configHasClientCredentials(projectConfig)) {
                // client credentials take precedence over api key
                client = CogniteClient.ofClientCredentials(projectConfig.getClientId().get(),
                        projectConfig.getClientSecret().get(), new URL(projectConfig.getTokenUrl().get()))
                        .withBaseUrl(projectConfig.getHost().get())
                        .withClientConfig(ClientConfig.create()
                                .withMaxRetries(hints.getMaxRetries().get())
                                .withAppIdentifier(configBase.getAppIdentifier())
                                .withSessionIdentifier(configBase.getSessionIdentifier()));
                // set custom auth scopes if they are configured in the ProjectConfig object.
                if (null != projectConfig.getAuthScopes() && projectConfig.getAuthScopes().isAccessible()) {
                    client = client.withScopes(projectConfig.getAuthScopes().get());
                }
            } else if (configHasApiKey(projectConfig)) {
                client = CogniteClient.ofKey(projectConfig.getApiKey().get())
                        .withBaseUrl(projectConfig.getHost().get())
                        .withClientConfig(ClientConfig.create()
                                .withMaxRetries(hints.getMaxRetries().get())
                                .withAppIdentifier(configBase.getAppIdentifier())
                                .withSessionIdentifier(configBase.getSessionIdentifier()));
            } else {
                throw new Exception("Neither client credentials nor API key cannot be accessed. Please check that it is configured.");
            }


            if (null != projectConfig.getProject() && projectConfig.getProject().isAccessible()) {
                client = client.withProject(projectConfig.getProject().get());
            }
        }

        return client;
    }

    /*
    Returns true if the project config contains accessible client credentials.
     */
    private boolean configHasClientCredentials(ProjectConfig projectConfig) {
        return (null != projectConfig.getClientId() && projectConfig.getClientId().isAccessible()
                && null != projectConfig.getClientSecret() && projectConfig.getClientSecret().isAccessible()
                && null != projectConfig.getTokenUrl() && projectConfig.getTokenUrl().isAccessible()
        );
    }

    /*
    Returns true if the project config contains an accessible api key.
     */
    private boolean configHasApiKey(ProjectConfig projectConfig) {
        return (null != projectConfig.getApiKey() && projectConfig.getApiKey().isAccessible());
    }
}
