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

package com.cognite.beam.io.transform.internal;

import com.cognite.beam.io.config.GcpSecretConfig;
import com.cognite.beam.io.config.ProjectConfig;
import com.google.auto.value.AutoValue;
import com.google.cloud.secretmanager.v1beta1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1beta1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1beta1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1beta1.SecretVersionName;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Utility transform for building project config based on:
 * 1. Input parameters
 * 2. File reference
 * 3. GCP secrets manager
 *
 * The auth config can be based on OpenID Connect client credentials or api key.
 * OpenID Connect will take precedence over api key.
 *
 */
@AutoValue
public abstract class BuildProjectConfig extends PTransform<PBegin, PCollection<ProjectConfig>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static BuildProjectConfig.Builder builder() {
        return new AutoValue_BuildProjectConfig.Builder()
                .setProjectConfigFile(ValueProvider.StaticValueProvider.of("."))
                .setProjectConfigParameters(ProjectConfig.create());
    }

    public static BuildProjectConfig create() {
        return builder().build();
    }
    public abstract BuildProjectConfig.Builder toBuilder();

    abstract ValueProvider<String> getProjectConfigFile();
    abstract ProjectConfig getProjectConfigParameters();

    public BuildProjectConfig withProjectConfigFile(ValueProvider<String> filePath) {
        Preconditions.checkNotNull(filePath, "File path cannot be null");
        return toBuilder().setProjectConfigFile(filePath).build();
    }

    public BuildProjectConfig withProjectConfigParameters(ProjectConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setProjectConfigParameters(config).build();
    }

    @Override
    public PCollection<ProjectConfig> expand(PBegin input) {
        LOG.debug("Reading project config file from: {}", getProjectConfigFile());

        // project config side input. Sourced from file
        PCollectionView<List<ProjectConfig>> projectConfigFileView = input.getPipeline()
                .apply("Read project config file", ReadProjectConfigFile.builder()
                        .setFilePath(getProjectConfigFile())
                        .build())
                .apply("To list view", View.<ProjectConfig>asList());

        PCollection<ProjectConfig> outputCollection = input
                .apply("Build config object", Create.of(ProjectConfig.create()))
                .apply("Populate config", ParDo.of(new DoFn<ProjectConfig, ProjectConfig>() {

                    @ProcessElement
                    public void processElement(@Element ProjectConfig inputConfig,
                                               OutputReceiver<ProjectConfig> out,
                                               ProcessContext context) throws Exception {
                        final String loggingPrefix = "BuildProjectConfig ["
                                + RandomStringUtils.randomAlphanumeric(5)
                                + "] - ";
                        LOG.debug(loggingPrefix + "Received config to process: {}", inputConfig.toString());

                        ProjectConfig output = inputConfig;
                        // Identify the project config to use
                        // Source from 1) file and 2) parameters
                        if (context.sideInput(projectConfigFileView).size() > 0) {
                            LOG.info(loggingPrefix + "Project config found in file.");
                            output = context.sideInput(projectConfigFileView).get(0);
                        }
                        if (getProjectConfigParameters().isConfigured()) {
                            LOG.info(loggingPrefix + "Project config found via parameters.");
                            // if the project config is set via parameter, it should overwrite the file based config.

                            if (null != getProjectConfigParameters().getClientId()
                                        && null != getProjectConfigParameters().getTokenUrl()
                                        && null != getProjectConfigParameters().getClientSecret()) {
                                LOG.info(loggingPrefix + "Client credentials specified via parameters");
                                output = getProjectConfigParameters();
                            } else if (null != getProjectConfigParameters().getClientId()
                                    && null != getProjectConfigParameters().getTokenUrl()
                                    && null != getProjectConfigParameters().getClientSecretGcpSecretConfig()) {
                                LOG.info(loggingPrefix + "Client credentials specified via GCP Secret Manager");
                                output = getProjectConfigParameters()
                                        .withClientSecret(getGcpSecret(getProjectConfigParameters().getClientSecretGcpSecretConfig(),
                                                loggingPrefix));
                            } else if (null != getProjectConfigParameters().getApiKey()) {
                                LOG.info(loggingPrefix + "Api key specified via parameters");
                                output = getProjectConfigParameters();
                            } else if (null != getProjectConfigParameters().getApiKeyGcpSecretConfig()) {
                                LOG.info(loggingPrefix + "Api key specified via GCP Secret Manager.");
                                output = getProjectConfigParameters()
                                        .withApiKey(getGcpSecret(getProjectConfigParameters().getApiKeyGcpSecretConfig(),
                                                loggingPrefix));
                            }
                        }

                        LOG.info(loggingPrefix + "Project config after processing: {}", output.toString());
                        out.output(output);
                    }

                    private String getGcpSecret(GcpSecretConfig config, String loggingPrefix) throws IOException {
                        config.validate();
                        String returnValue = "";

                        // Initialize client that will be used to send requests.
                        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
                            SecretVersionName name = SecretVersionName.of(config.getProjectId().get(),
                                    config.getSecretId().get(), config.getSecretVersion().get());

                            // Access the secret version.
                            AccessSecretVersionRequest request =
                                    AccessSecretVersionRequest.newBuilder().setName(name.toString()).build();
                            AccessSecretVersionResponse response = client.accessSecretVersion(request);
                            LOG.info(loggingPrefix + "Successfully read secret from GCP Secret Manager.");

                            returnValue = response.getPayload().getData().toStringUtf8();
                        } catch (Exception e) {
                            String message = "Could not read secret from GCP secret manager. " + e.getMessage();
                            LOG.error(message);
                            throw e;
                        }
                        return returnValue;
                    }

                }).withSideInputs(projectConfigFileView))
                ;

        return outputCollection;
    }

    @AutoValue.Builder
    public static abstract class Builder {
        public abstract Builder setProjectConfigFile(ValueProvider<String> value);
        public abstract Builder setProjectConfigParameters(ProjectConfig value);

        public abstract BuildProjectConfig build();
    }
}
