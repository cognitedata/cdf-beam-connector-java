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

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.RequestParameters;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Utility transform for adding project config to request parameters
 */
@AutoValue
public abstract class ApplyProjectConfig extends PTransform<PCollection<RequestParameters>, PCollection<RequestParameters>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static ApplyProjectConfig.Builder builder() {
        return new AutoValue_ApplyProjectConfig.Builder()
                .setProjectConfigFile(ValueProvider.StaticValueProvider.of("."))
                .setProjectConfigParameters(ProjectConfig.create())
                .setReaderConfig(ReaderConfig.create());
    }

    public static ApplyProjectConfig create() {
        return builder().build();
    }
    public abstract ApplyProjectConfig.Builder toBuilder();

    abstract ValueProvider<String> getProjectConfigFile();
    abstract ProjectConfig getProjectConfigParameters();
    abstract ReaderConfig getReaderConfig();

    public ApplyProjectConfig withProjectConfigFile(ValueProvider<String> filePath) {
        Preconditions.checkNotNull(filePath, "File path cannot be null");
        return toBuilder().setProjectConfigFile(filePath).build();
    }

    public ApplyProjectConfig withProjectConfigParameters(ProjectConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setProjectConfigParameters(config).build();
    }

    public ApplyProjectConfig withReaderConfig(ReaderConfig config) {
        Preconditions.checkNotNull(config, "Project config cannot be null.");
        return toBuilder().setReaderConfig(config).build();
    }

    @Override
    public PCollection<RequestParameters> expand(PCollection<RequestParameters> input) {
        LOG.info("Starting apply project config transform.");

        // project config side input
        PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                .apply("Build project config", BuildProjectConfig.create()
                        .withProjectConfigFile(getProjectConfigFile())
                        .withProjectConfigParameters(getProjectConfigParameters())
                        .withAppIdentifier(getReaderConfig().getAppIdentifier())
                        .withSessionIdentifier(getReaderConfig().getSessionIdentifier()))
                .apply("To list view", View.<ProjectConfig>asList());

        PCollection<RequestParameters> outputCollection = input
                .apply("Apply project config", ParDo.of(new DoFn<RequestParameters, RequestParameters>() {
                    @ProcessElement
                    public void processElement(@Element RequestParameters inputQuery,
                                               OutputReceiver<RequestParameters> out,
                                               ProcessContext context) throws Exception {
                        final String batchIdentifierPrefix = "Request id: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
                        LOG.info(batchIdentifierPrefix + "Received request to process {}",
                                inputQuery.getProjectConfig().toString());

                        // Identify the project config to use
                        RequestParameters query = inputQuery;
                        if (!query.getProjectConfig().isConfigured()) {
                            LOG.info(batchIdentifierPrefix + "Project config is not set in the request parameters."
                                    + " Sourcing from the config builder.");
                            // config is not set. Source from config builder
                            if (context.sideInput(projectConfigView).size() > 0) {
                                LOG.info(batchIdentifierPrefix + "Project config identified.");
                                query = query.withProjectConfig(context.sideInput(projectConfigView).get(0));
                            } else {
                                String message = batchIdentifierPrefix + "Cannot identify project config. Empty side input.";
                                LOG.error(message);
                                throw new Exception(message);
                            }
                        } else {
                            LOG.info(batchIdentifierPrefix + "Project config already set in the query");
                        }
                        LOG.info(batchIdentifierPrefix + "Request after processing {}",
                                query.getProjectConfig().toString());
                        out.output(query);
                    }

                }).withSideInputs(projectConfigView))
                ;

        return outputCollection;
    }

    @AutoValue.Builder
    public static abstract class Builder {
        public abstract Builder setProjectConfigFile(ValueProvider<String> value);
        public abstract Builder setProjectConfigParameters(ProjectConfig value);
        public abstract Builder setReaderConfig(ReaderConfig value);

        public abstract ApplyProjectConfig build();
    }
}
