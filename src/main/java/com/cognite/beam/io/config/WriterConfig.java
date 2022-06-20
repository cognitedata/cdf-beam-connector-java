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

package com.cognite.beam.io.config;

import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import javax.annotation.Nullable;

@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class WriterConfig extends ConfigBase {
    private static WriterConfig.Builder builder() {
        return new com.cognite.beam.io.config.AutoValue_WriterConfig.Builder()
                .setAppIdentifier(DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(DEFAULT_SESSION_IDENTIFIER)
                .setMetricsEnabled(DEFAULT_ENABLE_METRICS)
                .setUpsertMode(UpsertMode.UPDATE)
                .setExtractionPipelineRunStatusMode(ExtractionPipelineRun.Status.SUCCESS);
    }

    public static WriterConfig create() {
        return WriterConfig.builder().build();
    }

    public abstract WriterConfig.Builder toBuilder();
    public abstract com.cognite.client.config.UpsertMode getUpsertMode();
    @Nullable
    public abstract String getExtractionPipelineExtId();
    public abstract ExtractionPipelineRun.Status getExtractionPipelineRunStatusMode();

    /**
     * Set the app identifier. The identifier is encoded in the api calls to the Cognite instance and can be
     * used for tracing and statistics.
     *
     * @param identifier the application identifier
     * @return The {@code WriterConfig} with the parameter configured.
     */
    public WriterConfig withAppIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        Preconditions.checkArgument(!identifier.isEmpty(), "Identifier cannot be an empty string.");
        return toBuilder().setAppIdentifier(identifier).build();
    }

    /**
     * Set the session identifier. The identifier is encoded in the api calls to the Cognite instance and can be
     * used for tracing and statistics.
     *
     * @param identifier the session identifier
     * @return The {@code WriterConfig} with the parameter configured.
     */
    public WriterConfig withSessionIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        Preconditions.checkArgument(!identifier.isEmpty(), "Identifier cannot be an empty string.");
        return toBuilder().setSessionIdentifier(identifier).build();
    }

    /**
     * Controls the logging of metrics. When enabled, the writer will report Cognite api latency, batch size, etc.
     *
     * Metrics are enabled by default.
     *
     * @param enableMetrics Flag for switching on/off metrics. Default is <code>true</code>.
     * @return The {@code WriterConfig} with the parameter configured.
     */
    public WriterConfig enableMetrics(boolean enableMetrics) {
        return toBuilder().setMetricsEnabled(enableMetrics).build();
    }

    /**
     * Sets the upsert mode.
     *
     * When the data object to write does not exist, the writer will always create it. But, if the
     * object already exist, the writer can update the the existing object in one of two ways: update or replace.
     *
     * <code>UpsertMode.UPDATE</code> will update the provided fields in the target object--all other fields will remain
     * unchanged.
     *
     * <code>UpsertMode.REPLACE</code> will replace the entire target object with the provided fields
     * (<code>id</code> and <code>externalId</code> will remain unchanged).
     *
     * @param mode The upsert mode.
     * @return The {@code WriterConfig} with the parameter configured.
     */
    public WriterConfig withUpsertMode(com.cognite.client.config.UpsertMode mode) {
        return toBuilder().setUpsertMode(mode).build();
    }

    /**
     * Enables {@code extraction pipelines} status reporting. When enabled, the writer will report {@code extraction
     * pipeline runs} to CDF at the completion of each window.
     *
     * When the pipeline runs in batch mode, there will normally just be a single (global) window so you get a pipeline
     * run status at the end of the job. On the other hand, when the pipeline runs in streaming/continuous mode, you
     * typically have regular windows and triggers--the writer reports a pipeline run status at each window/trigger. The
     * status message contains the number of items written.
     *
     * If the pipeline fails, no status will be reported. I.e. the writer is not able to report a failed run.
     *
     * The default status is {@code ExtractionPipelineRun.Status.SUCCESS} which is a good default for batch jobs. If you
     * want to use a different status, for example {@code ExtractionPipelineRun.Status.SEEN} then you need to use the
     * {@link #withExtractionPipeline(String, ExtractionPipelineRun.Status)} method.
     *
     * @param externalId The external id of the extraction pipeline to report status to.
     * @return The {@code WriterConfig} with the parameter configured.
     */
    public WriterConfig withExtractionPipeline(String externalId) {
        return toBuilder().setExtractionPipelineExtId(externalId).build();
    }

    /**
     * Enables {@code extraction pipelines} status reporting. When enabled, the writer will report {@code extraction
     * pipeline runs} to CDF at the completion of each window.
     *
     * When the pipeline runs in batch mode, there will normally just be a single (global) window so you get a pipeline
     * run status at the end of the job. On the other hand, when the pipeline runs in streaming/continuous mode, you
     * typically have regular windows and triggers--the writer reports a pipeline run status at each window/trigger. The
     * status message contains the number of items written.
     *
     * If the pipeline fails, no status will be reported. I.e. the writer is not able to report a failed run.
     *
     * The default status to use for batch pipelines is {@code ExtractionPipelineRun.Status.SUCCESS}, and
     * {@code ExtractionPipelineRun.Status.SEEN} for streaming/continuous pipelines.
     *
     * @param externalId The external id of the extraction pipeline to report status to.
     * @param status The {@code ExtractionPipelineRun.Status} type to use when reporting runs.
     * @return The {@code WriterConfig} with the parameter configured.
     */
    public WriterConfig withExtractionPipeline(String externalId, ExtractionPipelineRun.Status status) {
        Preconditions.checkArgument(status == ExtractionPipelineRun.Status.SUCCESS
                || status == ExtractionPipelineRun.Status.SEEN,
                "The extraction pipeline run status must be either SUCCESS or SEEN.");

        return toBuilder()
                .setExtractionPipelineExtId(externalId)
                .setExtractionPipelineRunStatusMode(status)
                .build();
    }

    @AutoValue.Builder
    public abstract static class Builder extends ConfigBase.Builder<Builder> {
        abstract WriterConfig.Builder setUpsertMode(com.cognite.client.config.UpsertMode value);
        abstract WriterConfig.Builder setExtractionPipelineExtId(String value);
        abstract WriterConfig.Builder setExtractionPipelineRunStatusMode(ExtractionPipelineRun.Status value);
        abstract WriterConfig build();
    }
}
