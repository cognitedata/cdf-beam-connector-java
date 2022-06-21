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

import com.cognite.beam.io.ConnectorBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.transform.extractionPipelines.CreateRun;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal transform for reporting extraction pipeline runs from the {@code CogniteIO} writers.
 *
 * It takes the writer's published objects as input. Then the transform counts the number of items written during the
 * Beam window (single, global window in the case of batch jobs--multiple windows in the case of streaming jobs). The
 * transform builds a {@link ExtractionPipelineRun} object, with a message containing the object count and writes
 * it to Cognite Data Fusion.
 *
 * Extraction pipeline runs report the status of data producing modules (extractors, data pipelines, contextualization
 * pipelines, etc.) to Cognite Data Fusion.
 */
@AutoValue
public abstract class WritePipelineRun<T> extends ConnectorBase<PCollection<T>, PCollection<ExtractionPipelineRun>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static <T> WritePipelineRun.Builder<T> builder() {
        return new AutoValue_WritePipelineRun.Builder<T>()
                .setProjectConfig(ProjectConfig.create())
                .setHints(Hints.create())
                .setWriterConfig(WriterConfig.create().enableMetrics(false))
                .setProjectConfigFile(ValueProvider.StaticValueProvider.of("."))
                .setWriterOperationDescription("upserted");
    }

    /**
     * Creates the transform.
     *
     * This is the default starting method when creating/instantiating this transform. Subsequently you configure the
     * transform via the {@code withFooBar()} methods. For example:
     * <pre>
     * {@code
     * WritePipelineRun transform = WritePipelineRun.create()
     *                         .withProjectConfig(my-project-config)
     *                         .withWriterConfig(my-writer-config);
     * }
     * </pre>
     *
     * @return The transform
     */
    public static <T> WritePipelineRun<T> create() {
        return WritePipelineRun.<T>builder().build();
    }

    public WritePipelineRun<T> withProjectConfig(ProjectConfig config) {
        return toBuilder().setProjectConfig(config).build();
    }

    public WritePipelineRun<T> withWriterConfig(WriterConfig config) {
        return toBuilder().setWriterConfig(config).build();
    }

    public WritePipelineRun<T> withProjectConfigFile(String file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
        return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
    }

    public WritePipelineRun<T> withProjectConfigFile(ValueProvider<String> file) {
        return toBuilder().setProjectConfigFile(file).build();
    }

    /**
     * Set the writer operation description. This will be included in the status message submitted in the extraction
     * pipeline run. Examples of descriptions would be "upserted", "written", "deleted".
     *
     * The default description is {@code upsert}.
     * @param operation The operation name.
     * @return The transform with the configuration set.
     */
    public WritePipelineRun<T> withWriterOperationDescription(String operation) {
        return toBuilder().setWriterOperationDescription(operation).build();
    }

    abstract WritePipelineRun.Builder<T> toBuilder();
    abstract WriterConfig getWriterConfig();
    abstract String getWriterOperationDescription();

    @Override
    public PCollection<ExtractionPipelineRun> expand(PCollection<T> input) {


        // Read qualified rows from raw.
        PCollection<ExtractionPipelineRun> outputCollection = input
                .apply("Count elements", Combine.globally(Count.<T>combineFn()).withoutDefaults())
                .apply("Build pipeline status entry", MapElements.into(TypeDescriptor.of(ExtractionPipelineRun.class))
                        .via(count ->
                                ExtractionPipelineRun.newBuilder()
                                        .setExternalId(getWriterConfig().getExtractionPipelineExtId())
                                        .setStatus(getWriterConfig().getExtractionPipelineRunStatusMode())
                                        .setMessage(String.format("Number of data objects %s to CDF: %d",
                                                getWriterOperationDescription(),
                                                count))
                                        .build()
                        )
                )
                .apply("Write pipeline run", CreateRun.create()
                        .withProjectConfig(getProjectConfig())
                        .withProjectConfigFile(getProjectConfigFile())
                        .withWriterConfig(getWriterConfig()));

        return outputCollection;
    }

    @AutoValue.Builder
    static abstract class Builder<T> extends ConnectorBase.Builder<Builder<T>> {
        abstract Builder<T> setWriterConfig(WriterConfig value);
        abstract Builder<T> setWriterOperationDescription(String value);

        public abstract WritePipelineRun<T> build();
    }
}
