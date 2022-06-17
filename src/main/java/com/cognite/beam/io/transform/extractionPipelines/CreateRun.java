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

package com.cognite.beam.io.transform.extractionPipelines;

import com.cognite.beam.io.ConnectorBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.extractionPipelines.CreateExtPipelineRunFn;
import com.cognite.beam.io.transform.internal.BuildProjectConfig;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Transform for creating/writing extraction pipeline runs to Cognite Data Fusion.
 *
 * Extraction pipeline runs report the status of data producing modules (extractors, data pipelines, contextualization
 * pipelines, etc.) to Cognite Data Fusion.
 */
@AutoValue
public abstract class CreateRun extends ConnectorBase<PCollection<ExtractionPipelineRun>, PCollection<ExtractionPipelineRun>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static CreateRun.Builder builder() {
        return new AutoValue_CreateRun.Builder()
                .setProjectConfig(ProjectConfig.create())
                .setHints(Hints.create())
                .setWriterConfig(WriterConfig.create().enableMetrics(false))
                .setProjectConfigFile(ValueProvider.StaticValueProvider.of("."));
    }

    /**
     * Creates the transform.
     *
     * This is the default starting method when creating/instantiating this transform. Subsequently you configure the
     * transform via the {@code withFooBar()} methods. For example:
     * <pre>
     * {@code
     * CreateRun transform = CreateRun.create()
     *                         .withProjectConfig(my-project-config)
     *                         .withWriterConfig(my-writer-config);
     * }
     * </pre>
     *
     * @return The transform
     */
    public static CreateRun create() {
        return CreateRun.builder().build();
    }

    public CreateRun withProjectConfig(ProjectConfig config) {
        return toBuilder().setProjectConfig(config).build();
    }

    public CreateRun withWriterConfig(WriterConfig config) {
        return toBuilder().setWriterConfig(config).build();
    }

    public CreateRun withProjectConfigFile(String file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
        return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
    }

    public CreateRun withProjectConfigFile(ValueProvider<String> file) {
        return toBuilder().setProjectConfigFile(file).build();
    }

    abstract CreateRun.Builder toBuilder();
    abstract WriterConfig getWriterConfig();

    @Override
    public PCollection<ExtractionPipelineRun> expand(PCollection<ExtractionPipelineRun> input) {

        // project config side input
        PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                .apply("Build project config", BuildProjectConfig.create()
                        .withProjectConfigFile(getProjectConfigFile())
                        .withProjectConfigParameters(getProjectConfig()))
                .apply("To list view", View.<ProjectConfig>asList());

        // Read qualified rows from raw.
        PCollection<ExtractionPipelineRun> outputCollection = input
                .apply("Wrap into collection", MapElements.into(TypeDescriptors.iterables(TypeDescriptor.of(ExtractionPipelineRun.class)))
                        .via(item -> List.of(item)))
                .apply("Write pipeline run", ParDo.of(
                        new CreateExtPipelineRunFn(getHints(), getWriterConfig(), projectConfigView))
                        .withSideInputs(projectConfigView));

        return outputCollection;
    }

    @AutoValue.Builder
    static abstract class Builder extends ConnectorBase.Builder<Builder> {
        abstract Builder setWriterConfig(WriterConfig value);

        public abstract CreateRun build();
    }
}
