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
import com.cognite.beam.io.fn.parse.ParseProjectConfigFn;
import com.cognite.beam.io.transform.toml.ReadTomlFile;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility transform for reading a project config (toml) file from the specified location and parsing it into
 * {@link ProjectConfig}.
 */
@AutoValue
public abstract class ReadProjectConfigFile extends PTransform<PBegin, PCollection<ProjectConfig>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public static ReadProjectConfigFile.Builder builder() {
        return new com.cognite.beam.io.transform.internal.AutoValue_ReadProjectConfigFile.Builder();
    }

    abstract ValueProvider<String> getFilePath();

    public abstract ReadProjectConfigFile.Builder toBuilder();

    @Override
    public PCollection<ProjectConfig> expand(PBegin input) {
        LOG.info("Starting read project config file transform.");

        PCollection<ProjectConfig> outputCollection = input.getPipeline()
                .apply("Read Toml file", ReadTomlFile.from(getFilePath()))
                .apply("Remove key", Values.create())
                .apply("Parse project config", ParDo.of(new ParseProjectConfigFn()));

        return outputCollection;
    }

    @AutoValue.Builder
    public static abstract class Builder {
        public abstract Builder setFilePath(ValueProvider<String> value);

        public abstract ReadProjectConfigFile build();
    }
}
