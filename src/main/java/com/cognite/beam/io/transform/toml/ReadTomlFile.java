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

package com.cognite.beam.io.transform.toml;

import com.cognite.beam.io.fn.read.ReadTomlConfigFileFn;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility transform for reading a TOML config file from the specified location and parsing it into
 * a String representation which can be used by a TOML parser to read config settings.
 */
@AutoValue
public abstract class ReadTomlFile extends PTransform<PBegin, PCollection<KV<String, String>>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static ReadTomlFile.Builder builder() {
        return new AutoValue_ReadTomlFile.Builder();
    }

    /**
     * Reads from the given file name.
     * @param fileName
     * @return
     */
    public static ReadTomlFile from(ValueProvider<String> fileName) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadTomlFile.builder()
                .setFilePath(fileName)
                .build();
    }

    /**
     * Reads from the given file name.
     * @param fileName
     * @return
     */
    public static ReadTomlFile from(String fileName) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadTomlFile.builder()
                .setFilePath(ValueProvider.StaticValueProvider.of(fileName))
                .build();
    }

    abstract ValueProvider<String> getFilePath();

    @Override
    public PCollection<KV<String, String>> expand(PBegin input) {
        PCollection<KV<String, String>> outputCollection = input.getPipeline()
                .apply("Find file", FileIO.match()
                        .filepattern(getFilePath())
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
                .apply("Read file metadata", FileIO.readMatches()
                        .withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.SKIP))
                .apply("Read TOML file payload", ParDo.of(new ReadTomlConfigFileFn()));

        return outputCollection;
    }

    @AutoValue.Builder
    static abstract class Builder {
        abstract Builder setFilePath(ValueProvider<String> value);

        public abstract ReadTomlFile build();
    }
}
