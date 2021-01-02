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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tomlj.Toml;
import org.tomlj.TomlArray;
import org.tomlj.TomlParseError;
import org.tomlj.TomlParseResult;

/**
 * Utility transform for parsing a string array from a TOML config file. All entries in the array will be returned as
 * a {@code PCollection<String>}.
 *
 * You need to configure the source TOML file and the dotted key of the string array.
 *
 * Example uses include using TOML files for hosting white- and blacklists for filtering.
 */
@AutoValue
public abstract class ReadTomlStringArray extends PTransform<PBegin, PCollection<String>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static ReadTomlStringArray.Builder builder() {
        return new AutoValue_ReadTomlStringArray.Builder()
                .setArrayKey("");
    }

    /**
     * Reads from the given file name.
     * @param fileName
     * @return
     */
    public static ReadTomlStringArray from(ValueProvider<String> fileName) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadTomlStringArray.builder()
                .setFilePath(fileName)
                .build();
    }

    /**
     * Reads from the given file name.
     * @param fileName
     * @return
     */
    public static ReadTomlStringArray from(String fileName) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadTomlStringArray.builder()
                .setFilePath(ValueProvider.StaticValueProvider.of(fileName))
                .build();
    }

    /**
     * Reads the string array from the specified key.
     * @param dottedKey
     * @return
     */
    public ReadTomlStringArray withArrayKey(String dottedKey) {
        Preconditions.checkNotNull(dottedKey, "The array key cannot be null.");
        return toBuilder().setArrayKey(dottedKey).build();
    }

    abstract ReadTomlStringArray.Builder toBuilder();
    abstract ValueProvider<String> getFilePath();
    abstract String getArrayKey();

    @Override
    public PCollection<String> expand(PBegin input) {
        LOG.info("Starting read TOML config file transform.");

        PCollection<String> outputCollection = input.getPipeline()
                .apply("Find file", FileIO.match()
                        .filepattern(getFilePath())
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
                .apply("Read file metadata", FileIO.readMatches()
                        .withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.SKIP))
                .apply("Read TOML file payload", ParDo.of(new DoFn<FileIO.ReadableFile, String>() {
                    @ProcessElement
                    public void processElement(@Element FileIO.ReadableFile file,
                                               OutputReceiver<String> out) throws Exception {
                        LOG.info("Received readable file: {}", file.toString());
                        TomlParseResult parseResult = Toml.parse(file.readFullyAsUTF8String());
                        LOG.debug("Finish parsing toml file");

                        try {
                            if (parseResult.hasErrors()) {
                                for (TomlParseError parseError : parseResult.errors()) {
                                    LOG.warn("Error parsing project config file: {}", parseError.toString());
                                }
                                throw new Exception(parseResult.errors().get(0).getMessage());
                            }

                            // the key must be valid and represent a string array
                            if (parseResult.contains(getArrayKey()) && parseResult.isArray(getArrayKey())
                                    && parseResult.getArrayOrEmpty(getArrayKey()).containsStrings()) {
                                TomlArray stringArray = parseResult.getArrayOrEmpty(getArrayKey());
                                for (int i = 0; i < stringArray.size(); i++) {
                                    out.output(stringArray.getString(i));
                                }
                            } else {
                                throw new Exception("Cannot find string array entry for key [" + getArrayKey() + "]");
                            }
                        } catch (Exception e) {
                            LOG.warn("Could not parse TOML config file [{}]: {}", file.toString(), e);
                            return;
                        }
                    }
                }));

        return outputCollection;
    }

    @AutoValue.Builder
    static abstract class Builder {
        abstract Builder setFilePath(ValueProvider<String> value);
        abstract Builder setArrayKey(String value);

        public abstract ReadTomlStringArray build();
    }
}
