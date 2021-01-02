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
import com.google.common.collect.ImmutableSet;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tomlj.*;


/**
 * Utility transform for parsing a map of key-value pairs from a TOML config file. All entries must be string (keys and)
 * values. Other values, like numeric, dates, arrays, nested containers will be ignored. The map is returned as a
 * {@code PCollection<KV<String, String>>>}.
 *
 * You need to configure the source TOML file and the dotted key of the map (container). If the key is not set,
 * then this transform parses the root namespace.
 *
 * Example uses include using TOML files for hosting a map of config properties.
 */
@AutoValue
public abstract class ReadTomlStringMap extends PTransform<PBegin, PCollection<KV<String, String>>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static ReadTomlStringMap.Builder builder() {
        return new AutoValue_ReadTomlStringMap.Builder()
                .setMapKey("");
    }

    /**
     * Reads from the given file name.
     * @param fileName
     * @return
     */
    public static ReadTomlStringMap from(ValueProvider<String> fileName) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadTomlStringMap.builder()
                .setFilePath(fileName)
                .build();
    }

    /**
     * Reads from the given file name.
     * @param fileName
     * @return
     */
    public static ReadTomlStringMap from(String fileName) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadTomlStringMap.builder()
                .setFilePath(ValueProvider.StaticValueProvider.of(fileName))
                .build();
    }

    /**
     * Reads the map from the specified key.
     *
     * The key must represent a TOML container (for example, a table). The map is then produced based on the key - value
     * entries within that container.
     *
     * @param dottedKey
     * @return
     */
    public ReadTomlStringMap withMapKey(String dottedKey) {
        Preconditions.checkNotNull(dottedKey, "The array key cannot be null.");
        return toBuilder().setMapKey(dottedKey).build();
    }

    abstract ReadTomlStringMap.Builder toBuilder();
    abstract ValueProvider<String> getFilePath();
    abstract String getMapKey();

    @Override
    public PCollection<KV<String, String>> expand(PBegin input) {
        LOG.info("Starting read TOML config file transform.");

        PCollection<KV<String, String>> outputCollection = input.getPipeline()
                .apply("Find file", FileIO.match()
                        .filepattern(getFilePath())
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
                .apply("Read file metadata", FileIO.readMatches()
                        .withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.SKIP))
                .apply("Read TOML file payload", ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(@Element FileIO.ReadableFile file,
                                               OutputReceiver<KV<String, String>> out) throws Exception {
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

                            // get the map/table and key set of the map
                            ImmutableSet<String> keySet;
                            TomlTable table;
                            if (getMapKey().isEmpty()) {
                                // Special case, we extract from the root namespace of the TOML file
                                keySet = ImmutableSet.copyOf(parseResult.keySet());
                                table = parseResult;
                            } else {
                                // the map key must represent a valid table
                                if (parseResult.contains(getMapKey()) && parseResult.isTable(getMapKey())) {
                                    table = parseResult.getTable(getMapKey());
                                    keySet = ImmutableSet.copyOf(table.keySet());
                                } else {
                                    throw new Exception("The specified map key is not a valid TOML table.");
                                }
                            }

                            // Add string config keys to the map.
                            for (String key : keySet) {
                                if (table.isString(key)) {
                                    out.output(KV.of(key, table.getString(key)));
                                }
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
        abstract Builder setMapKey(String value);

        public abstract ReadTomlStringMap build();
    }
}
