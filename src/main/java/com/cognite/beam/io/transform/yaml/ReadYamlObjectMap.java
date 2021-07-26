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

package com.cognite.beam.io.transform.yaml;

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
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.representer.Representer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;

/**
 * Utility transform for parsing a map of key-value pairs from a YAML config file. The map is returned as a
 * {@code PCollection<T>}. This transform does perform parsing of the values,
 * so you need to provide .
 * <p>
 * Example uses include using YAML files for hosting a map of config properties.
 */
@AutoValue
public abstract class ReadYamlObjectMap<T> extends PTransform<PBegin, PCollection<T>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static <U> Builder<U> builder() {
        return new AutoValue_ReadYamlObjectMap.Builder<>();
    }

    /**
     * Reads from the given file name.
     *
     * @param fileName
     * @return
     */
    public static <U> ReadYamlObjectMap<U> from(ValueProvider<String> fileName, Class<U> klass) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadYamlObjectMap.<U>builder()
                .setFilePath(fileName)
                .setKlass(klass)
                .build();
    }

    /**
     * Reads from the given file name.
     *
     * @param fileName
     * @return
     */
    public static <U> ReadYamlObjectMap<U> from(String fileName, Class<U> klass) {
        return from(ValueProvider.StaticValueProvider.of(fileName), klass);
    }

    abstract ReadYamlObjectMap.Builder<T> toBuilder();

    abstract ValueProvider<String> getFilePath();

    abstract Class<T> getKlass();

    public ReadYamlObjectMap<T> withClass(Class<T> coder) {
        return this.toBuilder().setKlass(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
        LOG.info("Starting read YAML config file transform.");

        return input.getPipeline()
                .apply("Find file", FileIO.match()
                        .filepattern(getFilePath())
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
                .apply("Read file metadata", FileIO.readMatches()
                        .withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.SKIP))
                .apply("Read YAML file payload", ParDo.of(new DoFn<FileIO.ReadableFile, T>() {
                    @ProcessElement
                    public void processElement(@Element FileIO.ReadableFile file,
                                               OutputReceiver<T> out) throws Exception {
                        LOG.info("Received readable file: {}", file.toString());
                        try {
                            LoaderOptions loaderOptions = new LoaderOptions();
                            BaseConstructor constructor = new Constructor(getKlass(), loaderOptions);
                            Representer representer = new Representer();
                            DumperOptions dumperOptions = new DumperOptions();
                            Yaml yaml = new Yaml(constructor, representer, dumperOptions);
                            InputStream inputStream = new ByteArrayInputStream(file.readFullyAsBytes());
                            for (Object entry : yaml.loadAll(inputStream)) {
                                if (getKlass().isInstance(entry)) {
                                    out.output(getKlass().cast(entry));
                                } else {
                                    LOG.error("Failed to read YAML object");
                                }
                            }
                        } catch (Exception e) {
                            LOG.warn("Could not parse YAML config file [{}]: {}", file.toString(), e);
                        }
                    }
                }));
    }

    @AutoValue.Builder
    static abstract class Builder<T> {
        abstract Builder<T> setFilePath(ValueProvider<String> value);

        abstract Builder<T> setKlass(Class<T> value);

        public abstract ReadYamlObjectMap<T> build();
    }
}
