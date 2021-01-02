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

package com.cognite.beam.io.transform.request;

import com.cognite.beam.io.fn.request.BuildReadRawRequestFn;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This transform builds a read raw table request based on input from a config file.
 *
 * It is intended to be used downstream of the TOML reader which produces a collection of {@code KV<String, String>}
 * object representing a config map.
 *
 * You configure which input key(s) to read from. The values must contain a string with minimum two parts:
 * raw dbName and tableName. The value string is split using a custom delimiter.
 */
@AutoValue
public abstract class BuildReadRawRequest extends PTransform<PCollection<KV<String, String>>, PCollection<RequestParameters>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final String DEFAULT_SPLITTER = "\\.";

    private static Builder builder() {
        return new AutoValue_BuildReadRawRequest.Builder()
                .setSplitter(DEFAULT_SPLITTER)
                .setColumns(ImmutableList.of());
    }

    /**
     * Reads from the given key.
     * @param key
     * @return
     */
    public static BuildReadRawRequest from(String key) {
        Preconditions.checkNotNull(key, "Key name cannot be null");
        return BuildReadRawRequest.builder()
                .setKeys(ImmutableList.of(key))
                .build();
    }

    /**
     * Reads from the given keys.
     * @param keys
     * @return
     */
    public static BuildReadRawRequest from(List<String> keys) {
        Preconditions.checkNotNull(keys, "Key name cannot be null");
        return BuildReadRawRequest.builder()
                .setKeys(keys)
                .build();
    }


    abstract Builder toBuilder();
    abstract ImmutableList<String> getKeys();
    abstract ImmutableList<String> getColumns();
    abstract String getSplitter();

    /**
     * Specifies the delimiter (via regEx) to use to split a row into separate fields.
     *
     * @param splitter
     * @return
     */
    public BuildReadRawRequest withSplitter(String splitter) {
        Preconditions.checkNotNull(splitter, "Delimiter cannot be null.");
        return toBuilder().setSplitter(splitter).build();
    }

    /**
     * Specifies the columns to request from the raw table.
     *
     * The defaul behavior is to read all columns.
     *
     * @param columns
     * @return
     */
    public BuildReadRawRequest withColumns(List<String> columns) {
        Preconditions.checkNotNull(columns, "Columns cannot be null.");
        return toBuilder().setColumns(columns).build();
    }

    @Override
    public PCollection<RequestParameters> expand(PCollection<KV<String, String>> input) {

        PCollection<RequestParameters> outputCollection = input
                .apply("Build request", ParDo.of(new BuildReadRawRequestFn(getKeys(), getSplitter(), getColumns())));

        return outputCollection;
    }

    @AutoValue.Builder
    static abstract class Builder {
        abstract Builder setKeys(List<String> value);
        abstract Builder setColumns(List<String> value);
        abstract Builder setSplitter(String value);

        public abstract BuildReadRawRequest build();
    }
}
