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

package com.cognite.beam.io.transform;

import com.cognite.beam.io.ConnectorBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.transform.internal.BuildProjectConfig;
import com.cognite.client.dto.RawRow;
import com.cognite.beam.io.fn.read.ReadRawRow;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.transform.internal.ApplyProjectConfig;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Utility transform for writing a Unix style timestamp (ms since epoch) to Raw.
 */
@AutoValue
public abstract class ReadTimestamp extends ConnectorBase<PBegin, PCollection<Long>> {
    private static final String DEFAULT_IDENTIFIER = "delta-timestamp-identifier";

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static ReadTimestamp.Builder builder() {
        return new AutoValue_ReadTimestamp.Builder();
    }

    /**
     * Reads from the given cdf.raw table. The string must follow the format {@code <dbName.tableName>}.
     *
     * @param rawTableName
     * @return
     */
    public static ReadTimestamp from(ValueProvider<String> rawTableName) {
        Preconditions.checkNotNull(rawTableName, "Raw table name cannot be null");
        return ReadTimestamp.builder()
                .setRawTableName(rawTableName)
                .setIdentifier(ValueProvider.StaticValueProvider.of(DEFAULT_IDENTIFIER))
                .setProjectConfig(ProjectConfig.create())
                .setHints(Hints.create())
                .setReaderConfig(ReaderConfig.create().enableMetrics(false))
                .setProjectConfigFile(ValueProvider.StaticValueProvider.of("."))
                .build();
    }

    /**
     * Reads from the given cdf.raw table. The string must follow the format {@code <dbName.tableName>}.
     *
     * @param rawTableName
     * @return
     */
    public static ReadTimestamp from(String rawTableName) {
        Preconditions.checkNotNull(rawTableName, "Raw table name cannot be null");
        return ReadTimestamp.from(ValueProvider.StaticValueProvider.of(rawTableName));
    }

    /**
     * Sets the identifier for this reader. It uses the identifier to filter the timestamps to search for.
     *
     * @param identifier
     * @return
     */
    public ReadTimestamp withIdentifier(ValueProvider<String> identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        return toBuilder().setIdentifier(identifier).build();
    }

    /**
     * Sets the identifier for this reader. It uses the identifier to filter the timestamps to search for.
     *
     * @param identifier
     * @return
     */
    public ReadTimestamp withIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        return toBuilder().setIdentifier(ValueProvider.StaticValueProvider.of(identifier)).build();
    }

    public ReadTimestamp withProjectConfig(ProjectConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setProjectConfig(config).build();
    }

    public ReadTimestamp withReaderConfig(ReaderConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setReaderConfig(config).build();
    }

    public ReadTimestamp withProjectConfigFile(String file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
        return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
    }

    public ReadTimestamp withProjectConfigFile(ValueProvider<String> file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        return toBuilder().setProjectConfigFile(file).build();
    }

    abstract ReadTimestamp.Builder toBuilder();
    abstract ValueProvider<String> getRawTableName();
    abstract ValueProvider<String> getIdentifier();
    abstract ReaderConfig getReaderConfig();

    @Override
    public PCollection<Long> expand(PBegin input) {
        LOG.debug("Starting ReadTimestamp transform.");

        // project config side input
        PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                .apply("Build project config", BuildProjectConfig.create()
                        .withProjectConfigFile(getProjectConfigFile())
                        .withProjectConfigParameters(getProjectConfig()))
                .apply("To list view", View.<ProjectConfig>asList());

        // In case there are no qualified rows from Raw, we need a default value
        PCollection<RawRow> defaultRow = input.getPipeline()
                .apply("Trigger default row", Create.of(RawRow.newBuilder().build()))
                .apply("Populate default row", MapElements.into(TypeDescriptor.of(RawRow.class))
                        .via(row -> {
                            this.validate();
                            LOG.info("Building a default delta timestamp in case none can be sourced from raw.");
                            return RawRow.newBuilder()
                                    .setDbName(getRawTableName().get().split("\\.")[0])
                                    .setTableName(getRawTableName().get().split("\\.")[1])
                                    .setKey("default-row")
                                    .setLastUpdatedTime(1L) // set a low timestamp so real rows will take precedence.
                                    .setColumns(Struct.newBuilder()
                                            .putFields("timestamp", Value.newBuilder().setNumberValue(1D).build())
                                            .putFields("identifier", Value.newBuilder().setStringValue("default-row").build())
                                            .build())
                                    .build();
                        }));

        // Read qualified rows from raw.
        PCollection<RawRow> rawRows = input.getPipeline()
                .apply("Generate timestamp query", Create.of(RequestParameters.create()))
                .apply("Build timestamp query", MapElements.into(TypeDescriptor.of(RequestParameters.class))
                        .via(request -> {
                            this.validate();
                            LOG.info("Reading timestamp from raw table: {}", getRawTableName().get());

                            return RequestParameters.create()
                                    .withDbName(getRawTableName().get().split("\\.")[0])
                                    .withTableName(getRawTableName().get().split("\\.")[1]);
                                }
                        ))
                .apply("Read results", ParDo.of(
                        new ReadRawRow(getHints().withReadShards(1),
                                getReaderConfig().withFullReadOverride(true),
                                projectConfigView))
                        .withSideInputs(projectConfigView))
                .apply("Filter rows", Filter.by(row ->
                        row.hasColumns() && row.getColumns().containsFields("identifier")
                        && row.getColumns().containsFields("timestamp")
                        && row.getColumns().getFieldsMap().get("identifier")
                                .getStringValue().equals(getIdentifier().get())));

        /*
        Merge the default row and rows from Raw together and identify the most recent row. If no rows are found in
        Raw, then the default row will be used--if Raw does contain qualified rows, then these will take precedence.
        Lastly, extract the delta timestamp from the most recent row.
        */
        PCollectionList<RawRow> collections = PCollectionList.of(defaultRow).and(rawRows);
        PCollection<Long> outputCollection = collections
                .apply("Merge RawRow collections", Flatten.<RawRow>pCollections())
                .apply("Get newest row", Max.globally((Comparator<RawRow> & Serializable)
                        (RawRow left, RawRow right) ->
                                Long.compare(left.getLastUpdatedTime(), right.getLastUpdatedTime())
                        ))
                .apply("Parse row to timestamp", MapElements.into(TypeDescriptors.longs())
                        .via((RawRow row) -> {
                            LOG.info("Identified delta timestamp from Raw:" + System.lineSeparator() + row);
                            return Math.round(row.getColumns().getFieldsMap().get("timestamp").getNumberValue());
                        }));

        return outputCollection;
    }

    void validate() {
        Preconditions.checkState(getRawTableName().isAccessible()
                && getRawTableName().get().split("\\.").length == 2,
                "Raw table name does not follow the format <dbName.tableName>.");

        Preconditions.checkState(getIdentifier().isAccessible()
                        && !getIdentifier().get().isEmpty()
                        && getIdentifier().get().length() < 1000,
                "The identifier cannot be null and must be between 1 and 1000 characters.");
    }

    @AutoValue.Builder
    static abstract class Builder extends ConnectorBase.Builder<Builder> {
        abstract Builder setRawTableName(ValueProvider<String> value);
        abstract Builder setIdentifier(ValueProvider<String> value);
        abstract Builder setReaderConfig(ReaderConfig value);

        public abstract ReadTimestamp build();
    }
}
