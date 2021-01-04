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

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.ConnectorBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.RawRow;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Utility transform for writing a Unix style timestamp (ms since epoch) to Raw.
 */
@AutoValue
public abstract class WriteTimestamp extends ConnectorBase<PCollection<Long>, PCollection<RawRow>> {
    private static final String DEFAULT_IDENTIFIER = "delta-timestamp-identifier";

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static WriteTimestamp.Builder builder() {
        return new AutoValue_WriteTimestamp.Builder();
    }

    /**
     * Writes to the given cdf.raw table. The string must follow the format {@code <dbName.tableName>}.
     *
     * @param rawTableName
     * @return
     */
    public static WriteTimestamp to(ValueProvider<String> rawTableName) {
        Preconditions.checkNotNull(rawTableName, "Raw table name cannot be null");
        return WriteTimestamp.builder()
                .setRawTableName(rawTableName)
                .setIdentifier(ValueProvider.StaticValueProvider.of(DEFAULT_IDENTIFIER))
                .setProjectConfig(ProjectConfig.create())
                .setHints(Hints.create()
                        .withWriteShards(1))
                .setWriterConfig(WriterConfig.create())
                .setProjectConfigFile(ValueProvider.StaticValueProvider.of("."))
                .build();
    }

    /**
     * Writes to the given cdf.raw table. The string must follow the format {@code <dbName.tableName>}.
     *
     * @param rawTableName
     * @return
     */
    public static WriteTimestamp to(String rawTableName) {
        Preconditions.checkNotNull(rawTableName, "Raw table name cannot be null");
        return WriteTimestamp.to(ValueProvider.StaticValueProvider.of(rawTableName));
    }

    /**
     * Sets the identifier for this writer. If multiple writers are writing to the same table, the identifier
     * is used to separate between them.
     *
     * @param identifier
     * @return
     */
    public WriteTimestamp withIdentifier(ValueProvider<String> identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        return toBuilder().setIdentifier(identifier).build();
    }

    /**
     * Sets the identifier for this writer. If multiple writers are writing to the same table, the identifier
     * is used to separate between them.
     *
     * @param identifier
     * @return
     */
    public WriteTimestamp withIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        return toBuilder().setIdentifier(ValueProvider.StaticValueProvider.of(identifier)).build();
    }

    public WriteTimestamp withProjectConfig(ProjectConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setProjectConfig(config).build();
    }

    public WriteTimestamp withWriterConfig(WriterConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setWriterConfig(config).build();
    }

    public WriteTimestamp withProjectConfigFile(String file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
        return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
    }

    public WriteTimestamp withProjectConfigFile(ValueProvider<String> file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        return toBuilder().setProjectConfigFile(file).build();
    }

    abstract WriteTimestamp.Builder toBuilder();
    abstract ValueProvider<String> getRawTableName();
    abstract ValueProvider<String> getIdentifier();
    abstract WriterConfig getWriterConfig();

    @Override
    public PCollection<RawRow> expand(PCollection<Long> input) {
        LOG.info("Starting writeTimestamp transform.");

        PCollection<RawRow> outputCollection = input
                .apply("Build row object", MapElements.into(TypeDescriptor.of(RawRow.class))
                        .via((Long timeStamp) -> {
                            this.validate();
                            RawRow row = RawRow.newBuilder()
                                    .setDbName(getRawTableName().get().split("\\.")[0])
                                    .setTableName(getRawTableName().get().split("\\.")[1])
                                    .setKey(getIdentifier().get() + "_" + System.currentTimeMillis())
                                    .setColumns(Struct.newBuilder()
                                            .putFields("identifier", Value.newBuilder()
                                                    .setStringValue(getIdentifier().get()).build())
                                            .putFields("timestamp", Value.newBuilder()
                                                    .setNumberValue(timeStamp).build())
                                            .putFields("utc", Value.newBuilder()
                                                    .setStringValue(DateTimeFormatter.ISO_INSTANT.format(
                                                            Instant.ofEpochMilli(timeStamp)))
                                                    .build())
                                            .putFields("comment", Value.newBuilder()
                                                    .setStringValue("Delta read timestamp set by the Cognite Beam SDK")
                                                    .build())
                                            .build())
                                    .build();
                            LOG.info("Writing timestamp row: {}", row);
                            return row;
                                }
                        ))
                .apply("Write to raw", CogniteIO.writeRawRow()
                        .withWriterConfig(getWriterConfig().enableMetrics(false))
                        .withHints(getHints())
                        .withProjectConfig(getProjectConfig())
                        .withProjectConfigFile(getProjectConfigFile()));

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
        abstract Builder setWriterConfig(WriterConfig value);

        public abstract WriteTimestamp build();
    }
}
