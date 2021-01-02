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

package com.cognite.beam.io.config;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.ValueProvider;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class represents the configuration of a reader. You can configure the delta read behavior,
 * switch metrics on/off and set the app and session identifiers.
 */
@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class ReaderConfig extends ConfigBase {
    final static String DEFAULT_DELTA_READ_TABLE = "";
    final static String DEFAULT_DELTA_IDENTITY = "delta-timestamp-identifier";
    final static Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(10);
    final static Duration DEFAULT_POLL_OFFSET = Duration.ofSeconds(30);

    private static ReaderConfig.Builder builder() {
        return new com.cognite.beam.io.config.AutoValue_ReaderConfig.Builder()
                .setAppIdentifier(DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(DEFAULT_SESSION_IDENTIFIER)
                .setMetricsEnabled(DEFAULT_ENABLE_METRICS)
                .setDeltaReadTable(ValueProvider.StaticValueProvider.of(DEFAULT_DELTA_READ_TABLE))
                .setDeltaEnabled(false)
                .setDeltaReadIdentifier(ValueProvider.StaticValueProvider.of(DEFAULT_DELTA_IDENTITY))
                .setFullReadOverride(ValueProvider.StaticValueProvider.of(false))
                .setDeltaOffset(ValueProvider.StaticValueProvider.of(Duration.ofMillis(1)))
                .setStreamingEnabled(false)
                .setPollInterval(ValueProvider.StaticValueProvider.of(DEFAULT_POLL_INTERVAL))
                .setPollOffset(ValueProvider.StaticValueProvider.of(DEFAULT_POLL_OFFSET));
    }

    public static ReaderConfig create() {
        return ReaderConfig.builder().build();
    }

    public abstract ReaderConfig.Builder toBuilder();
    public abstract ValueProvider<Boolean> getFullReadOverride();
    public abstract ValueProvider<String> getDeltaReadTable();
    public abstract boolean isDeltaEnabled();
    public abstract ValueProvider<String> getDeltaReadIdentifier();
    public abstract ValueProvider<Duration> getDeltaOffset();
    public abstract boolean isStreamingEnabled();
    public abstract ValueProvider<Duration> getPollInterval();
    public abstract ValueProvider<Duration> getPollOffset();

    /**
     * Set the app identifier. The identifier is encoded in the api calls to the Cognite instance and can be
     * used for tracing and statistics.
     *
     * @param identifier the application identifier
     * @return
     */
    public ReaderConfig withAppIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        Preconditions.checkArgument(!identifier.isEmpty(), "Identifier cannot be an empty string.");
        return toBuilder().setAppIdentifier(identifier).build();
    }

    /**
     * Set the session identifier. The identifier is encoded in the api calls to the Cognite instance and can be
     * used for tracing and statistics.
     *
     * @param identifier the session identifier
     * @return
     */
    public ReaderConfig withSessionIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        Preconditions.checkArgument(!identifier.isEmpty(), "Identifier cannot be an empty string.");
        return toBuilder().setSessionIdentifier(identifier).build();
    }

    /**
     * Controls the logging of metrics. When enabled, the reader will report Cognite api latency, batch size, etc.
     *
     * Metrics are enabled by default.
     *
     * @param enableMetrics Flag for switching on/off metrics. Default is {@code true}.
     * @return
     */
    public ReaderConfig enableMetrics(boolean enableMetrics) {
        return toBuilder().setMetricsEnabled(enableMetrics).build();
    }

    /**
     * Enables timestamp based delta read for supported readers. Specify the CDF.Raw table to store the delta
     * timestamps into. The table name string must follow the format {@code <dbName.tableName>}.
     *
     * @param rawTable The table name, must follow the format {@code <dbName.tableName>}.
     * @return
     */
    public ReaderConfig enableDeltaRead(String rawTable) {
        Preconditions.checkNotNull(rawTable, "Raw table name cannot be null.");
        Preconditions.checkArgument(rawTable.split("\\.").length == 2,
                "Raw table name does not follow the format <dbName.tableName>.");
        return toBuilder().setDeltaReadTable(ValueProvider.StaticValueProvider.of(rawTable)).setDeltaEnabled(true).build();
    }

    /**
     * Enables timestamp based delta read for supported readers. Specify the CDF.Raw table to store the delta
     * timestamps into. The table name string must follow the format {@code <dbName.tableName>}.
     *
     * @param rawTable The table name, must follow the format {@code <dbName.tableName>}.
     * @return
     */
    public ReaderConfig enableDeltaRead(ValueProvider<String> rawTable) {
        Preconditions.checkNotNull(rawTable, "Raw table name cannot be null.");
        return toBuilder().setDeltaReadTable(rawTable).setDeltaEnabled(true).build();
    }

    /**
     * Sets the identifier for this reader. If multiple readers are writing delta timestamps to the same table,
     * the identifier is used to separate between them.
     *
     * @param identifier
     * @return
     */
    public ReaderConfig withDeltaIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null.");
        Preconditions.checkArgument(!identifier.isEmpty() && identifier.length() < 1000,
                "The identifier cannot be null and must be between 1 and 1000 characters.");
        return toBuilder().setDeltaReadIdentifier(ValueProvider.StaticValueProvider.of(identifier)).build();
    }

    /**
     * Sets the identifier for this reader. If multiple readers are writing delta timestamps to the same table,
     * the identifier is used to separate between them.
     *
     * @param identifier
     * @return
     */
    public ReaderConfig withDeltaIdentifier(ValueProvider<String> identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null.");
        return toBuilder().setDeltaReadIdentifier(identifier).build();
    }

    /**
     * If set to <code>true</code> the reader will perform a full read even if a delta configuration has been enabled.
     *
     * This parameter is designed to be used when you have a delta-enabled reader and want to perform a full read at
     * scheduled intervals.
     *
     * @param override
     * @return
     */
    public ReaderConfig withFullReadOverride(boolean override) {
        return toBuilder().setFullReadOverride(ValueProvider.StaticValueProvider.of(override)).build();
    }

    /**
     * If set to <code>true</code> the reader will perform a full read even if a delta configuration has been enabled.
     *
     * This parameter is designed to be used when you have a delta-enabled reader and want to perform a full read at
     * scheduled intervals.
     *
     * @param override
     * @return
     */
    public ReaderConfig withFullReadOverride(ValueProvider<Boolean> override) {
        Preconditions.checkNotNull(override, "Identifier cannot be null.");
        return toBuilder().setFullReadOverride(override).build();
    }

    /**
     * Set an offset / grace window for delta reads. This offset duration will be subtracted from the
     * delta timestamp when performing reads.
     *
     * For example, if the duration is set to two hours, then the reader will read all items added/changed from the
     * timestamp _and the two hour window before_. If the delta timestamp is 6:00, then the reader will query for items
     * added/changed from 4:00 (two hours before the delta timestamp).
     *
     * An offset is very useful for situations where you have eventual consistency. For example, the CDF.Clean resources
     * are eventually consistent, and in order to capture all changes, you should add a small grace period (from 5 min
     * to an hour--depending on load).
     *
     * @param duration
     * @return
     */
    public ReaderConfig withDeltaOffset(ValueProvider<Duration> duration) {
        Preconditions.checkNotNull(duration, "Duration cannot be null");
        return toBuilder().setDeltaOffset(duration).build();
    }

    /**
     * Set an offset / grace window for delta reads. This offset duration will be subtracted from the
     * delta timestamp when performing reads.
     *
     * For example, if the duration is set to two hours, then the reader will read all items added/changed from the
     * timestamp _and the two hour window before_. If the delta timestamp is 6:00, then the reader will query for items
     * added/changed from 4:00 (two hours before the delta timestamp).
     *
     * An offset is very useful for situations where you have eventual consistency. For example, the CDF.Clean resources
     * are eventually consistent, and in order to capture all changes, you should add a small grace period (from 5 min
     * to an hour--depending on load).
     *
     * @param duration
     * @return
     */
    public ReaderConfig withDeltaOffset(Duration duration) {
        Preconditions.checkNotNull(duration, "Duration cannot be null");
        return toBuilder().setDeltaOffset(ValueProvider.StaticValueProvider.of(duration)).build();
    }

    /**
     * Enables streaming mode. The reader will poll CDF regularly for updates until the end time of the
     * {@link com.cognite.beam.io.servicesV1.RequestParameters} are reached ("end", "maxLastUpdatedTime", etc.).
     * If no end time is specified, the reader will keep polling indefinitely (until the job is stopped).
     *
     * The polling interval and offset can be specified
     *
     * @return
     */
    public ReaderConfig watchForNewItems() {
        return toBuilder().setStreamingEnabled(true).build();
    }

    /**
     * Sets the target polling interval when watching for new new items. The default value is 10 seconds.
     *
     * This value is only used when the reader is operating in streaming mode.
     *
     * @param duration
     * @return
     */
    public ReaderConfig withPollInterval(ValueProvider<Duration> duration) {
        Preconditions.checkNotNull(duration, "Duration cannot be null.");
        return toBuilder().setPollInterval(duration).build();
    }

    /**
     * Sets the target polling interval when watching for new new items. The default value is 10 seconds.
     *
     * This value is only used when the reader is operating in streaming mode.
     *
     * @param duration
     * @return
     */
    public ReaderConfig withPollInterval(Duration duration) {
        Preconditions.checkNotNull(duration, "Duration cannot be null.");
        return toBuilder().setPollInterval(ValueProvider.StaticValueProvider.of(duration)).build();
    }

    /**
     * Set an offset / grace window for the reader. The default value is 30 seconds.
     *
     * This offset will be subtracted from the current time when reading from Cognite. That is, when a read is
     * performed it will have an upper timestamp limit of (current time - offset).
     *
     * This setting balances latency vs. consistency. If you use a low offset (for example, 500 milliseconds),
     * you risk missing data items because of CDF's eventual consistency (with the exception of Raw which is immediately
     * consistent). The recommended offset depends on the resource type and load on CDF. Under normal operating
     * conditions data items should be readable about 10 seconds after ingestion. Time series data points should be
     * queryable 1 - 2 seconds after ingestion, but there may be additional upstream delays in the data pipeline to
     * consider.
     *
     * @param duration
     * @return
     */
    public ReaderConfig withPollOffset(ValueProvider<Duration> duration) {
        Preconditions.checkNotNull(duration, "Duration cannot be null.");
        return toBuilder().setPollOffset(duration).build();
    }

    /**
     * Set an offset / grace window for the reader. The default value is 30 seconds.
     *
     * This offset will be subtracted from the current time when reading from Cognite. That is, when a read is
     * performed it will have an upper timestamp limit of (current time - offset).
     *
     * This setting balances latency vs. consistency. If you use a low offset (for example, 500 milliseconds),
     * you risk missing data items because of CDF's eventual consistency (with the exception of Raw which is immediately
     * consistent). The recommended offset depends on the resource type and load on CDF. Under normal operating
     * conditions data items should be readable about 10 seconds after ingestion. Time series data points should be
     * queryable 1 - 2 seconds after ingestion, but there may be additional upstream delays in the data pipeline to
     * consider.
     *
     * @param duration
     * @return
     */
    public ReaderConfig withPollOffset(Duration duration) {
        Preconditions.checkNotNull(duration, "Duration cannot be null.");
        return toBuilder().setPollOffset(ValueProvider.StaticValueProvider.of(duration)).build();
    }

    /**
     * Validates that that this reader config is in a valid state.
     *
     */
    public void validate() {
        checkArgument(getDeltaReadTable() != null && getDeltaReadTable().isAccessible(),
                "Could not obtain the delta read table.");

        checkArgument(getDeltaReadIdentifier() != null && getDeltaReadIdentifier().isAccessible(),
                "Could not obtain the delta read identifier.");

        checkArgument(getFullReadOverride() != null && getFullReadOverride().isAccessible(),
                "Could not obtain the full read override.");

        checkArgument(getDeltaReadTable().get().isEmpty()
                        || getDeltaReadTable().get().split("\\.").length == 2,
                "Raw table name does not follow the format <dbName.tableName>.");

        checkArgument(getPollInterval() != null && getPollInterval().isAccessible(),
                "Could not obtain the poll interval.");

        checkArgument(getPollOffset() != null && getPollOffset().isAccessible(),
                "Could not obtain the poll offset.");
    }

    @AutoValue.Builder
    public abstract static class Builder extends ConfigBase.Builder<Builder> {
        abstract Builder setFullReadOverride(ValueProvider<Boolean> value);
        abstract Builder setDeltaReadTable(ValueProvider<String> value);
        abstract Builder setDeltaEnabled(boolean value);
        abstract Builder setDeltaReadIdentifier(ValueProvider<String> value);
        abstract Builder setDeltaOffset(ValueProvider<Duration> value);
        abstract Builder setStreamingEnabled(boolean value);
        abstract Builder setPollInterval(ValueProvider<Duration> value);
        abstract Builder setPollOffset(ValueProvider<Duration> value);

        abstract ReaderConfig build();
    }
}
