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

import com.cognite.client.config.UpsertMode;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class WriterConfig extends ConfigBase {
    private static WriterConfig.Builder builder() {
        return new com.cognite.beam.io.config.AutoValue_WriterConfig.Builder()
                .setAppIdentifier(DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(DEFAULT_SESSION_IDENTIFIER)
                .setMetricsEnabled(DEFAULT_ENABLE_METRICS)
                .setUpsertMode(UpsertMode.UPDATE);
    }

    public static WriterConfig create() {
        return WriterConfig.builder().build();
    }

    public abstract WriterConfig.Builder toBuilder();
    public abstract com.cognite.client.config.UpsertMode getUpsertMode();

    /**
     * Set the app identifier. The identifier is encoded in the api calls to the Cognite instance and can be
     * used for tracing and statistics.
     *
     * @param identifier the application identifier
     * @return
     */
    public WriterConfig withAppIdentifier(String identifier) {
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
    public WriterConfig withSessionIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        Preconditions.checkArgument(!identifier.isEmpty(), "Identifier cannot be an empty string.");
        return toBuilder().setSessionIdentifier(identifier).build();
    }

    /**
     * Controls the logging of metrics. When enabled, the writer will report Cognite api latency, batch size, etc.
     *
     * Metrics are enabled by default.
     *
     * @param enableMetrics Flag for switching on/off metrics. Default is <code>true</code>.
     * @return
     */
    public WriterConfig enableMetrics(boolean enableMetrics) {
        return toBuilder().setMetricsEnabled(enableMetrics).build();
    }

    /**
     * Sets the upsert mode.
     *
     * When the data object to write does not exist, the writer will always create it. But, if the
     * object already exist, the writer can update the the existing object in one of two ways: update or replace.
     *
     * <code>UpsertMode.UPDATE</code> will update the provided fields in the target object--all other fields will remain
     * unchanged.
     *
     * <code>UpsertMode.REPLACE</code> will replace the entire target object with the provided fields
     * (<code>id</code> and <code>externalId</code> will remain unchanged).
     *
     * @param mode
     * @return
     */
    public WriterConfig withUpsertMode(com.cognite.client.config.UpsertMode mode) {
        Preconditions.checkNotNull(mode, "Upsert mode cannot be null");
        return toBuilder().setUpsertMode(mode).build();
    }

    @AutoValue.Builder
    public abstract static class Builder extends ConfigBase.Builder<Builder> {
        abstract WriterConfig.Builder setUpsertMode(com.cognite.client.config.UpsertMode value);
        abstract WriterConfig build();
    }
}
