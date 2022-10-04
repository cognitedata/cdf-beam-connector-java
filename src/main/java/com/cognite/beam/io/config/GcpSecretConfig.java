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
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.ValueProvider;

import java.io.Serializable;

import static com.google.common.base.Preconditions.*;

/**
 * This class represents a reference to a GCP Secret Manager secret.
 *
 * It is used to securely source api-keys from Secret Manager to a Beam job.
 */
@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class GcpSecretConfig implements Serializable {
    private final static String DEFAULT_SECRET_VERSION = "latest";

    private static Builder builder() {
        return new AutoValue_GcpSecretConfig.Builder()
                .setSecretVersion(ValueProvider.StaticValueProvider.of(DEFAULT_SECRET_VERSION));
    }

    /**
     * Creates a {@code GcpSecretConfig} based on {@link ValueProvider<String>} parameters. This is useful when sourcing
     * the secret reference from job parameters.
     *
     * @param projectId The GCP project id
     * @param secretId The GCP secret manager id
     * @return
     */
    public static GcpSecretConfig of(ValueProvider<String> projectId, ValueProvider<String> secretId) {
        return GcpSecretConfig.builder()
                .setProjectId(projectId)
                .setSecretId(secretId)
                .build();
    }

    /**
     * Creates a {@code GcpSecretConfig} based on the GCP project id and secret id.
     *
     * @param projectId The GCP project id
     * @param secretId The GCP secret manager id
     * @return
     */
    public static GcpSecretConfig of(String projectId, String secretId) {
        checkNotNull(projectId, "Project id cannot be null.");
        checkNotNull(secretId, "Secret id cannot be null.");

        return GcpSecretConfig.of(ValueProvider.StaticValueProvider.of(projectId),
                ValueProvider.StaticValueProvider.of(secretId));
    }

    public abstract ValueProvider<String> getProjectId();
    public abstract ValueProvider<String> getSecretId();
    public abstract ValueProvider<String> getSecretVersion();

    public abstract GcpSecretConfig.Builder toBuilder();

    /**
     * Returns a new {@link GcpSecretConfig} that represents the specified project id.
     * @param value The GCP project id to interact with.
     */
    public GcpSecretConfig withProjectId(ValueProvider<String> value) {
        checkArgument(value != null, "Project id can not be null");
        return toBuilder().setProjectId(value).build();
    }

    /**
     * Returns a new {@link GcpSecretConfig} that represents the specified project id.
     * @param value The GCP project id to interact with.
     */
    public GcpSecretConfig withProjectId(String value) {
        checkArgument(value != null, "Project id cannot be null");
        return withProjectId(ValueProvider.StaticValueProvider.of(value));
    }

    /**
     * Returns a new {@link GcpSecretConfig} that represents the specified secret id.
     * @param value The secret id to read.
     */
    public GcpSecretConfig withSecretId(ValueProvider<String> value) {
        checkArgument(value != null, "Secret id cannot be null");
        return toBuilder().setSecretId(value).build();
    }

    /**
     * Returns a new {@link GcpSecretConfig} that represents the secret id.
     * @param value The secret id to read.
     */
    public GcpSecretConfig withSecretId(String value) {
        checkArgument(value != null, "Secret id can not be null");
        return withSecretId(ValueProvider.StaticValueProvider.of(value));
    }

    /**
     * Returns a new {@link GcpSecretConfig} that represents the specified secret version.
     * @param value The project id interact with.
     */
    public GcpSecretConfig withSecretVersion(ValueProvider<String> value) {
        checkArgument(value != null, "Secret version cannot be null");
        return toBuilder().setSecretVersion(value).build();
    }

    /**
     * Returns a new {@link GcpSecretConfig} that represents the specified secret version.
     * @param value The project id interact with.
     */
    public GcpSecretConfig withSecretVersion(String value) {
        checkArgument(value != null, "Secret version cannot be null");
        return withSecretVersion(ValueProvider.StaticValueProvider.of(value));
    }

    public void validate() {
        checkArgument(getProjectId() != null
                        && getProjectId().isAccessible() && !getProjectId().get().isEmpty(),
                "Could not obtain the project id");

        checkArgument(getSecretId() != null
                        && getSecretId().isAccessible() && !getSecretId().get().isEmpty(),
                "Could not obtain the secret id");

        checkArgument(getSecretVersion() != null
                        && getSecretVersion().isAccessible() && !getSecretVersion().get().isEmpty(),
                "Could not obtain the secret version");
    }

    @AutoValue.Builder public abstract static class Builder {
        public abstract GcpSecretConfig build();

        abstract Builder setProjectId(ValueProvider<String> value);
        abstract Builder setSecretId(ValueProvider<String> value);
        abstract Builder setSecretVersion(ValueProvider<String> value);
    }
}
