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
import com.squareup.moshi.JsonClass;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.ValueProvider;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@AutoValue
@DefaultCoder(SerializableCoder.class)
@JsonClass(generateAdapter = true, generator = "avm")
public abstract class ProjectConfig implements Serializable {
    private final static String DEFAULT_HOST = "https://api.cognitedata.com";

    private static Builder builder() {
        return new com.cognite.beam.io.config.AutoValue_ProjectConfig.Builder()
                .setHost(ValueProvider.StaticValueProvider.of(DEFAULT_HOST))
                .setConfigured(false);
    }

    public static ProjectConfig create() {
        return ProjectConfig.builder().build();
    }

    @Nullable public abstract ValueProvider<String> getProject();
    @Nullable public abstract ValueProvider<String> getApiKey();
    @Nullable public abstract ValueProvider<String> getClientId();
    @Nullable public abstract ValueProvider<String> getClientSecret();
    @Nullable public abstract ValueProvider<String> getTokenUrl();
    @Nullable public abstract ValueProvider<Collection<String>> getAuthScopes();
    public abstract ValueProvider<String> getHost();
    public abstract boolean isConfigured();
    @Nullable  public abstract GcpSecretConfig getApiKeyGcpSecretConfig();
    @Nullable  public abstract GcpSecretConfig getClientSecretGcpSecretConfig();

    public abstract ProjectConfig.Builder toBuilder();

    /**
     * Returns a new {@code ProjectConfig} that represents the specified host.
     *
     * @param value The Cognite Data Fusion host.
     */
    public ProjectConfig withHost(ValueProvider<String> value) {
        return toBuilder().setHost(value).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} that represents the specified host.
     *
     * @param value The Cognite Data Fusion host.
     */
    public ProjectConfig withHost(String value) {
        return withHost(ValueProvider.StaticValueProvider.of(value));
    }

    /**
     * Returns a new {@code ProjectConfig} that represents the specified project.
     *
     * @param value The project id to interact with.
     */
    public ProjectConfig withProject(ValueProvider<String> value) {
        return toBuilder().setProject(value).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} that represents the specified project.
     *
     * @param value The project id to interact with.
     */
    public ProjectConfig withProject(String value) {
        return withProject(ValueProvider.StaticValueProvider.of(value));
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified api key.
     *
     * @param value The api key.
     */
    public ProjectConfig withApiKey(ValueProvider<String> value) {
        return toBuilder().setApiKey(value).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified api key.
     *
     * @param value The api key.
     */
    public ProjectConfig withApiKey(String value) {
        return withApiKey(ValueProvider.StaticValueProvider.of(value));
    }

    /**
     * Returns a new {@code ProjectConfig} with the api key stored in GCP Secret Manager.
     *
     * @param config The reference to the secret in GCP Secret Manager
     * @return
     */
    public ProjectConfig withApiKeyFromGcpSecret(GcpSecretConfig config) {
        return toBuilder().setApiKeyGcpSecretConfig(config).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified client id.
     *
     * @param value The client id.
     */
    public ProjectConfig withClientId(ValueProvider<String> value) {
        return toBuilder().setClientId(value).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified client id.
     *
     * @param value The client id.
     */
    public ProjectConfig withClientId(String value) {
        return toBuilder().setClientId(ValueProvider.StaticValueProvider.of(value)).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified client secret.
     *
     * @param value The client secret.
     */
    public ProjectConfig withClientSecret(ValueProvider<String> value) {
        return toBuilder().setClientSecret(value).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified client secret.
     *
     * @param value The client secret.
     */
    public ProjectConfig withClientSecret(String value) {
        return toBuilder().setClientSecret(ValueProvider.StaticValueProvider.of(value)).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the client secret stored in GCP Secret Manager.
     *
     * @param config The reference to the secret in GCP Secret Manager
     * @return
     */
    public ProjectConfig withClientSecretFromGcpSecret(GcpSecretConfig config) {
        return toBuilder().setClientSecretGcpSecretConfig(config).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified token URL.
     *
     * @param value The token URL.
     */
    public ProjectConfig withTokenUrl(ValueProvider<String> value) {
        return toBuilder().setTokenUrl(value).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified token URL.
     *
     * @param value The token URL.
     */
    public ProjectConfig withTokenUrl(String value) {
        return toBuilder().setTokenUrl(ValueProvider.StaticValueProvider.of(value)).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified auth scopes.
     *
     * @param scopes The auth scopes.
     */
    public ProjectConfig withAuthScopes(Collection<String> scopes) {
        return toBuilder().setAuthScopes(ValueProvider.StaticValueProvider.of(scopes)).setConfigured(true).build();
    }

    /**
     * Returns a new {@code ProjectConfig} with the specified auth scopes.
     *
     * @param scopes The auth scopes.
     */
    public ProjectConfig withAuthScopes(ValueProvider<Collection<String>> scopes) {
        return toBuilder().setAuthScopes(scopes).setConfigured(true).build();
    }

    public void validate() {
        checkState(isConfigured(), "ProjectConfig parameters have not been configured.");
        checkArgument(getHost() != null
                        && getHost().isAccessible() && !getHost().get().isEmpty(),
                "Could not obtain Cognite host name");

        checkArgument(getProject() != null
                        && getProject().isAccessible() && !getProject().get().isEmpty() && getProject().get() != null,
                "Could not obtain Cognite project name");

        checkArgument(
                (getApiKey() != null && getApiKey().isAccessible() && getApiKey().get() != null && !getApiKey().get().isBlank())
                        || (getClientId() != null && getClientId().isAccessible() && getClientId().get() != null && !getClientId().get().isBlank()
                        && getClientSecret() != null && getClientSecret().isAccessible() && getClientSecret().get() != null && !getClientSecret().get().isBlank()
                        && getTokenUrl() != null && getTokenUrl().isAccessible() && getTokenUrl().get() != null && !getTokenUrl().get().isBlank())
                ,
                "Could not obtain Cognite api key nor OpenID client credentials.");
    }

    @Override
    public final String toString() {
        return "ProjectConfig{"
                + "project=" + getProject() + ", "
                + "apiKey=" + "*********" + ", "
                + "host=" + getHost() + ", "
                + "apiKeyGcpSecretConfig=" + getApiKeyGcpSecretConfig() + ", "
                + "clientId=" + getClientId() + ", "
                + "clientSecret=" + "*********" + ", "
                + "authScopes=" + getAuthScopes() + ", "
                + "clientSecretGcpSecretConfig=" + getClientSecretGcpSecretConfig() + ", "
                + "tokenUrl=" + getTokenUrl() + ", "
                + "configured=" + isConfigured()
                + "}";
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract ProjectConfig build();

        abstract Builder setProject(ValueProvider<String> value);
        abstract Builder setHost(ValueProvider<String> value);
        abstract Builder setApiKey(ValueProvider<String> value);
        abstract Builder setClientId(ValueProvider<String> value);
        abstract Builder setClientSecret(ValueProvider<String> value);
        abstract Builder setTokenUrl(ValueProvider<String> value);
        abstract Builder setAuthScopes(ValueProvider<Collection<String>> value);
        abstract Builder setConfigured(boolean value);
        abstract Builder setApiKeyGcpSecretConfig(GcpSecretConfig value);
        abstract Builder setClientSecretGcpSecretConfig(GcpSecretConfig value);
    }
}
