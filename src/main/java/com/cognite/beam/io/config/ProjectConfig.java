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

import java.io.Serializable;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.ValueProvider;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.*;

@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class ProjectConfig implements Serializable {
  private final static String DEFAULT_HOST = "https://api.cognitedata.com";
  private final static String DEFAULT_PROJECT = "undefined";
  private final static String DEFAULT_API_KEY = "undefined";

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
  public abstract ValueProvider<String> getHost();
  public abstract boolean isConfigured();
  @Nullable  public abstract GcpSecretConfig getGcpSecretConfig();

  public abstract ProjectConfig.Builder toBuilder();

  /**
   * Returns a new {@code ProjectConfig} that represents the specified host.
   * @param value The project id interact with.
   */
  public ProjectConfig withHost(ValueProvider<String> value) {
    return toBuilder().setHost(value).setConfigured(true).build();
  }

  /**
   * Returns a new {@code ProjectConfig} that represents the specified host.
   * @param value The project id interact with.
   */
  public ProjectConfig withHost(String value) {
    return withHost(ValueProvider.StaticValueProvider.of(value));
  }

  /**
   * Returns a new {@code ProjectConfig} that represents the specified project.
   * @param value The project id interact with.
   */
  public ProjectConfig withProject(ValueProvider<String> value) {
    return toBuilder().setProject(value).setConfigured(true).build();
  }

  /**
   * Returns a new {@code ProjectConfig} that represents the specified project.
   * @param value The project id interact with.
   */
  public ProjectConfig withProject(String value) {
    return withProject(ValueProvider.StaticValueProvider.of(value));
  }

  /**
   * Returns a new {@code ProjectConfig} that represents the specified api key.
   * @param value The project id interact with.
   */
  public ProjectConfig withApiKey(ValueProvider<String> value) {
    return toBuilder().setApiKey(value).setConfigured(true).build();
  }

  /**
   * Returns a new {@code ProjectConfig} that represents the specified api key.
   * @param value The project id interact with.
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
    return toBuilder().setGcpSecretConfig(config).setConfigured(true).build();
  }

  public void validate() {
    checkState(isConfigured(), "ProjectConfig parameters have not been configured.");
    checkArgument(getHost() != null
                    && getHost().isAccessible() && !getHost().get().isEmpty(),
            "Could not obtain Cognite host name");

    checkArgument(getProject() != null
                    && getProject().isAccessible() && !getProject().get().isEmpty() && getProject().get() != null,
            "Could not obtain Cognite project name");

    checkArgument(getApiKey() != null
                    && getApiKey().isAccessible() && !getApiKey().get().isEmpty() && getApiKey().get() != null,
            "Could not obtain Cognite api key");
  }

  @Override
  public final String toString() {
    return "ProjectConfig{"
            + "project=" + getProject() + ", "
            + "apiKey=" + "*********" + ", "
            + "host=" + getHost() + ", "
            + "GcpSecretConfig=" + getGcpSecretConfig() + ", "
            + "configured=" + isConfigured()
            + "}";
  }

  @AutoValue.Builder public abstract static class Builder {

    public abstract ProjectConfig build();

    abstract Builder setProject(ValueProvider<String> value);
    abstract Builder setHost(ValueProvider<String> value);
    abstract Builder setApiKey(ValueProvider<String> value);
    abstract Builder setConfigured(boolean value);
    abstract Builder setGcpSecretConfig(GcpSecretConfig value);
  }
}
