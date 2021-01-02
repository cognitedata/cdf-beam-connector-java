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

/**
 * Base class for reader and writer config. Not intended to be used directly.
 *
 * This class collects common config attributes across readers and writers.
 */
public abstract class ConfigBase implements Serializable {
    final static String DEFAULT_APP_IDENTIFIER = "cognite-beam-sdk";
    final static String DEFAULT_SESSION_IDENTIFIER = "cognite-beam-sdk";
    final static boolean DEFAULT_ENABLE_METRICS = true;

    public abstract String getAppIdentifier();
    public abstract String getSessionIdentifier();
    public abstract boolean isMetricsEnabled();

    public static abstract class Builder<B extends Builder<B>> {
        abstract B setAppIdentifier(String value);
        abstract B setSessionIdentifier(String value);
        abstract B setMetricsEnabled(boolean value);
    }
}
