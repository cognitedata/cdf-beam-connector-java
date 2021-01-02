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

package com.cognite.beam.io;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the various connectors (asset, event, ts, raw, etc.).
 *
 * This class collects the set of common attributes across all connectors. The individual connectors
 * will automatically pick these up via the autovalue generator.
 *
 * @param <InputT>
 * @param <OutputT>
 */
public abstract class ConnectorBase<InputT extends PInput,OutputT extends POutput> extends PTransform<InputT, OutputT> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public abstract ProjectConfig getProjectConfig();
    public abstract Hints getHints();
    public abstract ValueProvider<String> getProjectConfigFile();

    public static abstract class Builder<B extends Builder<B>> {
        public abstract B setProjectConfig(ProjectConfig value);
        public abstract B setHints(Hints value);
        public abstract B setProjectConfigFile(ValueProvider<String> value);
    }
}
