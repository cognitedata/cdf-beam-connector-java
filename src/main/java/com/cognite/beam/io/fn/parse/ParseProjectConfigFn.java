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

package com.cognite.beam.io.fn.parse;

import com.cognite.beam.io.config.ProjectConfig;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tomlj.Toml;
import org.tomlj.TomlParseError;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses a String TOML config into a ProjectConfig object.
 */
public class ParseProjectConfigFn extends DoFn<String, ProjectConfig> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ImmutableList<String> configKeyList = ImmutableList.of("host", "project", "api_key");
    private final ImmutableList<String> mandatoryConfigKeyList = ImmutableList.of("project", "api_key");

    @Setup
    public void setup() {
        LOG.info("Setting up ProjectReadConfigFile.");
    }

    @ProcessElement
    public void processElement(@Element String tomlString,
                               OutputReceiver<ProjectConfig> out) throws Exception {
        LOG.debug("Received TOML string. Size: {}", tomlString.length());
        LOG.debug("Parsing TOML string");
        TomlParseResult parseResult = Toml.parse(tomlString);
        LOG.debug("Finish parsing toml string");

        if (parseResult.hasErrors()) {
            for (TomlParseError parseError : parseResult.errors()) {
                LOG.error("Error parsing TOML string: {}", parseError.toString());
            }
            throw new Exception(parseResult.errors().get(0).getMessage());
        }

        TomlTable configTable = parseResult.getTableOrEmpty("project");

        // Check that the all mandatory keys are defined.
        List<String> keys = new ArrayList<>(3);
        for (String keyName : configKeyList) {
            if (configTable.contains(keyName)) {
                keys.add(keyName);
            }
        }
        LOG.info("Found {} keys in the project configuration setting", keys.size());
        if (keys.size() == 0) {
            LOG.warn("Could not find any valid keys in the project configuration setting.");
        }
        if (!keys.containsAll(mandatoryConfigKeyList)) {
            LOG.warn("Could not find all mandatory keys in the project configuration setting.");
            return;
        }

        ProjectConfig returnObject = ProjectConfig.create()
                .withProject(configTable.getString("project"))
                .withApiKey(configTable.getString("api_key"));
        if (configTable.contains("host")) {
            returnObject = returnObject.withHost(configTable.getString("host"));
        }
        out.output(returnObject);
    }
}
