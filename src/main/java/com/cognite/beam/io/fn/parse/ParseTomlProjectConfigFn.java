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
import org.tomlj.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Parses a String TOML config into a ProjectConfig object.
 *
 * It will look for the authentication configuration in the following order:
 * 1. OpenID Connect, client credentials.
 * 2. API keys.
 */
public class ParseTomlProjectConfigFn extends DoFn<String, ProjectConfig> {
    private final static String HOST_KEY = "host";
    private final static String CDF_PROJECT_KEY = "cdf_project";
    private final static String API_KEY = "api_key";
    private final static String CLIENT_ID_KEY = "client_id";
    private final static String CLIENT_SECRET_KEY = "client_secret";
    private final static String TOKEN_URL_KEY = "token_url";
    private final static String AUTH_SCOPES_KEY = "auth_scopes";

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ImmutableList<String> configKeyList = ImmutableList.of(HOST_KEY, CDF_PROJECT_KEY, API_KEY,
            CLIENT_ID_KEY, CLIENT_SECRET_KEY, TOKEN_URL_KEY, AUTH_SCOPES_KEY);
    private final ImmutableList<String> mandatoryConfigKeyListApiKey = ImmutableList.of(API_KEY);
    private final ImmutableList<String> mandatoryConfigKeyListClientCredentials =
            ImmutableList.of(CLIENT_ID_KEY, CLIENT_SECRET_KEY, TOKEN_URL_KEY, CDF_PROJECT_KEY);

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

        // Collect the valid config keys from the file.
        List<String> keys = new ArrayList<>();
        for (String keyName : configKeyList) {
            if (configTable.contains(keyName)) {
                keys.add(keyName);
            }
        }
        LOG.info("Found {} keys in the project configuration setting", keys.size());

        // Build the project config. OpenID takes precedence over api keys
        // Check that the all mandatory keys are defined.
        ProjectConfig returnObject = ProjectConfig.create();
        if (keys.containsAll(mandatoryConfigKeyListClientCredentials)) {
            returnObject = returnObject
                    .withClientId(configTable.getString(CLIENT_ID_KEY))
                    .withClientSecret(configTable.getString(CLIENT_SECRET_KEY))
                    .withTokenUrl(configTable.getString(TOKEN_URL_KEY))
                    .withProject(configTable.getString(CDF_PROJECT_KEY));
            if (keys.contains(AUTH_SCOPES_KEY)) {
                TomlArray scopesArray = configTable.getArray(AUTH_SCOPES_KEY);
                List<String> scopes = new ArrayList<>();
                for (int i = 0; i < scopesArray.size(); i++) {
                    scopes.add(scopesArray.getString(i));
                }
                returnObject = returnObject.withAuthScopes(scopes);
            }

        } else if (keys.containsAll(mandatoryConfigKeyListApiKey)) {
            returnObject = returnObject
                    .withApiKey(configTable.getString(API_KEY));
            if (keys.contains(CDF_PROJECT_KEY)) {
                returnObject = returnObject
                        .withProject(configTable.getString(CDF_PROJECT_KEY));
            }
        } else {
            // Could not find any valid configs
            LOG.warn("Could not find the mandatory keys in the project configuration setting. Cannot build project config.");
            return;
        }

        // Add the optional host parameter if set.
        if (configTable.contains(HOST_KEY)) {
            returnObject = returnObject.withHost(configTable.getString(HOST_KEY));
        }
        out.output(returnObject);
    }
}
