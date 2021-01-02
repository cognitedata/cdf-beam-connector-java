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

package com.cognite.beam.io.fn.read;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tomlj.Toml;
import org.tomlj.TomlParseError;
import org.tomlj.TomlParseResult;

/**
 * Reads and parses a TOML config file.
 *
 * Returns the TOML as String keyed to the full file path/URL.
 */
public class ReadTomlConfigFileFn extends DoFn<FileIO.ReadableFile, KV<String, String>> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Setup
    public void setup() {
        LOG.info("Setting up ReadTomlConfigFileFn.");
    }

    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile file,
                               OutputReceiver<KV<String, String>> out) throws Exception {
        LOG.info("Received readable file: {}", file.toString());
        String filePayloadString = file.readFullyAsUTF8String();
        TomlParseResult parseResult = Toml.parse(filePayloadString);
        LOG.debug("Finish parsing toml file");

        if (parseResult.hasErrors()) {
            for (TomlParseError parseError : parseResult.errors()) {
                LOG.error("Error parsing project config file: {}", parseError.toString());
            }
            throw new Exception(parseResult.errors().get(0).getMessage());
        }
        out.output(KV.of(file.toString(), filePayloadString));
    }
}
