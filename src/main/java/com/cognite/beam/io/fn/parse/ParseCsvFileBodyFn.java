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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cognite.client.servicesV1.util.CharSplitter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

/**
 * Reads and parses a TOML config file.
 * <p>
 * Returns the TOML as String keyed to the full file path/URL.
 */
public class ParseCsvFileBodyFn extends DoFn<String, Struct> {
  private final static CharSplitter splitter = new CharSplitter();
  private final static String BOM_STRING = "\uFEFF"; // Unicode char representing the BOM

  private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  private final ValueProvider<String> delimiter;
  private final PCollectionView<Map<String, List<String>>> headerMapView;

  public ParseCsvFileBodyFn(ValueProvider<String> delimiter,
      PCollectionView<Map<String, List<String>>> headerMapView) {
    this.delimiter = delimiter;
    this.headerMapView = headerMapView;
  }

  @Setup
  public void setup() {
    LOG.info("Setting up ParseCsvFileBodyFn.");
    Preconditions.checkState(delimiter.isAccessible(),
        "Delimiter is not accessible from the ValueProvider.");
  }

  @ProcessElement
  public void processElement(@Element String textLine,
            OutputReceiver<Struct> out,
            ProcessContext context) throws Exception {
      Map<String, List<String>> headerMap = context.sideInput(headerMapView);
      LOG.debug("Received line to parse: {}", textLine);

    // Check the string for BOM (byte order mark) and remove it if present
    String input = textLine;
    if (input.startsWith(BOM_STRING)) {
      LOG.info("Byte order mark detected. Will remove it.");
      input = input.substring(1);
    }

    // Identify the relevant header. Will just use the first header.
    // Later we may implement support for using different headers with different input files.
    List<String> header = ImmutableList.<String>of();
    if (!headerMap.isEmpty()) {
      header = headerMap.values().iterator().next();
    }

    // Break the text line up into the individual components.
    ImmutableList<String> lineComponents =
            ImmutableList.copyOf(splitter.split(delimiter.get().charAt(0), input));

    // If there is only one component (no delimiter detected), check that this isn't an empty line
    if (lineComponents.size() < 2) {
        LOG.info("No delimiter in text line. Checking if it is an empty string...");
        boolean foundAlphabetChar = false;
        for (String component : lineComponents) {
            for (int codePoint : component.codePoints().toArray()) {
                if (Character.isAlphabetic(codePoint)) {
                    foundAlphabetChar = true;
                }
            }
        }
        if (!foundAlphabetChar) {
            LOG.warn("Empty text line, or no alphabetic characters. Skipping this line. Text line: " + lineComponents);
            return;
        } else {
            LOG.info("Found alphabetic characters. Will keep this string. Text line: " + lineComponents);
        }
    }

    // Check if this line is a header
    if (lineComponents.equals(header)) {
      LOG.info("Text line equals the header. Will not be added as payload. Text: {}",
          lineComponents);
      return;
    }
    LOG.debug("Completed parsing of text line: {}", lineComponents);
    Map<String, Value> refinery = new HashMap<>();

    // Build the Struct (concatenate same keys)
    for (int i = 0; i < Math.max(header.size(), lineComponents.size()); i++) {
      String key = i < header.size() ? header.get(i).trim() : "col_" + i;
      final Value.Builder valueBuilder = Value.newBuilder();
      final String value = i < lineComponents.size() ? lineComponents.get(i).trim() : "";
      if (!refinery.containsKey(key)) {
        valueBuilder.setStringValue(value);
      } else {
        String existing = refinery.get(key).getStringValue();
        String newValue = existing + value;
        valueBuilder.setStringValue(newValue);
      }

      refinery.put(key, valueBuilder.build());
    }
    refinery.entrySet().stream()
        .map(e -> String.join(":", e.getKey(), e.getValue().getStringValue()))
        .forEach(e -> LOG.debug("Completed building struct output: {}", e));

    out.output(Struct.newBuilder().putAllFields(refinery).build());
  }
}
