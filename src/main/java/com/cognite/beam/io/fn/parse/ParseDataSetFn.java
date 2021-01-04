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

import com.cognite.beam.io.dto.DataSet;
import com.cognite.client.servicesV1.parser.DataSetParser;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parse Cognite result json items to {@link DataSet}.
 */
public class ParseDataSetFn extends DoFn<String, DataSet> {
  private final Logger LOG = LoggerFactory.getLogger(ParseDataSetFn.class);
  private final String parseErrorDefaultPrefix = "Parsing error. Unable to parse result item. ";

  @Setup
  public void setup() {
    LOG.debug("Setting up ParseDataSetFn.");
  }

  @ProcessElement
  public void processElement(@Element String inputElement, OutputReceiver<DataSet> out) throws Exception {
    try {
      out.output(DataSetParser.parseDataSet(inputElement));
    } catch (Exception e) {
      LOG.error(parseErrorDefaultPrefix, e);
      throw e;
    }
  }
}
