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

package com.cognite.client.config;

public enum ResourceType {
  ASSET,
  ASSETS_AGGREGATES,
  ASSETS_BY_ID,
  SEQUENCE_HEADER,
  SEQUENCE_AGGREGATES,
  SEQUENCE_BY_ID,
  SEQUENCE_BODY,
  THREED_MODEL_HEADER,
  TIMESERIES_HEADER,
  TIMESERIES_AGGREGATES,
  TIMESERIES_BY_ID,
  TIMESERIES_DATAPOINTS,
  EVENT,
  EVENT_AGGREGATES,
  EVENT_BY_ID,
  FILE_HEADER,
  FILE_AGGREGATES,
  FILE_BY_ID,
  FILE,
  RAW_DB,
  RAW_TABLE,
  RAW_ROW,
  DATA_SET,
  RELATIONSHIP,
  RELATIONSHIP_BY_ID,
  LABEL
}
