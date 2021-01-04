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

package com.cognite.beam.io.fn.write;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.Event;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.EventParser;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.*;

/**
 * Writes events to CDF.Clean.
 *
 * This function will first try to write the events as new events. In case the events already exists (based on externalId
 * or Id), the events will be updated. Effectively this results in an upsert.
 *
 */
public class UpsertEventFn extends UpsertItemBaseFn<Event> {
    public UpsertEventFn(Hints hints, WriterConfig writerConfig, PCollectionView<List<ProjectConfig>> projectConfig) {
        super(hints, writerConfig, projectConfig);
    }

    @Override
    protected void populateMaps(Iterable<Event> element, Map<Long, Event> internalIdInsertMap,
                      Map<String, Event> externalIdInsertMap) throws Exception {
        for (Event value : element) {
            if (value.hasExternalId()) {
                externalIdInsertMap.put(value.getExternalId().getValue(), value);
            } else if (value.hasId()) {
                internalIdInsertMap.put(value.getId().getValue(), value);
            } else {
                throw new Exception("Item does not contain id nor externalId: " + value.toString());
            }
        }
    }

    @Override
    protected ConnectorServiceV1.ItemWriter getItemWriterInsert() {
        return connector.writeEvents();
    }

    @Override
    protected ConnectorServiceV1.ItemWriter getItemWriterUpdate() {
        return connector.updateEvents();
    }

    @Override
    protected List<Map<String, Object>> toRequestInsertItems(Iterable<Event> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Event element : input) {
            listBuilder.add(EventParser.toRequestInsertItem(element));
        }
        return listBuilder.build();
    }

    @Override
    protected List<Map<String, Object>> toRequestUpdateItems(Iterable<Event> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Event element : input) {
            listBuilder.add(EventParser.toRequestUpdateItem(element));
        }
        return listBuilder.build();
    }

    @Override
    protected List<Map<String, Object>> toRequestReplaceItems(Iterable<Event> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Event element : input) {
            listBuilder.add(EventParser.toRequestReplaceItem(element));
        }
        return listBuilder.build();
    }
}
