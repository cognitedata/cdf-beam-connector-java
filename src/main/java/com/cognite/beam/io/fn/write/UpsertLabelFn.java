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
import com.cognite.beam.io.dto.Label;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.LabelParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;
import java.util.Map;

/**
 * Function for upserting {@link Label} to CDF.
 *
 * This function will first try to write the items as new items. In case the items already exists (based on externalId),
 * the item will be deleted and then created. Effectively this results in an upsert.
 *
 */
public class UpsertLabelFn extends UpsertItemViaDeleteBaseFn<Label> {

    public UpsertLabelFn(Hints hints, WriterConfig writerConfig,
                         PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, writerConfig, projectConfigView);
    }



    /**
     * Adds the individual elements into maps for externalId/id based de-duplication.
     *
     * @param element
     * @param externalIdMap
     */
    @Override
    protected void populateMaps(Iterable<Label> element,
                              Map<String, Label> externalIdMap) throws Exception {
        for (Label value : element) {
            externalIdMap.put(value.getExternalId(), value);
        }
    }

    /**
     * Build the resource specific insert item writer.
     * @return
     */
    @Override
    protected ConnectorServiceV1.ItemWriter getItemWriterInsert() {return connector.writeLabels();}

    /**
     * Build the resource specific delete item writer.
     * @return
     */
    @Override
    protected ConnectorServiceV1.ItemWriter getItemWriterDelete() {return connector.deleteLabels();}

    /**
     * Convert a collection of resource specific types into a generic representation of insert items.
     *
     * Insert items follow the Cognite API schema of inserting items of the resource type. The insert items
     * representation is a <code>List</code> of <code>Map<String, Object></code> where each <code>Map</code> represents
     * an insert object.
     *
     * @param input
     * @return
     * @throws Exception
     */
    @Override
    protected List<Map<String, Object>> toRequestInsertItems(Iterable<Label> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Label element : input) {
            listBuilder.add(LabelParser.toRequestInsertItem(element));
        }
        return listBuilder.build();
    }

    @Override
    protected List<Map<String, Object>> toRequestDeleteItems(Iterable<Label> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Label element : input) {
            listBuilder.add(ImmutableMap.of("externalId", element.getExternalId()));
        }
        return listBuilder.build();
    }
}
