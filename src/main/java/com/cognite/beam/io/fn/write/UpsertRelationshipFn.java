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
import com.cognite.beam.io.config.UpsertMode;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.Event;
import com.cognite.beam.io.dto.Item;
import com.cognite.beam.io.dto.Relationship;
import com.cognite.beam.io.servicesV1.ConnectorServiceV1;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.ResponseItems;
import com.cognite.beam.io.servicesV1.parser.EventParser;
import com.cognite.beam.io.servicesV1.parser.ItemParser;
import com.cognite.beam.io.servicesV1.parser.RelationshipParser;
import com.cognite.beam.io.util.internal.MetricsUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Function for upserting {@link Relationship} to CDF. {@link Relationship} requires special handling for upserts
 * so we cannot use the standard logic in {@link UpsertItemBaseFn}.
 *
 * This function will first try to write the items as new items. In case the items already exists (based on externalId),
 * the item will be deleted and then created. Effectively this results in an upsert.
 *
 */
public class UpsertRelationshipFn extends UpsertItemViaDeleteBaseFn<Relationship> {

    public UpsertRelationshipFn(Hints hints, WriterConfig writerConfig,
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
    protected void populateMaps(Iterable<Relationship> element,
                              Map<String, Relationship> externalIdMap) throws Exception {
        for (Relationship value : element) {
            externalIdMap.put(value.getExternalId(), value);
        }
    }

    /**
     * Build the resource specific insert item writer.
     * @return
     */
    @Override
    protected ConnectorServiceV1.ItemWriter getItemWriterInsert() {return connector.writeRelationships();}

    /**
     * Build the resource specific delete item writer.
     * @return
     */
    @Override
    protected ConnectorServiceV1.ItemWriter getItemWriterDelete() {return connector.deleteRelationships();}

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
    protected List<Map<String, Object>> toRequestInsertItems(Iterable<Relationship> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Relationship element : input) {
            listBuilder.add(RelationshipParser.toRequestInsertItem(element));
        }
        return listBuilder.build();
    }

    @Override
    protected List<Map<String, Object>> toRequestDeleteItems(Iterable<Relationship> input) throws Exception {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        for (Relationship element : input) {
            listBuilder.add(ImmutableMap.of("externalId", element.getExternalId()));
        }
        return listBuilder.build();
    }
}
