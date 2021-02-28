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

package com.cognite.beam.io.fn.context;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.cognite.client.CogniteClient;
import com.cognite.client.dto.EntityMatch;
import com.cognite.client.dto.EntityMatchResult;
import com.google.protobuf.Struct;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for matching a set of entities using an entity matcher ML model.
 *
 * This function will match the inbound entities with a given ML modelId. It outputs the results as
 * a {@code KV<Struct, List<EntityMatch>} where the key is the inbound entity and the value contains
 * the candidate matches.
 *
 * The entities are specified as a {@link Struct} which is the protobuf equivalent of a JSON object. Match
 * candidates are also specified as {@link Struct} within the {@link EntityMatch} container object.
 *
 */
public abstract class MatchEntitiesBaseFn extends IOBaseFn<Iterable<Struct>, KV<Struct, List<EntityMatch>>> {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected final ReaderConfig readerConfig;
    protected final PCollectionView<List<ProjectConfig>> projectConfigView;
    protected final int maxNumMatches;
    protected final double scoreThreshold;

    @Nullable
    protected final ValueProvider<Long> modelId;
    @Nullable
    protected final ValueProvider<String> modelExternalId;

    public MatchEntitiesBaseFn(Hints hints,
                               ReaderConfig readerConfig,
                               PCollectionView<List<ProjectConfig>> projectConfigView,
                               @Nullable ValueProvider<Long> modelId,
                               @Nullable ValueProvider<String> modelExternalId) {
        this(hints, readerConfig, projectConfigView, modelId, modelExternalId, 1, 0d);
    }

    public MatchEntitiesBaseFn(Hints hints,
                               ReaderConfig readerConfig,
                               PCollectionView<List<ProjectConfig>> projectConfigView,
                               @Nullable ValueProvider<Long> modelId,
                               @Nullable ValueProvider<String> modelExternalId,
                               int maxNumMatches,
                               double scoreThreshold) {
        super(hints);
        this.readerConfig = readerConfig;
        this.projectConfigView = projectConfigView;
        this.modelId = modelId;
        this.modelExternalId = modelExternalId;
        this.maxNumMatches = maxNumMatches;
        this.scoreThreshold = scoreThreshold;
    }

    @ProcessElement
    public void processElement(@Element Iterable<Struct> element,
                               OutputReceiver<KV<Struct, List<EntityMatch>>> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "matchEntitiesBaseFn - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        LOG.info(batchLogPrefix + "Received a batch of entities to match.");
        Instant batchStartInstant = Instant.now();

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchLogPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // Read the items
        List<Struct> elementList = new ArrayList<>();
        element.forEach(item -> elementList.add(item));
        try {
            List<EntityMatchResult> resultsItems = predictMatches(getClient(projectConfig, readerConfig), elementList, context);
            if (readerConfig.isMetricsEnabled()) {
                apiBatchSize.update(resultsItems.size());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }

            LOG.info(batchLogPrefix + "Completed matching {} items within a duration of {}.",
                    resultsItems.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            resultsItems.forEach(result -> outputReceiver.output(KV.of(result.getSource(), result.getMatchesList())));
        } catch (Exception e) {
            LOG.error("Error reading results from the Cognite connector.", e);
            throw e;
        }
    }

    /**
     * Perform the entity matching.
     *
     * @param client The {@link CogniteClient} to use for writing the items.
     * @param element The list of entities to try and match from.
     * @param context The {@link ProcessContext}, offering access to side inputs.
     * @return The request for the entity matcher.
     */
    protected abstract List<EntityMatchResult> predictMatches(CogniteClient client,
                                                              List<Struct> element,
                                                              ProcessContext context) throws Exception;
}