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
import com.cognite.beam.io.RequestParameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Matches a set of entities using an entity matcher ML model.
 *
 * This function will match the inbound entities with a given ML modelId. The candidate matches are defined by
 * the ML model itself--in practice this means the [matchTo] entities that were defined when the model
 * was trained.
 *
 * If you want to explicitly specify the collection of candidate matches, use the {@link MatchEntitiesWithContextFn}
 * function.
 *
 */
public class MatchEntitiesFn extends MatchEntitiesBaseFn {

    public MatchEntitiesFn(Hints hints,
                           ReaderConfig readerConfig,
                           PCollectionView<List<ProjectConfig>> projectConfigView,
                           @Nullable ValueProvider<Long> modelId,
                           @Nullable ValueProvider<String> modelExternalId) {
        super(hints, readerConfig, projectConfigView, modelId, modelExternalId);
    }

    public MatchEntitiesFn(Hints hints,
                           ReaderConfig readerConfig,
                           PCollectionView<List<ProjectConfig>> projectConfigView,
                           @Nullable ValueProvider<Long> modelId,
                           @Nullable ValueProvider<String> modelExternalId,
                           int maxNumMatches,
                           double scoreThreshold) {
        super(hints, readerConfig, projectConfigView, modelId, modelExternalId, maxNumMatches, scoreThreshold);
    }

    /**
     * Build the request for the entity matcher.
     *
     * @param element The list of entities to try and match from.
     * @param context The {@link ProcessContext}, offering access to side inputs.
     * @return The request for the entity matcher.
     */
    @Override
    protected RequestParameters buildRequestParameters(List<Struct> element,
                                                     ProcessContext context) {
        Preconditions.checkState(null != modelId || null != modelExternalId,
                "Neither model id nor model externalId is specified.");

        if (null != modelExternalId) {
            Preconditions.checkState(modelExternalId.isAccessible(),
                    "Parameter [modelExternalId] is not available.");
            return RequestParameters.create()
                    .withRootParameter("externalId", modelExternalId.get())
                    .withRootParameter("sources", ImmutableList.copyOf(element))
                    .withRootParameter("numMatches", maxNumMatches)
                    .withRootParameter("scoreThreshold", scoreThreshold);
        } else {
            Preconditions.checkState(modelId.isAccessible(), "Parameter [modelId] is not available.");
            return RequestParameters.create()
                    .withRootParameter("id", modelId.get())
                    .withRootParameter("sources", ImmutableList.copyOf(element))
                    .withRootParameter("numMatches", maxNumMatches)
                    .withRootParameter("scoreThreshold", scoreThreshold);
        }
    }
}
