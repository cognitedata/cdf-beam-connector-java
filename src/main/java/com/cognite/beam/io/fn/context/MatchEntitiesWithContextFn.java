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
import com.cognite.beam.io.servicesV1.RequestParameters;
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
 * the side input. The side input must take the form of {@code PCollectionView<List<Struct>>}.
 *
 * If you want to use the candidate matches from the ML model itself, use the {@link MatchEntitiesFn}
 * function.
 */
public class MatchEntitiesWithContextFn extends MatchEntitiesBaseFn {
    private final PCollectionView<List<Struct>> matchToView;

    public MatchEntitiesWithContextFn(Hints hints,
                                      ReaderConfig readerConfig,
                                      PCollectionView<List<ProjectConfig>> projectConfigView,
                                      @Nullable ValueProvider<Long> modelId,
                                      @Nullable ValueProvider<String> modelExternalId,
                                      PCollectionView<List<Struct>> matchToView) {
        super(hints, readerConfig, projectConfigView, modelId, modelExternalId);
        this.matchToView = matchToView;
    }

    public MatchEntitiesWithContextFn(Hints hints,
                                      ReaderConfig readerConfig,
                                      PCollectionView<List<ProjectConfig>> projectConfigView,
                                      @Nullable ValueProvider<Long> modelId,
                                      @Nullable ValueProvider<String> modelExternalId,
                                      int maxNumMatches,
                                      double scoreThreshold,
                                      PCollectionView<List<Struct>> matchToView) {
        super(hints, readerConfig, projectConfigView, modelId, modelExternalId, maxNumMatches, scoreThreshold);
        this.matchToView = matchToView;
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
        List<Struct> matchToList = context.sideInput(matchToView);

        if (null != modelExternalId) {
            Preconditions.checkState(modelExternalId.isAccessible(),
                    "Parameter [modelExternalId] is not available.");
            return RequestParameters.create()
                    .withRootParameter("externalId", modelExternalId.get())
                    .withRootParameter("sources", ImmutableList.copyOf(element))
                    .withRootParameter("targets", ImmutableList.copyOf(matchToList))
                    .withRootParameter("numMatches", maxNumMatches)
                    .withRootParameter("scoreThreshold", scoreThreshold);
        } else {
            Preconditions.checkState(modelId.isAccessible(), "Parameter [modelId] is not available.");
            return RequestParameters.create()
                    .withRootParameter("id", modelId.get())
                    .withRootParameter("sources", ImmutableList.copyOf(element))
                    .withRootParameter("targets", ImmutableList.copyOf(matchToList))
                    .withRootParameter("numMatches", maxNumMatches)
                    .withRootParameter("scoreThreshold", scoreThreshold);
        }
    }
}
