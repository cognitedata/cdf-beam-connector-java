/*
 * Copyright (c) 2021 Cognite AS
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
import com.cognite.client.dto.DiagramResponse;
import com.cognite.client.dto.Item;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Detects annotations and builds interactive engineering diagrams/P&ID (svg and png) from a PDF.
 *
 * The function detects entities (for example, assets) in the engineering diagrams and highlights them in the (optional)
 * SVG/PNG. The detected entities can be used to enrich the viewer experience in an application displaying
 * the engineering diagram/P&ID.
 *
 * Annotations are detected based on a side input of {@link List<Struct>}. {@code Struct.name} is used
 * for annotation matching.
 *
 * The input specifies which file (id) to process.
 *
 */
public class CreateInteractiveDiagramsFn extends IOBaseFn<Iterable<Item>, DiagramResponse> {

    private final static Logger LOG = LoggerFactory.getLogger(CreateInteractiveDiagramsFn.class);
    private final ReaderConfig readerConfig;
    private final PCollectionView<List<ProjectConfig>> projectConfigView;
    private final PCollectionView<List<Struct>> entitiesView;
    private final String searchField;
    private final boolean convertFile;
    private final boolean grayscale;
    private final boolean partialMatch;
    private final int minTokens;

    public CreateInteractiveDiagramsFn(Hints hints,
                                       ReaderConfig readerConfig,
                                       PCollectionView<List<ProjectConfig>> projectConfigView,
                                       PCollectionView<List<Struct>> entitiesView,
                                       String searchField,
                                       boolean convertFile) {
        this(hints, readerConfig, projectConfigView, entitiesView, searchField, convertFile,
                false, 2);
    }

    public CreateInteractiveDiagramsFn(Hints hints,
                                       ReaderConfig readerConfig,
                                       PCollectionView<List<ProjectConfig>> projectConfigView,
                                       PCollectionView<List<Struct>> entitiesView,
                                       String searchField,
                                       boolean convertFile,
                                       boolean partialMatch,
                                       int minTokens) {
        this(hints, readerConfig, projectConfigView, entitiesView, searchField, convertFile,
                partialMatch, minTokens, false);
    }

    public CreateInteractiveDiagramsFn(Hints hints,
                                       ReaderConfig readerConfig,
                                       PCollectionView<List<ProjectConfig>> projectConfigView,
                                       PCollectionView<List<Struct>> entitiesView,
                                       String searchField,
                                       boolean convertFile,
                                       boolean partialMatch,
                                       int minTokens,
                                       boolean grayscale) {
        super(hints);
        this.projectConfigView = projectConfigView;
        this.entitiesView = entitiesView;
        this.searchField = searchField;
        this.convertFile = convertFile;
        this.partialMatch = partialMatch;
        this.minTokens = minTokens;
        this.readerConfig = readerConfig;
        this.grayscale = grayscale;
    }

    @ProcessElement
    public void processElement(@Element Iterable<Item> element,
                               OutputReceiver<DiagramResponse> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "CreateInteractivePnIDFn - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        LOG.info(batchLogPrefix + "Received a batch of files to process.");
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
        LOG.debug(batchLogPrefix + "Input items: {}",
                ImmutableList.copyOf(element).toString());

        // Read the items
        List<Item> elementList = new ArrayList<>();
        element.forEach(item -> elementList.add(item));
        try {
            List<Struct> matchToList = context.sideInput(entitiesView);
            List<DiagramResponse> resultsItems = getClient(projectConfig, readerConfig)
                    .contextualization()
                    .engineeringDiagrams()
                    .detectAnnotations(elementList, matchToList, searchField, partialMatch, minTokens, false);

            if (convertFile) {
                resultsItems = getClient(projectConfig, readerConfig)
                        .contextualization()
                        .engineeringDiagrams()
                        .convert(resultsItems, grayscale);
            }

            if (readerConfig.isMetricsEnabled()) {
                apiBatchSize.update(resultsItems.size());
                apiLatency.update(Duration.between(batchStartInstant, Instant.now()).toMillis());
            }

            LOG.info(batchLogPrefix + "Completed processing of {} files within a duration of {}.",
                    resultsItems.size(),
                    Duration.between(batchStartInstant, Instant.now()).toString());

            resultsItems.forEach(result -> outputReceiver.output(result));
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
