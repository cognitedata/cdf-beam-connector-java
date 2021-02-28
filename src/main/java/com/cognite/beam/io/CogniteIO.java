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

package com.cognite.beam.io;

import com.cognite.beam.io.config.Hints;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CogniteIO {
    static final Hints defaultHints = Hints.create();
    static final RequestParameters emptyRequestParameters = RequestParameters.create();
    static final ValueProvider.StaticValueProvider<String> invalidProjectConfigFile =
            ValueProvider.StaticValueProvider.of(".");

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    /**
     * This class should not be instantiated. Instead, use the factory methods to produce specific reader and writer
     * objects.
     */
    private CogniteIO() {
    }

    /**
     * Creates an uninitialized Cognite.Assets.Read. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Assets.Read readAssets() {
        return Assets.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.ReadAllRow. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Assets.ReadAll readAllAssets() {
        return Assets.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.ReadAllById. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Assets.ReadAllById readAllAssetsByIds() {
        return Assets.ReadAllById.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.ReadAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Assets.ReadAggregate readAggregatesAssets() {
        return Assets.ReadAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.ReadAllAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Assets.ReadAllAggregate readAllAggregatesAssets() {
        return Assets.ReadAllAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Assets.Write writeAssets() {
        return Assets.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.WriteMultipleHierarchies. Before use, the writer must be initialized
     * with a configuration object.
     */
    public static Assets.WriteMultipleHierarchies writeAssetsMultipleHierarchies() {
        return Assets.WriteMultipleHierarchies.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.SynchronizeHierarchies. Before use, the writer must be initialized
     * with a configuration object.
     */
    public static Assets.SynchronizeHierarchies synchronizeHierarchies() {
        return Assets.SynchronizeHierarchies.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.Delete. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Assets.Delete deleteAssets() {
        return Assets.Delete.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Events.Read Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Events.Read readEvents() {
        return Events.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Events.ReadAll. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Events.ReadAll readAllEvents() {
        return Events.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Events.ReadAllById. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Events.ReadAllById readAllEventsByIds() {
        return Events.ReadAllById.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Events.ReadAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Events.ReadAggregate readAggregatesEvents() {
        return Events.ReadAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Events.ReadAllAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Events.ReadAllAggregate readAllAggregatesEvents() {
        return Events.ReadAllAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Events.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Events.Write writeEvents() {
        return Events.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Events.Delete. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Events.Delete deleteEvents() {
        return Events.Delete.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Files.Read. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Files.Read readFiles() {
        return Files.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Files.ReadAll. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Files.ReadAll readAllFiles() {
        return Files.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Files.ReadAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Files.ReadAggregate readAggregatesFiles() {
        return Files.ReadAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Files.ReadAllAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Files.ReadAllAggregate readAllAggregatesFiles() {
        return Files.ReadAllAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Files.Write. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Files.Write writeFiles() {
        return Files.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.FilesMetadata.Delete. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Files.Delete deleteFiles() {
        return Files.Delete.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.FilesMetadata.Read. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static FilesMetadata.Read readFilesMetadata() {
        return FilesMetadata.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.FilesMetadata.ReadAll. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static FilesMetadata.ReadAll readAllFilesMetadata() {
        return FilesMetadata.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Assets.ReadAllById. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static FilesMetadata.ReadAllById readAllFilesMetadataByIds() {
        return FilesMetadata.ReadAllById.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.FilesMetadata.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static FilesMetadata.Write writeFilesMetadata() {
        return FilesMetadata.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.FilesBinary.ReadAllById. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static FilesBinary.ReadAllById readAllFilesBinariesByIds() {
        return FilesBinary.ReadAllById.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.Read. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static TSMetadata.Read readTimeseriesMetadata() {
        return TSMetadata.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.ReadAll. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static TSMetadata.ReadAll readAllTimeseriesMetadata() {
        return TSMetadata.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.ReadAllById. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static TSMetadata.ReadAllById readAllTimeseriesMetadataByIds() {
        return TSMetadata.ReadAllById.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.ReadAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static TSMetadata.ReadAggregate readAggregatesTimeseriesMetadata() {
        return TSMetadata.ReadAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.ReadAllAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static TSMetadata.ReadAllAggregate readAllAggregatesTimeseriesMetadata() {
        return TSMetadata.ReadAllAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static TSMetadata.Write writeTimeseriesMetadata() {
        return TSMetadata.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.Delete. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static TSMetadata.Delete deleteTimeseries() {
        return TSMetadata.Delete.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSPoints.Read. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static TSPoints.Read readTimeseriesPoints() {
        return TSPoints.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSPoints.ReadAll. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static TSPoints.ReadAll readAllTimeseriesPoints() {
        return TSPoints.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSPoints.ReadAllDirect. Before use, the reader must be initialized with
     * a configuration object, and optionally query getQueryParameters.
     */
    public static TSPoints.ReadAllDirect readAllDirectTimeseriesPoints() {
        return TSPoints.ReadAllDirect.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static TSPoints.Write writeTimeseriesPoints() {
        return TSPoints.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.TSMetadata.WriteDirect. Before use, the writer must be initialized with a
     * configuration object.
     */
    public static TSPoints.WriteDirect writeDirectTimeseriesPoints() {
        return TSPoints.WriteDirect.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Sequences.Read. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Sequences.Read readSequencesMetadata() {
        return Sequences.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Sequences.ReadAllRow. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Sequences.ReadAll readAllSequencesMetadata() {
        return Sequences.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Sequences.ReadAllById. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Sequences.ReadAllById readAllSequencesMetadataByIds() {
        return Sequences.ReadAllById.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Sequences.ReadAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Sequences.ReadAggregate readAggregatesSequencesMetadata() {
        return Sequences.ReadAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Sequences.ReadAllAggregate. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Sequences.ReadAllAggregate readAllAggregatesSequencesMetadata() {
        return Sequences.ReadAllAggregate.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Sequences.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Sequences.Write writeSequencesMetadata() {
        return Sequences.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Sequences.Delete. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Sequences.Delete deleteSequences() {
        return Sequences.Delete.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.SequenceRows.Read. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static SequenceRows.Read readSequenceRows() {
        return SequenceRows.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.SequenceRows.ReadAll. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static SequenceRows.ReadAll readAllSequenceRows() {
        return SequenceRows.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.SequenceRows.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static SequenceRows.Write writeSequenceRows() {
        return SequenceRows.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.SequenceRows.Delete. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static SequenceRows.Delete deleteSequenceRows() {
        return SequenceRows.Delete.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Raw.ReadRow. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Raw.ReadRow readRawRow() {
        return Raw.ReadRow.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Raw.ReadAllRow. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Raw.ReadAllRow readAllRawRow() {
        return Raw.ReadAllRow.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Raw.WriteRow. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Raw.WriteRow writeRawRow() {
        return Raw.WriteRow.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Raw.WriteRowDirect. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Raw.WriteRowDirect writeRawRowDirect() {
        return Raw.WriteRowDirect.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Raw.DeleteRow. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Raw.DeleteRow deleteRawRow() {
        return Raw.DeleteRow.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Raw.ReadDatabase. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Raw.ReadDatabase readRawDatabase() {
        return Raw.ReadDatabase.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Raw.ReadTable. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Raw.ReadTable readRawTable() {
        return Raw.ReadTable.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Raw.ReadAllTable. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Raw.ReadAllTable readAllRawTable() {
        return Raw.ReadAllTable.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.DataSets.Read Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static DataSets.Read readDataSets() {
        return DataSets.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.DataSets.ReadAll. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static DataSets.ReadAll readAllDataSets() {
        return DataSets.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.DataSets.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static DataSets.Write writeDataSets() {
        return DataSets.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.Read Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Relationships.Read readRelationships() {
        return Relationships.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.ReadAll. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Relationships.ReadAll readAllRelationships() {
        return Relationships.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.ReadAllById. Before use, the reader must be initialized with a configuration
     * object.
     */
    public static Relationships.ReadAllById readAllRelationshipsByIds() {
        return Relationships.ReadAllById.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Relationships.Write writeRelationships() {
        return Relationships.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.Delete. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Relationships.Delete deleteRelationships() {
        return Relationships.Delete.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.Read Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Labels.Read readLabels() {
        return Labels.Read.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.ReadAll. Before use, the reader must be initialized with a configuration
     * object, and optionally query getQueryParameters.
     */
    public static Labels.ReadAll readAllLabels() {
        return Labels.ReadAll.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.Write. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Labels.Write writeLabels() {
        return Labels.Write.builder().build();
    }

    /**
     * Creates an uninitialized Cognite.Relationships.Delete. Before use, the writer must be initialized with a configuration
     * object.
     */
    public static Labels.Delete deleteLabels() {
        return Labels.Delete.builder().build();
    }

    /**
     * Creates an entity matcher for matching {@link com.google.protobuf.Struct} entities using a matching model.
     *
     * You must configure the {@link com.cognite.beam.io.Context.MatchStructEntities} transform
     * with a matching model id.
     *
     * @return An entity matcher transform for matching {@link com.google.protobuf.Struct} entities.
     */
    public static Context.MatchStructEntities matchStructEntities() {
        return Context.MatchStructEntities.create();
    }

    /**
     * Creates an entity matcher for matching entities of type {@code T} using a matching model.
     *
     * You must configure the {@link com.cognite.beam.io.Context.MatchStructEntities} transform
     * with a matching model id.
     *
     * @return An entity matcher transform for matching {@link com.google.protobuf.Struct} entities.
     */
    public static <T> Context.MatchEntities<T> matchEntities() {
        return Context.MatchEntities.<T>create();
    }

    /**
     * Creates a transform for detecting annotations/entities in a PDF P&ID file and optionally
     * generate SVG and PNG representations of that document.
     *
     * You must configure the {@link com.cognite.beam.io.Context.CreateInteractivePnID} transform
     * with target {@code MatchTo} set of target entities to use for matching.
     *
     * @return A transform for creating interactive P&IDs.
     */
    public static Context.CreateInteractivePnID createInteractivePnID() {
        return Context.CreateInteractivePnID.create();
    }
}
