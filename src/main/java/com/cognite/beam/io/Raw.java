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
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.read.*;
import com.cognite.client.dto.RawRow;
import com.cognite.client.dto.RawTable;
import com.cognite.client.config.ResourceType;
import com.cognite.beam.io.fn.delete.DeleteRawRowFn;
import com.cognite.beam.io.fn.request.GenerateReadRequestsUnboundFn;
import com.cognite.beam.io.fn.write.UpsertRawRowFn;
import com.cognite.beam.io.transform.internal.*;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.cognite.beam.io.CogniteIO.*;

public class Raw {

    @AutoValue
    public abstract static class ReadRow extends ConnectorBase<PBegin, PCollection<RawRow>> {
        private static final ValueProvider<String> INVALID_DB_NAME = ValueProvider.StaticValueProvider.of("_____");
        private static final ValueProvider<String> INVALID_TABLE_NAME = ValueProvider.StaticValueProvider.of("_____");

        public static ReadRow.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_ReadRow.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setRequestParameters(CogniteIO.emptyRequestParameters)
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setDbName(INVALID_DB_NAME)
                    .setTableName(INVALID_TABLE_NAME);
        }

        public abstract RequestParameters getRequestParameters();
        public abstract ReaderConfig getReaderConfig();
        public abstract ValueProvider<String> getDbName();
        public abstract ValueProvider<String> getTableName();

        public abstract ReadRow.Builder toBuilder();

        public ReadRow withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadRow withRequestParameters(RequestParameters params) {
            Preconditions.checkNotNull(params, "Parameters cannot be null.");
            return toBuilder().setRequestParameters(params).build();
        }

        public ReadRow withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadRow withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public ReadRow withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public ReadRow withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public ReadRow withDbName(ValueProvider<String> dbName) {
            Preconditions.checkNotNull(dbName, "Database name cannot be null");
            return toBuilder().setDbName(dbName).build();
        }

        public ReadRow withDbName(String dbName) {
            Preconditions.checkNotNull(dbName, "Database name cannot be null");
            Preconditions.checkArgument(!dbName.isEmpty(), "Database name cannot be empty.");
            return this.withDbName(ValueProvider.StaticValueProvider.of(dbName));
        }

        public ReadRow withTableName(ValueProvider<String> tableName) {
            Preconditions.checkNotNull(tableName, "Table name cannot be null");
            return toBuilder().setTableName(tableName).build();
        }

        public ReadRow withTableName(String tableName) {
            Preconditions.checkNotNull(tableName, "Table name cannot be null");
            Preconditions.checkArgument(!tableName.isEmpty(), "Table name cannot be empty.");
            return this.withTableName(ValueProvider.StaticValueProvider.of(tableName));
        }

        @Override
        public PCollection<RawRow> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");

            PCollection<RawRow> outputCollection = input.getPipeline()
                    .apply("Generate Query", Create.of(getRequestParameters()))
                    .apply("Apply db/table name", ParDo.of(new DoFn<RequestParameters, RequestParameters>() {

                        @ProcessElement
                        public void processElement(@Element RequestParameters inputRequest,
                                                   OutputReceiver<RequestParameters> out) {
                            RequestParameters requestParameters = inputRequest;
                            if (!getDbName().get().equals(INVALID_DB_NAME.get())) {
                                LOG.info("Database name specified in the reader config: " + getDbName().get());
                                requestParameters = requestParameters.withDbName(getDbName().get());
                            }
                            if (!getTableName().get().equals(INVALID_TABLE_NAME.get())) {
                                LOG.info("Table name specified in the reader config: " + getTableName().get());
                                requestParameters = requestParameters.withTableName(getTableName().get());
                            }
                            out.output(requestParameters);
                        }
                    }))
                    .apply("Read results", CogniteIO.readAllRawRow()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                    );
            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract ReadRow.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadRow.Builder setRequestParameters(RequestParameters value);
            public abstract ReadRow.Builder setDbName(ValueProvider<String> value);
            public abstract ReadRow.Builder setTableName(ValueProvider<String> value);

            public abstract ReadRow build();
        }
    }

    @Experimental(Experimental.Kind.SOURCE_SINK)
    @AutoValue
    public abstract static class ReadAllRow
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<RawRow>> {

        public static ReadAllRow.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_ReadAllRow.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract ReadAllRow.Builder toBuilder();

        public ReadAllRow withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAllRow withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadAllRow withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public ReadAllRow withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public ReadAllRow withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<RawRow> expand(PCollection<RequestParameters> input) {

            // Read from Raw
            PCollection<RawRow> outputCollection = input
                    .apply("Read rows direct", CogniteIO.readAllDirectRawRow()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig())
                            .withHints(getHints()))
                    .apply("Unwrap rows", ParDo.of(new DoFn<List<RawRow>, RawRow>() {
                        @ProcessElement
                        public void processElement(@Element List<RawRow> element,
                                                   OutputReceiver<RawRow> out) {
                            if (getReaderConfig().isStreamingEnabled()) {
                                // output with timestamp
                                element.forEach(row -> out.outputWithTimestamp(row,
                                        org.joda.time.Instant.ofEpochMilli(row.getLastUpdatedTime())));
                            } else {
                                // output without timestamp
                                element.forEach(row -> out.output(row));
                            }
                        }
                    }))
                    ;



            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract ReadAllRow.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAllRow build();
        }
    }

    @Experimental(Experimental.Kind.SOURCE_SINK)
    @AutoValue
    public abstract static class ReadAllRowDirect
            extends ConnectorBase<PCollection<RequestParameters>, PCollection<List<RawRow>>> {

        public static ReadAllRowDirect.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_ReadAllRowDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract ReadAllRowDirect.Builder toBuilder();

        public ReadAllRowDirect withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAllRowDirect withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadAllRowDirect withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public ReadAllRowDirect withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public ReadAllRowDirect withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<List<RawRow>> expand(PCollection<RequestParameters> input) {
            Preconditions.checkState(!(getReaderConfig().isStreamingEnabled() && getReaderConfig().isDeltaEnabled()),
                    "Using delta read in combination with streaming is not supported.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // conditional streaming
            PCollection<RequestParameters> requestParametersPCollection;

            if (getReaderConfig().isStreamingEnabled()) {
                // streaming mode
                LOG.info("Setting up streaming mode");
                requestParametersPCollection = input
                        .apply("Watch for new items", ParDo.of(
                                new GenerateReadRequestsUnboundFn(getReaderConfig(), ResourceType.RAW_ROW)));
            } else {
                // batch mode
                LOG.info("Setting up batch mode");
                requestParametersPCollection = input;
            }

            // Add delta and cursors for batch reads
            PCollection<RequestParameters> finalizedConfig;
            if (getReaderConfig().isStreamingEnabled()) {
                finalizedConfig = requestParametersPCollection;
            } else {
                finalizedConfig = requestParametersPCollection
                        .apply("Apply delta timestamp", ApplyDeltaTimestamp.to(ResourceType.RAW_ROW)
                                .withProjectConfig(getProjectConfig())
                                .withProjectConfigFile(getProjectConfigFile())
                                .withReaderConfig(getReaderConfig()))
                        .apply("Read cursors", ParDo.of(new ReadCursorsFn(getHints(),
                                        getReaderConfig().enableMetrics(false), projectConfigView))
                                .withSideInputs(projectConfigView))
                        .apply("Break fusion", BreakFusion.<RequestParameters>create());
            }

            // Read from Raw
            PCollection<List<RawRow>> outputCollection = finalizedConfig
                    .apply("Read results", ParDo.of(
                                    new ReadRawRowBatch(getHints(), getReaderConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            // Record delta timestamp
            outputCollection
                    .apply("Extract last change timestamp", MapElements.into(TypeDescriptors.longs())
                            .via((List<RawRow> batch) -> batch.stream()
                                    .mapToLong(RawRow::getLastUpdatedTime)
                                    .max()
                                    .orElse(1L))
                    )
                    .apply("Record delta timestamp", RecordDeltaTimestamp.create()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withReaderConfig(getReaderConfig()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract ReadAllRowDirect.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAllRowDirect build();
        }
    }

    @AutoValue
    public abstract static class WriteRow extends ConnectorBase<PCollection<RawRow>, PCollection<RawRow>> {

        public static WriteRow.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_WriteRow.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract WriteRow.Builder toBuilder();

        public WriteRow withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public WriteRow withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public WriteRow withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public WriteRow withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public WriteRow withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<RawRow> expand(PCollection<RawRow> input) {
            LOG.debug("Building write raw row composite transform.");

            final Coder<String> utf8Coder = StringUtf8Coder.of();
            final Coder<RawRow> eventCoder = ProtoCoder.of(RawRow.class);
            final KvCoder<String, RawRow> keyValueCoder = KvCoder.of(utf8Coder, eventCoder);

            PCollection<RawRow> outputCollection = input
                    .apply("Shard rows", WithKeys.of((RawRow inputItem) ->
                            inputItem.getDbName()
                            + "-" + inputItem.getTableName()
                            + "-" + String.valueOf(ThreadLocalRandom.current().nextInt(getHints().getWriteShards()))
                    )).setCoder(keyValueCoder)
                    .apply("Batch rows", GroupIntoBatches.<String, RawRow>of(keyValueCoder)
                            .withMaxBatchSize(4000)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<RawRow>>create())
                    .apply("Write rows", CogniteIO.writeDirectRawRow()
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(getWriterConfig())
                            .withHints(getHints()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract WriteRow.Builder setWriterConfig(WriterConfig value);
            public abstract WriteRow build();
        }
    }

    /**
     * Writes Raw Rows directly to the Cognite API, bypassing the regular validation and optimization steps. This
     * writer is designed for advanced use with very large data volumes (100+ million items). Most use cases should
     * use the regular {@link Raw.WriteRow} writer which will perform shuffling and batching to optimize
     * the write performance.
     *
     * This writer will push each input {@link Iterable< RawRow >} as a single batch. If the input
     * violates any constraints, the write will fail. Also, the performance of the writer depends heavily on the
     * input being batched as optimally as possible.
     *
     * If your source system offers data pre-batched, you may get additional performance from this writer as
     * it bypasses the regular shuffle and batch steps.
     */
    @AutoValue
    public abstract static class WriteRowDirect
            extends ConnectorBase<PCollection<? extends Iterable<RawRow>>, PCollection<RawRow>> {

        public static WriteRowDirect.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_WriteRowDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract WriteRowDirect.Builder toBuilder();

        public WriteRowDirect withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public WriteRowDirect withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public WriteRowDirect withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public WriteRowDirect withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public WriteRowDirect withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<RawRow> expand(PCollection<? extends Iterable<RawRow>> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<RawRow> outputCollection = input
                    .apply("Write rows", ParDo.of(
                            new UpsertRawRowFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract WriteRowDirect.Builder setWriterConfig(WriterConfig value);
            public abstract WriteRowDirect build();
        }
    }

    @AutoValue
    public abstract static class DeleteRow extends ConnectorBase<PCollection<RawRow>, PCollection<RawRow>> {

        public static DeleteRow.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_DeleteRow.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract DeleteRow.Builder toBuilder();

        public DeleteRow withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public DeleteRow withWriterConfig(WriterConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setWriterConfig(config).build();
        }

        public DeleteRow withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public DeleteRow withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public DeleteRow withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<RawRow> expand(PCollection<RawRow> input) {
            LOG.info("Starting Cognite reader.");

            LOG.debug("Building write raw row composite transform.");

            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<RawRow> eventCoder = ProtoCoder.of(RawRow.class);
            KvCoder<String, RawRow> keyValueCoder = KvCoder.of(utf8Coder, eventCoder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<RawRow> outputCollection = input
                    .apply("Shard rows", WithKeys.of((RawRow inputItem) ->
                            inputItem.getDbName()
                                    + "-" + inputItem.getTableName()
                                    + "-" + (inputItem.hashCode() % getHints().getWriteShards())
                    )).setCoder(keyValueCoder)
                    .apply("Batch rows", GroupIntoBatches.<String, RawRow>of(keyValueCoder)
                            .withMaxBatchSize(10000)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<RawRow>>create())
                    .apply("Delete rows", ParDo.of(
                            new DeleteRawRowFn(getHints(), getWriterConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract DeleteRow.Builder setWriterConfig(WriterConfig value);
            public abstract DeleteRow build();
        }
    }

    @AutoValue
    public abstract static class ReadDatabase
            extends ConnectorBase<PBegin, PCollection<String>> {

        public static ReadDatabase.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_ReadDatabase.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract ReadDatabase.Builder toBuilder();

        public ReadDatabase withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadDatabase withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadDatabase withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public ReadDatabase withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public ReadDatabase withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<String> expand(PBegin input) {
            LOG.debug("Building read raw database names composite transform.");

            // main input
            PCollection<String> outputCollection = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("Read database names", ParDo.of(
                            new ReadRawDatabase(getHints(), getReaderConfig()))
                            );

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract ReadDatabase.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadDatabase build();
        }
    }

    @AutoValue
    public abstract static class ReadTable
            extends ConnectorBase<PBegin, PCollection<RawTable>> {

        public static ReadTable.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_ReadTable.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract String getDbName();

        public abstract ReadTable.Builder toBuilder();

        public ReadTable withDbName(String dbName) {
            Preconditions.checkNotNull(dbName, "Database name cannot be null.");
            return toBuilder().setDbName(dbName).build();
        }

        public ReadTable withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadTable withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadTable withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public ReadTable withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public ReadTable withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<RawTable> expand(PBegin input) {
            LOG.info("Starting Cognite reader.");

            LOG.debug("Building read raw table names composite transform.");

            PCollection<RawTable> outputCollection = input
                    .apply("Generate Query", Create.of(getDbName()))
                    .apply("Read table names", CogniteIO.readAllRawTable()
                            .withProjectConfig(getProjectConfig())
                            .withHints(getHints())
                            .withReaderConfig(getReaderConfig()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract ReadTable.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadTable.Builder setDbName(String value);

            public abstract ReadTable build();
        }
    }

    @AutoValue
    public abstract static class ReadAllTable
            extends ConnectorBase<PCollection<String>, PCollection<RawTable>> {

        public static ReadAllTable.Builder builder() {
            return new com.cognite.beam.io.AutoValue_Raw_ReadAllTable.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract ReadAllTable.Builder toBuilder();

        public ReadAllTable withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAllTable withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public ReadAllTable withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public ReadAllTable withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public ReadAllTable withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        @Override
        public PCollection<RawTable> expand(PCollection<String> input) {
            LOG.info("Starting Cognite reader.");
            LOG.debug("Building read raw table names composite transform.");

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<RawTable> outputCollection = input
                    .apply("Read table names", ParDo.of(
                            new ReadRawTable(getHints(), getReaderConfig(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract ReadAllTable.Builder setReaderConfig(ReaderConfig value);
            public abstract ReadAllTable build();
        }
    }
}
