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

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.fn.statestore.RawStateStoreDeleteStateFn;
import com.cognite.beam.io.fn.statestore.RawStateStoreExpandHighFn;
import com.cognite.beam.io.fn.statestore.RawStateStoreGetHighFn;
import com.cognite.beam.io.fn.statestore.RawStateStoreSetHighFn;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.internal.*;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.cognite.beam.io.CogniteIO.invalidProjectConfigFile;

public abstract class RawStateStore {
    private static final int MAX_WRITE_BATCH_SIZE = 1000;

    /**
     * Reads the high watermark state of a RAW state store. It takes a {@code String} representing
     * the key as input and outputs the corresponding high watermark value in a {@code KV<String, Long>}.
     */
    @AutoValue
    public abstract static class GetHigh
            extends ConnectorBase<PBegin, PCollection<KV<String, Long>>> {

        public static GetHigh.Builder builder() {
            return new AutoValue_RawStateStore_GetHigh.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract GetHigh.Builder toBuilder();
        @Nullable
        public abstract String getKey();
        @Nullable
        public abstract String getDbName();
        @Nullable
        public abstract String getTableName();

        public GetHigh withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public GetHigh withReaderConfig(ReaderConfig config) {
            return toBuilder().setReaderConfig(config).build();
        }

        public GetHigh withDbName(String dbName) {
            return toBuilder().setDbName(dbName).build();
        }

        public GetHigh withTableName(String tableName) {
            return toBuilder().setTableName(tableName).build();
        }

        public GetHigh withKey(String key) {
            return toBuilder().setKey(key).build();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PBegin input) {
            Preconditions.checkState(null != getKey() && !getKey().isBlank(),
                    "You must specify the key to get the high watermark/state for.");

            PCollection<KV<String, Long>> outputCollection = input
                    .apply("Generate query", Create.of(getKey()))
                    .apply("Read high watermark", CogniteIO.getHighAllRawStateStorage()
                            .withProjectConfig(getProjectConfig())
                            .withReaderConfig(getReaderConfig())
                            .withDbName(getDbName())
                            .withTableName(getTableName()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<GetHigh.Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);
            public abstract Builder setKey(String value);
            public abstract GetHigh build();
        }
    }

    /**
     * Reads the high watermark state of a RAW state store. It takes a {@code String} representing
     * the key as input and outputs the corresponding high watermark value in a {@code KV<String, Long>}.
     */
    @AutoValue
    public abstract static class GetHighAll
            extends ConnectorBase<PCollection<String>, PCollection<KV<String, Long>>> {

        public static GetHighAll.Builder builder() {
            return new AutoValue_RawStateStore_GetHighAll.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setReaderConfig(ReaderConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract ReaderConfig getReaderConfig();
        public abstract GetHighAll.Builder toBuilder();
        @Nullable
        public abstract String getDbName();
        @Nullable
        public abstract String getTableName();

        public GetHighAll withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public GetHighAll withDbName(String dbName) {
            return toBuilder().setDbName(dbName).build();
        }

        public GetHighAll withTableName(String tableName) {
            return toBuilder().setTableName(tableName).build();
        }

        public GetHighAll withReaderConfig(ReaderConfig readerConfig) {
            return toBuilder().setReaderConfig(readerConfig).build();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<KV<String, Long>> outputCollection = input
                    .apply("Read high watermark", ParDo.of(
                                            new RawStateStoreGetHighFn(getHints(),
                                                    getReaderConfig(),
                                                    getDbName(),
                                                    getTableName(),
                                                    projectConfigView) {
                                            })
                                    .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<GetHighAll.Builder> {
            public abstract Builder setReaderConfig(ReaderConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);
            public abstract GetHighAll build();
        }
    }

    /**
     * Sets the high watermark state to a RAW state store. It takes a {@code KV<String, Long>} representing
     * the key and high watermark value as input and outputs the same element after it has been committed to the
     * state store.
     */
    @AutoValue
    public abstract static class SetHigh
            extends ConnectorBase<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {
        private static final int MAX_WRITE_BATCH_SIZE = 1000;

        public static Builder builder() {
            return new AutoValue_RawStateStore_SetHigh.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();
        @Nullable
        public abstract String getDbName();
        @Nullable
        public abstract String getTableName();

        public SetHigh withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public SetHigh withWriterConfig(WriterConfig config) {
            return toBuilder().setWriterConfig(config).build();
        }

        public SetHigh withDbName(String dbName) {
            return toBuilder().setDbName(dbName).build();
        }

        public SetHigh withTableName(String tableName) {
            return toBuilder().setTableName(tableName).build();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<KV<String, Long>> input) {
            Schema keyValueSchema = Schema.builder()
                    .addStringField("key")
                    .addInt64Field("value")
                    .build();

            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<Row> rowCoder = RowCoder.of(keyValueSchema);
            KvCoder<String, Row> keyRowCoder = KvCoder.of(utf8Coder, rowCoder);

            // main input
            PCollection<KV<String, Long>> outputCollection = input
                    .apply("Wrap KV", MapElements.into(TypeDescriptors.rows())
                            .via(kv -> Row.withSchema(keyValueSchema)
                                    .withFieldValue("key", kv.getKey())
                                    .withFieldValue("value", kv.getValue())
                                    .build()))
                    .setRowSchema(keyValueSchema)
                    .apply("Shard items", WithKeys.of(inputItem -> "single-key")).setCoder(keyRowCoder)
                    .apply("Batch items", GroupIntoBatches.<String, Row>of(keyRowCoder)
                            .withMaxBatchSize(MAX_WRITE_BATCH_SIZE)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<Row>>create())
                    .apply("Unwrap KV", MapElements.into(TypeDescriptors.iterables(
                                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
                            .via(rows -> {
                                List<KV<String, Long>> kvList = new ArrayList<>();
                                for (Row element : rows) {
                                    kvList.add(KV.of(element.getString("key"), element.getInt64("value")));
                                }
                                return kvList;
                            }))
                    .apply("Set high", CogniteIO.setHighDirectRawStateStore()
                            .withProjectConfig(getProjectConfig())
                            .withWriterConfig(getWriterConfig())
                            .withDbName(getDbName())
                            .withTableName(getTableName()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);

            public abstract SetHigh build();
        }
    }

    /**
     * Sets the high watermark state to a RAW state store. It takes a {@code KV<String, Long>} representing
     * the key and high watermark value as input and outputs the same element after it has been committed to the
     * state store.
     */
    @AutoValue
    public abstract static class SetHighDirect
            extends ConnectorBase<PCollection<Iterable<KV<String, Long>>>, PCollection<KV<String, Long>>> {

        public static Builder builder() {
            return new AutoValue_RawStateStore_SetHighDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();
        @Nullable
        public abstract String getDbName();
        @Nullable
        public abstract String getTableName();

        public SetHighDirect withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public SetHighDirect withWriterConfig(WriterConfig config) {
            return toBuilder().setWriterConfig(config).build();
        }

        public SetHighDirect withDbName(String dbName) {
            return toBuilder().setDbName(dbName).build();
        }

        public SetHighDirect withTableName(String tableName) {
            return toBuilder().setTableName(tableName).build();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<Iterable<KV<String, Long>>> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<KV<String, Long>> outputCollection = input
                    .apply("Set high direct", ParDo.of(
                                    new RawStateStoreSetHighFn(getHints(),
                                            getWriterConfig(),
                                            getDbName(),
                                            getTableName(),
                                            projectConfigView) {
                                    })
                            .withSideInputs(projectConfigView))
                    .apply("Unwrap", ParDo.of(new DoFn<List<KV<String, Long>>, KV<String, Long>>() {
                        @ProcessElement
                        public void processElement(@Element List<KV<String, Long>> element,
                                                   OutputReceiver<KV<String, Long>> out) {
                            // output without timestamp
                            element.forEach(row -> out.output(row));
                        }
                    }));


            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);

            public abstract SetHighDirect build();
        }
    }

    /**
     * Expands the high watermark state to a RAW state store. It takes a {@code KV<String, Long>} representing
     * the key and high watermark value as input and outputs the same element after it has been committed to the
     * state store.
     *
     * Expand high will only set a new value it the supplied value is higher than the current state for the key.
     */
    @AutoValue
    public abstract static class ExpandHigh
            extends ConnectorBase<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {
        private static final int MAX_WRITE_BATCH_SIZE = 1000;

        public static Builder builder() {
            return new AutoValue_RawStateStore_ExpandHigh.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();
        @Nullable
        public abstract String getDbName();
        @Nullable
        public abstract String getTableName();

        public ExpandHigh withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public ExpandHigh withWriterConfig(WriterConfig writerConfig) {
            return toBuilder().setWriterConfig(writerConfig).build();
        }

        public ExpandHigh withDbName(String dbName) {
            return toBuilder().setDbName(dbName).build();
        }

        public ExpandHigh withTableName(String tableName) {
            return toBuilder().setTableName(tableName).build();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<KV<String, Long>> input) {
            Schema keyValueSchema = Schema.builder()
                    .addStringField("key")
                    .addInt64Field("value")
                    .build();

            Coder<String> utf8Coder = StringUtf8Coder.of();
            Coder<Row> rowCoder = RowCoder.of(keyValueSchema);
            KvCoder<String, Row> keyRowCoder = KvCoder.of(utf8Coder, rowCoder);

            // main input
            PCollection<KV<String, Long>> outputCollection = input
                    .apply("Wrap KV", MapElements.into(TypeDescriptors.rows())
                            .via(kv -> Row.withSchema(keyValueSchema)
                                    .withFieldValue("key", kv.getKey())
                                    .withFieldValue("value", kv.getValue())
                                    .build()))
                    .setRowSchema(keyValueSchema)
                    .apply("Shard items", WithKeys.of(inputItem -> "single-key")).setCoder(keyRowCoder)
                    .apply("Batch items", GroupIntoBatches.<String, Row>of(keyRowCoder)
                            .withMaxBatchSize(MAX_WRITE_BATCH_SIZE)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<Row>>create())
                    .apply("Unwrap KV", MapElements.into(TypeDescriptors.iterables(
                                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
                            .via(rows -> {
                                List<KV<String, Long>> kvList = new ArrayList<>();
                                for (Row element : rows) {
                                    kvList.add(KV.of(element.getString("key"), element.getInt64("value")));
                                }
                                return kvList;
                            }))
                    .apply("Expand high", CogniteIO.expandHighDirectRawStateStore()
                            .withProjectConfig(getProjectConfig())
                            .withWriterConfig(getWriterConfig())
                            .withDbName(getDbName())
                            .withTableName(getTableName()));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);

            public abstract ExpandHigh build();
        }
    }

    /**
     * Expands the high watermark state to a RAW state store. It takes a {@code KV<String, Long>} representing
     * the key and high watermark value as input and outputs the same element after it has been committed to the
     * state store.
     *
     * Expand high will only set a new value it the supplied value is higher than the current state for the key.
     */
    @AutoValue
    public abstract static class ExpandHighDirect
            extends ConnectorBase<PCollection<Iterable<KV<String, Long>>>, PCollection<KV<String, Long>>> {

        public static Builder builder() {
            return new AutoValue_RawStateStore_ExpandHighDirect.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();
        @Nullable
        public abstract String getDbName();
        @Nullable
        public abstract String getTableName();

        public ExpandHighDirect withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public ExpandHighDirect withWriterConfig(WriterConfig config) {
            return toBuilder().setWriterConfig(config).build();
        }

        public ExpandHighDirect withDbName(String dbName) {
            return toBuilder().setDbName(dbName).build();
        }

        public ExpandHighDirect withTableName(String tableName) {
            return toBuilder().setTableName(tableName).build();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<Iterable<KV<String, Long>>> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            // main input
            PCollection<KV<String, Long>> outputCollection = input
                    .apply("Expand high direct", ParDo.of(
                                    new RawStateStoreExpandHighFn(getHints(),
                                            getWriterConfig(),
                                            getDbName(),
                                            getTableName(),
                                            projectConfigView) {
                                    })
                            .withSideInputs(projectConfigView))
                    .apply("Unwrap", ParDo.of(new DoFn<List<KV<String, Long>>, KV<String, Long>>() {
                        @ProcessElement
                        public void processElement(@Element List<KV<String, Long>> element,
                                                   OutputReceiver<KV<String, Long>> out) {
                            // output without timestamp
                            element.forEach(row -> out.output(row));
                        }
                    }));


            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);

            public abstract ExpandHighDirect build();
        }
    }

    /**
     * Transform that will delete a state from the Raw state store. It takes the state key to delete as input and
     * outputs the same key after the delete has been committed.
     */
    @AutoValue
    public abstract static class DeleteState
            extends ConnectorBase<PCollection<String>, PCollection<String>> {
        public static Builder builder() {
            return new AutoValue_RawStateStore_DeleteState.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setWriterConfig(WriterConfig.create())
                    .setProjectConfigFile(invalidProjectConfigFile);
        }
        public abstract WriterConfig getWriterConfig();
        public abstract Builder toBuilder();
        @Nullable
        public abstract String getDbName();
        @Nullable
        public abstract String getTableName();

        public DeleteState withProjectConfig(ProjectConfig config) {
            return toBuilder().setProjectConfig(config).build();
        }

        public DeleteState withWriterConfig(WriterConfig config) {
            return toBuilder().setWriterConfig(config).build();
        }

        public DeleteState withDbName(String dbName) {
            return toBuilder().setDbName(dbName).build();
        }

        public DeleteState withTableName(String tableName) {
            return toBuilder().setTableName(tableName).build();
        }

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            Coder<String> utf8Coder = StringUtf8Coder.of();
            KvCoder<String, String> keyValueCoder = KvCoder.of(utf8Coder, utf8Coder);

            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<String> outputCollection = input
                    .apply("Shard items", WithKeys.of((String inputItem) -> "single-key"))
                            .setCoder(keyValueCoder)
                    .apply("Batch items", GroupIntoBatches.<String, String>of(keyValueCoder)
                            .withMaxBatchSize(MAX_WRITE_BATCH_SIZE)
                            .withMaxLatency(getHints().getWriteMaxBatchLatency()))
                    .apply("Remove key", Values.<Iterable<String>>create())
                    .apply("Delete items", ParDo.of(
                                    new RawStateStoreDeleteStateFn(getHints(),
                                                                    getWriterConfig(),
                                                                    getDbName(),
                                                                    getTableName(),
                                                                    projectConfigView) {
                                    })
                            .withSideInputs(projectConfigView))
                    .apply("Unwrap", ParDo.of(new DoFn<List<String>, String>() {
                        @ProcessElement
                        public void processElement(@Element List<String> element,
                                                   OutputReceiver<String> out) {
                                // output without timestamp
                                element.forEach(row -> out.output(row));
                        }
                    }));


            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<Builder> {
            public abstract Builder setWriterConfig(WriterConfig value);
            public abstract Builder setDbName(String value);
            public abstract Builder setTableName(String value);
            public abstract DeleteState build();
        }
    }
}
