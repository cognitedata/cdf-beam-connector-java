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

package com.cognite.beam.io.transform.internal;

import com.cognite.beam.io.ConnectorBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.RawRow;
import com.cognite.beam.io.transform.WriteTimestamp;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility transform for recording delta timestamps from a reader.
 */
@AutoValue
public abstract class RecordDeltaTimestamp
        extends ConnectorBase<PCollection<Long>, PCollection<RawRow>> {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());


    private static RecordDeltaTimestamp.Builder builder() {
        return new AutoValue_RecordDeltaTimestamp.Builder()
                .setProjectConfig(ProjectConfig.create())
                .setHints(Hints.create())
                .setReaderConfig(ReaderConfig.create())
                .setProjectConfigFile(ValueProvider.StaticValueProvider.of("."));
    }

    public static RecordDeltaTimestamp create() {
        return RecordDeltaTimestamp.builder().build();
    }

    public RecordDeltaTimestamp withProjectConfig(ProjectConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setProjectConfig(config).build();
    }

    public RecordDeltaTimestamp withReaderConfig(ReaderConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setReaderConfig(config).build();
    }

    public RecordDeltaTimestamp withProjectConfigFile(String file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
        return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
    }

    public RecordDeltaTimestamp withProjectConfigFile(ValueProvider<String> file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        return toBuilder().setProjectConfigFile(file).build();
    }

    abstract RecordDeltaTimestamp.Builder toBuilder();
    abstract ReaderConfig getReaderConfig();

    @Override
    public PCollection<RawRow> expand(PCollection<Long> input) {
        LOG.info("Starting RecordDeltaTimestamp transform.");

        PCollection<RawRow> result;

        if (getReaderConfig().isDeltaEnabled()) {
            LOG.info("Delta read enabled. Will record the most recent delta timestamp.");

            // Main flow. Add the delta timestamp to the request
            result = input
                    .apply("Get max timestamp", Max.longsGlobally().withoutDefaults())
                    .apply("Log settings", MapElements.into(TypeDescriptors.longs())
                            .via(timestamp -> {
                                getReaderConfig().validate();
                                LOG.debug("Delta read config. FullReadOverride = {}",
                                        getReaderConfig().getFullReadOverride().get());
                                LOG.debug("Delta read config. Delta read table name = {}",
                                        getReaderConfig().getDeltaReadTable().get());
                                LOG.debug("Delta read config. Delta read table name isEmpty = {}",
                                        getReaderConfig().getDeltaReadTable().get().isEmpty());
                                LOG.debug("Delta read config. Is delta enabled = {}",
                                        getReaderConfig().isDeltaEnabled());

                                if (getReaderConfig().getFullReadOverride().get()) {
                                    LOG.info("Delta full read override enabled. Will not record delta timestamp.");
                                }
                                return timestamp;
                            }))
                    .apply("Full read override filter", Filter.by(timestamp ->
                            !getReaderConfig().getFullReadOverride().get()))
                    .apply("Record timestamp", WriteTimestamp.to(getReaderConfig().getDeltaReadTable())
                            .withIdentifier(getReaderConfig().getDeltaReadIdentifier())
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withWriterConfig(WriterConfig.create()
                                    .withAppIdentifier(getReaderConfig().getAppIdentifier())
                                    .withSessionIdentifier(getReaderConfig().getSessionIdentifier())
                                    .enableMetrics(false)));

        } else {
            LOG.info("Delta read disabled. Delta timestamp will not be recorded.");
            //result = input.getPipeline().apply("Empty collection", Create.empty(TypeDescriptor.of(RawRow.class)));
            result = input
                    .apply("Empty collection", ParDo.of(new DoFn<Long, RawRow>() {
                        @ProcessElement
                        public void processElement(@Element Long input, OutputReceiver<RawRow> out) {
                            return;
                        }
                    }));
        }

        return result;
    }

    @AutoValue.Builder
    static abstract class Builder extends ConnectorBase.Builder<Builder> {
        abstract Builder setReaderConfig(ReaderConfig value);

        public abstract RecordDeltaTimestamp build();
    }
}
