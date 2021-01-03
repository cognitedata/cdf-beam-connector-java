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
import com.cognite.beam.io.fn.ResourceType;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.transform.ReadTimestamp;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility transform for applying delta timestamp to a request.
 *
 * The delta timestamp will be added as a lower boundary filter to the request:
 * - Assets, Events and file headers. <code>lastUpdatedTime</code> will be set with a min filter.
 * - Raw rows. <code>minLastUpdatedTime</code> will be set.
 *
 * If the request already contains a lower boundary filter, the most recent alternative will be used. If the delta
 * timestamp is a more recent instant compared to the existing request filter, the request filter will be replaced
 * by the delta timestamp. On the other hand, if the request filter is more recent than the delta timestamp, then the
 * request filter will be used.
 */
@AutoValue
public abstract class ApplyDeltaTimestamp
        extends ConnectorBase<PCollection<RequestParameters>, PCollection<RequestParameters>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static ApplyDeltaTimestamp.Builder builder() {
        return new AutoValue_ApplyDeltaTimestamp.Builder()
                .setProjectConfig(ProjectConfig.create())
                .setHints(Hints.create())
                .setReaderConfig(ReaderConfig.create().enableMetrics(false))
                .setProjectConfigFile(ValueProvider.StaticValueProvider.of("."));
    }

    public static ApplyDeltaTimestamp to(ResourceType resource) {
        return ApplyDeltaTimestamp.builder()
                .setResourceType(resource)
                .build();
    }

    public ApplyDeltaTimestamp withProjectConfig(ProjectConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setProjectConfig(config).build();
    }

    public ApplyDeltaTimestamp withReaderConfig(ReaderConfig config) {
        Preconditions.checkNotNull(config, "Config cannot be null");
        return toBuilder().setReaderConfig(config).build();
    }

    public ApplyDeltaTimestamp withProjectConfigFile(String file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
        return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
    }

    public ApplyDeltaTimestamp withProjectConfigFile(ValueProvider<String> file) {
        Preconditions.checkNotNull(file, "File cannot be null");
        return toBuilder().setProjectConfigFile(file).build();
    }

    abstract ApplyDeltaTimestamp.Builder toBuilder();
    abstract ReaderConfig getReaderConfig();
    abstract ResourceType getResourceType();

    @Override
    public PCollection<RequestParameters> expand(PCollection<RequestParameters> input) {
        LOG.info("Starting AddDeltaTimestamp transform.");

        PCollection<RequestParameters> result;

        if (getReaderConfig().isDeltaEnabled()) {
            LOG.info("Delta read enabled. Identifying most recent delta timestamp.");

            // Set up the delta timestamp as a side input
            PCollection<Long> deltaTimestampCollection = input.getPipeline()
                    .apply("Reading delta timestamp", ReadTimestamp.from(getReaderConfig().getDeltaReadTable())
                            .withReaderConfig(ReaderConfig.create()
                                    .withAppIdentifier(getReaderConfig().getAppIdentifier())
                                    .withSessionIdentifier(getReaderConfig().getSessionIdentifier())
                                    .enableMetrics(false))
                            .withProjectConfig(getProjectConfig())
                            .withProjectConfigFile(getProjectConfigFile())
                            .withIdentifier(getReaderConfig().getDeltaReadIdentifier()));

            PCollectionView<Long> deltaTimestampView = deltaTimestampCollection
                    .apply("To singleton view", View.asSingleton());

            // Main flow. Add the delta timestamp to the request
            result = input
                    .apply("Add delta timestamp to request", ParDo.of(new DoFn<RequestParameters, RequestParameters>() {
                        final String ITEM_TS_KEY = "lastUpdatedTime";
                        final String RAW_TS_KEY = "minLastUpdatedTime";

                        @Setup
                        public void setup() {
                            LOG.info("Setting up add delta timestamp function.");
                            getReaderConfig().validate();
                        }

                        @ProcessElement
                        public void processElement(@Element RequestParameters inputRequest,
                                                   OutputReceiver<RequestParameters> out,
                                                   ProcessContext context) throws Exception {

                            LOG.debug("Delta read config. FullReadOverride = {}",
                                    getReaderConfig().getFullReadOverride().get());
                            LOG.debug("Delta read config. Delta read table name = {}",
                                    getReaderConfig().getDeltaReadTable().get());
                            LOG.debug("Delta read config. Delta read table name isEmpty = {}",
                                    getReaderConfig().getDeltaReadTable().get().isEmpty());
                            LOG.debug("Delta read config. Is delta enabled = {}",
                                    getReaderConfig().isDeltaEnabled());

                            if (getReaderConfig().getFullReadOverride().get()) {
                                LOG.info("Delta full read override enabled. Will not set delta timestamp.");
                                out.output(inputRequest);
                                return;
                            }

                            Long requestTimestamp = 0L;
                            long deltaTimestamp = Math.max(1L, context.sideInput(deltaTimestampView)
                                    - getReaderConfig().getDeltaOffset().get().toMillis());
                            LOG.info("Delta timestamp identified: " + deltaTimestamp);
                            LOG.debug("Timestamp from delta raw table: " + context.sideInput(deltaTimestampView));
                            LOG.debug("Timestamp offset (ms): " + getReaderConfig().getDeltaOffset().get().toMillis());

                            if (getResourceType().equals(ResourceType.FILE_HEADER)
                                    || getResourceType().equals(ResourceType.ASSET)
                                    || getResourceType().equals(ResourceType.EVENT)
                                    || getResourceType().equals(ResourceType.TIMESERIES_HEADER)
                                    || getResourceType().equals(ResourceType.RELATIONSHIP)
                                    || getResourceType().equals(ResourceType.SEQUENCE_HEADER)) {

                                // Check for existing limit in the request object
                                if (inputRequest.getFilterParameters().containsKey(ITEM_TS_KEY)
                                        && inputRequest.getFilterParameters().get(ITEM_TS_KEY) instanceof Map
                                        && ((Map) inputRequest.getFilterParameters().get(ITEM_TS_KEY))
                                                .getOrDefault("min", Long.valueOf(0L)) instanceof Long) {
                                    LOG.info("The request parameter already contains [lastUpdatedTime] filter: "
                                            + System.lineSeparator() + inputRequest);
                                    requestTimestamp = (Long) ((Map) inputRequest.getFilterParameters().get(ITEM_TS_KEY))
                                            .getOrDefault("min", Long.valueOf(0L));
                                }
                                if (requestTimestamp > deltaTimestamp) {
                                    LOG.info("Request timestamp is more recent than the delta timestamp. "
                                            + "Will use the request timestamp: {}", requestTimestamp);
                                    deltaTimestamp = requestTimestamp;
                                }
                                out.output(inputRequest.withFilterParameter(ITEM_TS_KEY,
                                        ImmutableMap.of("min", deltaTimestamp)));

                            } else if (getResourceType().equals(ResourceType.RAW_ROW)) {
                                // Check for existing limit in the request object
                                if (inputRequest.getRequestParameters().containsKey(RAW_TS_KEY)
                                        && inputRequest.getRequestParameters().get(RAW_TS_KEY) instanceof Long) {
                                    LOG.info("The request parameter already contains [minLastUpdatedTime] filter: "
                                            + System.lineSeparator() + inputRequest);
                                    requestTimestamp = (Long) inputRequest.getRequestParameters().get(RAW_TS_KEY);
                                }
                                if (requestTimestamp > deltaTimestamp) {
                                    LOG.info("Request timestamp is more recent than the delta timestamp. "
                                            + "Will use the request timestamp: {}", requestTimestamp);
                                    deltaTimestamp = requestTimestamp;
                                }
                                out.output(inputRequest.withRootParameter(RAW_TS_KEY, deltaTimestamp));

                            } else {
                                String message = "Not a supported resource type: " + getResourceType();
                                LOG.error(message);
                                throw new Exception(message);
                            }

                        }
                    }).withSideInputs(deltaTimestampView));
        } else {
            // full read override. Do not add any delta timestamp.
            LOG.info("Delta read disabled. Delta timestamp will not be added to the request.");
            result = input;
        }

        return result;
    }

    private void validate() {

    }

    @AutoValue.Builder
    static abstract class Builder extends ConnectorBase.Builder<Builder> {
        abstract Builder setReaderConfig(ReaderConfig value);
        abstract Builder setResourceType(ResourceType value);

        public abstract ApplyDeltaTimestamp build();
    }
}
