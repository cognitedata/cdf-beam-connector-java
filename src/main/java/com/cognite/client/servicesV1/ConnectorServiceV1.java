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

package com.cognite.client.servicesV1;

import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.servicesV1.executor.FileBinaryRequestExecutor;
import com.cognite.client.servicesV1.parser.ItemParser;

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.executor.RequestExecutor;
import com.cognite.client.servicesV1.parser.FileParser;
import com.cognite.client.servicesV1.parser.LoginStatusParser;
import com.cognite.client.servicesV1.request.*;
import com.cognite.client.servicesV1.response.*;
import com.cognite.client.servicesV1.util.JsonUtil;
import com.cognite.v1.timeseries.proto.DataPointListItem;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import okhttp3.HttpUrl;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.annotation.Nullable;

import static com.cognite.client.servicesV1.ConnectorConstants.*;

/**
 * The reader service handles connections to the Cognite REST api.
 */
@AutoValue
public abstract class ConnectorServiceV1 implements Serializable {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    // Logger identifier per instance
    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
    private final String loggingPrefix = "ConnectorService [" + randomIdString + "] -";

    public static Builder builder() {
        return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1.Builder()
                .setMaxRetries(ValueProvider.StaticValueProvider.of(ConnectorConstants.DEFAULT_MAX_RETRIES))
                .setMaxBatchSize(ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)
                .setAppIdentifier(DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER);
    }

    public static ConnectorServiceV1 create() {
        return ConnectorServiceV1.builder().build();
    }

    public static ConnectorServiceV1 create(int newMaxRetries) {
        return ConnectorServiceV1.builder()
                .setMaxRetries(newMaxRetries)
                .build();
    }

    public static ConnectorServiceV1 create(int newMaxRetries,
                                            String appIdentifier,
                                            String sessionIdentifier) {
        return ConnectorServiceV1.builder()
                .setMaxRetries(ValueProvider.StaticValueProvider.of(newMaxRetries))
                .setAppIdentifier(appIdentifier)
                .setSessionIdentifier(sessionIdentifier)
                .build();
    }

    public abstract ValueProvider<Integer> getMaxRetries();
    public abstract int getMaxBatchSize();

    public abstract String getAppIdentifier();
    public abstract String getSessionIdentifier();
    /*
    @Nullable
    abstract OkHttpClient getHttpClient();
    @Nullable
    abstract ExecutorService getExecutorService();

     */

    abstract Builder toBuilder();


    /**
     * Sets the http client to use for api requests. Returns a {@link ConnectorServiceV1} with
     * the setting applied.
     *
     * @param client The {@link OkHttpClient} to use.
     * @return a {@link ConnectorServiceV1} object with the configuration applied.
     */
    /*
    public ConnectorServiceV1 withHttpClient(OkHttpClient client) {
        return toBuilder().setHttpClient(client).build();
    }

     */

    /**
     * Sets the {@link ExecutorService} to use for multi-threaded api requests. Returns a {@link ConnectorServiceV1}
     * with the setting applied.
     *
     * @param executorService The {@link ExecutorService} to use.
     * @return a {@link ConnectorServiceV1} object with the configuration applied.
     */
    /*
    public ConnectorServiceV1 withExecutorService(ExecutorService executorService) {
        return toBuilder().setExecutorService(executorService).build();
    }

     */

    /**
     * Must lazily validate the state of parameters delivered via ValueProviders.
     */
    private void validate() {
        LOG.debug(loggingPrefix + "Validating retries.");
        Preconditions.checkState(getMaxRetries().isAccessible()
                , "Max retries could not be obtained.");
        Preconditions.checkState(getMaxRetries().get() <= ConnectorConstants.MAX_MAX_RETRIES
                        && getMaxRetries().get() >= ConnectorConstants.MIN_MAX_RETRIES
                , "Max retries out of range. Must be between 1 and 20");
    }

    /**
     * Read assets from Cognite.
     *
     * @param queryParameters The parameters for the assets query.
     * @return
     */
    public ResultFutureIterator<String> readAssets(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read assets service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("assets/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read assets aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readAssetsAggregates() {
        LOG.debug(loggingPrefix + "Initiating read assets aggregates service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets/aggregate")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read assets by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readAssetsById() {
        LOG.debug(loggingPrefix + "Initiating read assets by id service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets/byids")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write Assets to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeAssets() {
        LOG.debug(loggingPrefix + "Initiating write assets service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Update Assets in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateAssets() {
        LOG.debug(loggingPrefix + "Initiating update assets service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets/update")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete Assets in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteAssets() {
        LOG.debug(loggingPrefix + "Initiating delete assets service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Read events from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<String> readEvents(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read events service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("events/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read events aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readEventsAggregates() {
        LOG.debug(loggingPrefix + "Initiating read events aggregates service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events/aggregate")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read events by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readEventsById() {
        LOG.debug(loggingPrefix + "Initiating read events service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events/byids")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write Events to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeEvents() {
        LOG.debug(loggingPrefix + "Initiating write events service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Update Events in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateEvents() {
        LOG.debug(loggingPrefix + "Initiating update events service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events/update")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete Events in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteEvents() {
        LOG.debug(loggingPrefix + "Initiating delete events service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Fetch sequences headers from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<String> readSequencesHeaders(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read sequences headers service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("sequences/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read sequences aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readSequencesAggregates() {
        LOG.debug(loggingPrefix + "Initiating read sequences aggregates service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/aggregate")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read sequences by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readSequencesById() {
        LOG.debug(loggingPrefix + "Initiating read sequences by id service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/byids")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write sequences headers to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeSequencesHeaders() {
        LOG.debug(loggingPrefix + "Initiating write sequences headers service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.of(requestProvider)
                .withMaxRetries(getMaxRetries().get())
                .withDuplicatesResponseParser(JsonErrorMessageDuplicateResponseParser.builder().build());
    }

    /**
     * Update sequences headers in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateSequencesHeaders() {
        LOG.debug(loggingPrefix + "Initiating update sequences headers service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/update")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete sequences headers in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteSequencesHeaders() {
        LOG.debug(loggingPrefix + "Initiating delete sequences service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Fetch sequences rows / body from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<String> readSequencesRows(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read sequences rows service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("sequences/data/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write sequences rows to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeSequencesRows() {
        LOG.debug(loggingPrefix + "Initiating write sequences rows service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/data")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete sequences rows in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteSequencesRows() {
        LOG.debug(loggingPrefix + "Initiating delete sequences service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/data/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * List timeseries headers from Cognite.
     *
     * @param queryParameters The parameters for the TS query.
     * @return
     */
    public ResultFutureIterator<String> readTsHeaders(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read TS headers service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("timeseries/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read timeseries aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readTsAggregates() {
        LOG.debug(loggingPrefix + "Initiating read timeseries aggregates service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/aggregate")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read time series headers by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readTsById() {
        LOG.debug(loggingPrefix + "Initiating read time series by id service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/byids")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write time series headers to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeTsHeaders() {
        LOG.debug(loggingPrefix + "Initiating write ts headers service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Update time series headers in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateTsHeaders() {
        LOG.debug(loggingPrefix + "Initiating update ts headers service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/update")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete TS headers in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteTsHeaders() {
        LOG.debug(loggingPrefix + "Initiating delete ts service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Fetch timeseries datapoints from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<String> readTsDatapoints(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read TS datapoints service.");
        this.validate();

        TSPointsRequestProvider requestProvider = TSPointsRequestProvider.builder()
                .setEndpoint("timeseries/data/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        TSPointsResponseParser responseParser = TSPointsResponseParser.builder().build()
                .withRequestParameters(queryParameters);

        return ResultFutureIterator.<String>of(requestProvider, responseParser)
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Fetch timeseries datapoints from Cognite using protobuf encoding.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<DataPointListItem>
            readTsDatapointsProto(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read TS datapoints service.");
        this.validate();

        TSPointsReadProtoRequestProvider requestProvider = TSPointsReadProtoRequestProvider.builder()
                .setEndpoint("timeseries/data/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        TSPointsProtoResponseParser responseParser = TSPointsProtoResponseParser.builder().build()
                .withRequestParameters(queryParameters);

        return ResultFutureIterator.<DataPointListItem>of(requestProvider, responseParser)
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read latest data point from Cognite.
     *
     * @return
     */
    public ItemReader<String> readTsDatapointsLatest() {
        LOG.debug(loggingPrefix + "Initiating read latest data point service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/data/latest")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write time series headers to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeTsDatapoints() {
        LOG.debug(loggingPrefix + "Building writer for ts datapoints service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/data")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Write time series data points to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeTsDatapointsProto() {
        LOG.debug(loggingPrefix + "Building writer for ts datapoints service.");
        this.validate();

        TSPointsWriteProtoRequestProvider requestProvider = TSPointsWriteProtoRequestProvider.builder()
                .setEndpoint("timeseries/data")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete data points in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteDatapoints() {
        LOG.debug(loggingPrefix + "Initiating delete data points service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/data/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Fetch 3d models from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public Iterator<CompletableFuture<ResponseItems<String>>> read3dModels(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read 3d models service.");
        this.validate();

        GetSimpleListRequestProvider requestProvider = GetSimpleListRequestProvider.builder()
                .setEndpoint("3d/models")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Fetch Raw rows from Cognite.
     *
     * @param queryParameters The parameters for the raw query.
     * @return
     */
    public Iterator<CompletableFuture<ResponseItems<String>>> readRawRows(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read raw rows service.");
        this.validate();

        RawReadRowsRequestProvider requestProvider = RawReadRowsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonRawRowResponseParser.builder().build())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read cursors for retrieving events in parallel. The results set is split into n partitions.
     *
     * @return
     */
    public ItemReader<String> readCursorsRawRows() {
        LOG.debug(loggingPrefix + "Initiating read raw cursors service.");
        this.validate();

        RawReadRowsCursorsRequestProvider requestProvider = RawReadRowsCursorsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write rows to Raw in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     */
    public ItemWriter writeRawRows() {
        LOG.debug(loggingPrefix + "Initiating write raw rows service.");
        this.validate();

        RawWriteRowsRequestProvider requestProvider = RawWriteRowsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete Assets in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteRawRows() {
        LOG.debug(loggingPrefix + "Initiating delete raw rows service.");
        this.validate();

        RawDeleteRowsRequestProvider requestProvider = RawDeleteRowsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * List the Raw database names from Cognite.
     *
     * @return
     */
    public Iterator<CompletableFuture<ResponseItems<String>>> readRawDbNames(ProjectConfig config) {
        LOG.debug(loggingPrefix + "Initiating read raw database names service.");
        this.validate();

        GetSimpleListRequestProvider requestProvider = GetSimpleListRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequestParameters(RequestParameters.create()
                        .withRootParameter("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)
                        .withProjectConfig(config))
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * List the Raw tables for a given database.
     *
     * @param dbName The name of the database to list tables from.
     * @return
     */
    public ResultFutureIterator<String> readRawTableNames(String dbName, ProjectConfig config) {
        Preconditions.checkNotNull(dbName);
        Preconditions.checkArgument(!dbName.isEmpty(), "Database name cannot be empty.");
        LOG.debug(loggingPrefix + "Listing tables for database {}", dbName);
        this.validate();

        GetSimpleListRequestProvider requestProvider = GetSimpleListRequestProvider.builder()
                .setEndpoint("raw/dbs/" + dbName + "/tables")
                .setRequestParameters(RequestParameters.create()
                        .withRootParameter("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)
                        .withProjectConfig(config))
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * List the Raw tables for a given database.
     *
     * @param dbName The name of the database to list tables from.
     * @return
     */
    public ItemWriter writeRawTableNames(String dbName) {
        Preconditions.checkNotNull(dbName);
        Preconditions.checkArgument(!dbName.isEmpty(), "Database name cannot be empty.");
        LOG.debug(loggingPrefix + "Listing tables for database {}", dbName);
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("raw/dbs/" + dbName + "/tables")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * List file headers from Cognite.
     *
     * @param queryParameters The parameters for the file query.
     * @return
     */
    public ResultFutureIterator<String> readFileHeaders(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read File headers service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("files/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read files aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readFilesAggregates() {
        LOG.debug(loggingPrefix + "Initiating read files aggregates service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("files/aggregate")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read files by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readFilesById() {
        LOG.debug(loggingPrefix + "Initiating read files by id service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("files/byids")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read file binaries from Cognite.
     *
     * Returns an <code>FileBinaryReader</code> which can be used to read file binaries by id.
     *
     * @return
     */
    public FileBinaryReader readFileBinariesByIds() {
        LOG.debug(loggingPrefix + "Initiating read File binaries by ids service.");
        this.validate();

        return FileBinaryReader.builder()
                .setMaxRetries(getMaxRetries().get())
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();
    }

    /**
     * Write files to Cognite. This is a two-step request with 1) post the file metadata / header
     * and 2) post the file binary.
     *
     * This method returns an <code>ItemWriter</code> to which you can post the file metadata / header.
     * I.e. it performs step 1), but not step 2). The response from the <code>ItemWriter</code>
     * contains a URL reference for the file binary upload.
     *
     * @return
     */
    public ItemWriter writeFileHeaders() {
        LOG.debug(loggingPrefix + "Initiating write file header / metadata service.");
        this.validate();

        FilesUploadRequestProvider requestProvider = FilesUploadRequestProvider.builder()
                .setEndpoint("files")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Write files to Cognite.
     *
     * This method returns an <code>FileWriter</code> to which you can post the <code>FileContainer</code>
     * with file metadata / header. I.e. this writer allows you to post both the file header and the
     * file binary in a single method call. The response contains the the metadata response item for the file.
     *
     * @return
     */
    public FileWriter writeFileProto() {
        LOG.debug(loggingPrefix + "Initiating write file proto service.");
        this.validate();

        return FileWriter.builder()
                .setMaxRetries(getMaxRetries().get())
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();
    }

    /**
     * Update file headers to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateFileHeaders() {
        LOG.debug(loggingPrefix + "Initiating update file headers service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("files/update")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete Files (including headers) in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteFiles() {
        LOG.debug(loggingPrefix + "Initiating delete files service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("files/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Get the login status from Cognite.
     *
     * @param host
     * @param apiKey
     * @return
     */
    public LoginStatus readLoginStatusByApiKey(String host, String apiKey) throws Exception {
        LOG.debug(loggingPrefix + "Getting login status for host [{}].", host);
        this.validate();

        GetLoginRequestProvider requestProvider = GetLoginRequestProvider.builder()
                .setEndpoint("status")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        JsonResponseParser responseParser = JsonResponseParser.create();

        SingleRequestItemReader<String> itemReader = SingleRequestItemReader.of(requestProvider, responseParser)
                .withMaxRetries(getMaxRetries().get());

        // Send the request to the Cognite api
        ResponseItems<String> responseItems = itemReader.getItems(RequestParameters.create()
                .withProjectConfig(ProjectConfig.create()
                        .withHost(host)
                        .withApiKey(apiKey)));

        // Check the response
        if (!responseItems.getResponseBinary().getResponse().isSuccessful()
                || responseItems.getResultsItems().size() != 1) {
            String message = loggingPrefix + "Cannot get login status from Cognite. Could not get a valid response.";
            LOG.error(message);
            throw new Exception(message);
        }

        // parser the response
        return LoginStatusParser.parseLoginStatus(responseItems.getResultsItems().get(0));
    }

    /**
     * Fetch relationships from Cognite.
     *
     * @param queryParameters The parameters for the data sets query.
     * @return
     */
    public ResultFutureIterator<String> readRelationships(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read relationships service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("relationships/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .setBetaEnabled(true)
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read relationships by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readRelationshipsById() {
        LOG.debug(loggingPrefix + "Initiating read relationships by id service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("relationships/byids")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write relationships to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeRelationships() {
        LOG.debug(loggingPrefix + "Initiating write relationships service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("relationships")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .setBetaEnabled(true)
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete relationships in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteRelationships() {
        LOG.debug(loggingPrefix + "Initiating delete relationships service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("relationships/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .setBetaEnabled(true)
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Update data sets in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateDataSets() {
        LOG.debug(loggingPrefix + "Initiating update data sets service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("datasets/update")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Fetch data sets from Cognite.
     *
     * @param queryParameters The parameters for the data sets query.
     * @return
     */
    public ResultFutureIterator<String> readDataSets(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read data sets service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("datasets/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read data sets aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readDataSetsAggregates() {
        LOG.debug(loggingPrefix + "Initiating read data set aggregates service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("datasets/aggregate")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Read data sets by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readDataSetsById() {
        LOG.debug(loggingPrefix + "Initiating read data sets by id service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("datasets/byids")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write data sets to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeDataSets() {
        LOG.debug(loggingPrefix + "Initiating write data sets service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("datasets")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Read labels from Cognite.
     *
     * @param queryParameters The parameters for the data sets query.
     * @return
     */
    public ResultFutureIterator<String> readLabels(RequestParameters queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read labels service.");
        this.validate();

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("labels/list")
                .setRequestParameters(queryParameters)
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Write labels to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeLabels() {
        LOG.debug(loggingPrefix + "Initiating write labels service.");
        this.validate();

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("labels")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Delete labels in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteLabels() {
        LOG.debug(loggingPrefix + "Initiating delete labels service.");
        this.validate();

        PostPlaygroundJsonRequestProvider requestProvider = PostPlaygroundJsonRequestProvider.builder()
                .setEndpoint("labels/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Detect references to assets and files in a P&ID and annotate the references with bounding boxes.
     * Finds entities in the P&ID that match a list of entity names,
     * for instance asset names. The P&ID must be a single-page PDF file.
     *
     * @return
     */
    public ItemReader<String> detectAnnotationsPnid() {
        LOG.debug(loggingPrefix + "Initiating the annotation detection service.");
        this.validate();

        PostPlaygroundJsonRequestProvider jobStartRequestProvider =
                PostPlaygroundJsonRequestProvider.builder()
                        .setEndpoint("context/pnid/detect")
                        .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                        .setAppIdentifier(getAppIdentifier())
                        .setSessionIdentifier(getSessionIdentifier())
                        .build();

        RequestParametersResponseParser jobStartResponseParser = RequestParametersResponseParser.of(
                ImmutableMap.of("jobId", "jobId"));

        GetPlaygroundJobIdRequestProvider jobResultsRequestProvider = GetPlaygroundJobIdRequestProvider.of("context/pnid/detect")
                .toBuilder()
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return AsyncJobReader.of(jobStartRequestProvider, jobResultsRequestProvider, JsonResponseParser.create())
                .withJobStartResponseParser(jobStartResponseParser)
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Convert a single-page P&ID in PDF format to an interactive SVG where
     * the provided annotations are highlighted.
     *
     * @return
     */
    public ItemReader<String> convertPnid() {
        LOG.debug(loggingPrefix + "Initiating the convert PDF service.");
        this.validate();

        PostPlaygroundJsonRequestProvider jobStartRequestProvider =
                PostPlaygroundJsonRequestProvider.builder()
                        .setEndpoint("context/pnid/convert")
                        .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                        .setAppIdentifier(getAppIdentifier())
                        .setSessionIdentifier(getSessionIdentifier())
                        .build();

        RequestParametersResponseParser jobStartResponseParser = RequestParametersResponseParser.of(
                ImmutableMap.of("jobId", "jobId"));

        GetPlaygroundJobIdRequestProvider jobResultsRequestProvider = GetPlaygroundJobIdRequestProvider.of("context/pnid/convert")
                .toBuilder()
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return AsyncJobReader.of(jobStartRequestProvider, jobResultsRequestProvider, JsonResponseParser.create())
                .withJobStartResponseParser(jobStartResponseParser)
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Create a reader for listing entity matcher models.
     *
     * @return An {@link ItemReader<String>} for reading the models.
     */
    public ItemReader<String> readEntityMatcherModels() {
        LOG.debug(loggingPrefix + "Initiating read entity matcher models service.");
        this.validate();

        GetPlaygroundRequestProvider requestProvider = GetPlaygroundRequestProvider.builder()
                .setEndpoint("context/entity_matching")
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(requestProvider, JsonItemResponseParser.create())
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Create a writer for deleting entity matcher models.
     *
     * @return An {@link ItemWriter} for deleting the models
     */
    public ItemWriter deleteEntityMatcherModels() {
        LOG.debug(loggingPrefix + "Initiating delete entity matcher models service.");
        this.validate();

        PostPlaygroundJsonRequestProvider requestProvider = PostPlaygroundJsonRequestProvider.builder()
                .setEndpoint("context/entitymatching/delete")
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return ItemWriter.builder()
                .setRequestProvider(requestProvider)
                .setMaxRetries(getMaxRetries().get())
                .build();
    }

    /**
     * Create an entity matcher predict reader.
     *
     * @return
     */
    public ItemReader<String> entityMatcherPredict() {
        LOG.debug(loggingPrefix + "Initiating entity matcher predict service.");
        this.validate();

        PostPlaygroundJsonRequestProvider jobStartRequestProvider =
                PostPlaygroundJsonRequestProvider.builder()
                        .setEndpoint("context/entitymatching/predict")
                        .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                        .setAppIdentifier(getAppIdentifier())
                        .setSessionIdentifier(getSessionIdentifier())
                        .build();

        RequestParametersResponseParser jobStartResponseParser = RequestParametersResponseParser
                .of(ImmutableMap.of("jobId", "jobId"));

        GetPlaygroundJobIdRequestProvider jobResultsRequestProvider =
                GetPlaygroundJobIdRequestProvider.of("context/entitymatching/jobs")
                .toBuilder()
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(getAppIdentifier())
                .setSessionIdentifier(getSessionIdentifier())
                .build();

        return AsyncJobReader.of(jobStartRequestProvider, jobResultsRequestProvider, JsonItemResponseParser.create())
                .withJobStartResponseParser(jobStartResponseParser)
                .withMaxRetries(getMaxRetries().get());
    }

    /**
     * Create an entity matcher training executor.
     *
     * @return
     */
    public Connector<String> entityMatcherFit() {
        LOG.debug(loggingPrefix + "Initiating entity matcher training service.");
        this.validate();

        PostPlaygroundJsonRequestProvider jobStartRequestProvider =
                PostPlaygroundJsonRequestProvider.builder()
                        .setEndpoint("context/entitymatching")
                        .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                        .setAppIdentifier(getAppIdentifier())
                        .setSessionIdentifier(getSessionIdentifier())
                        .build();

        RequestParametersResponseParser jobStartResponseParser = RequestParametersResponseParser.of(
                ImmutableMap.of("id", "jobId"));

        GetPlaygroundJobIdRequestProvider jobResultsRequestProvider =
                GetPlaygroundJobIdRequestProvider.of("context/entitymatching")
                        .toBuilder()
                        .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                        .setAppIdentifier(getAppIdentifier())
                        .setSessionIdentifier(getSessionIdentifier())
                        .build();

        return AsyncJobReader.of(jobStartRequestProvider, jobResultsRequestProvider, JsonResponseParser.create())
                .withJobStartResponseParser(jobStartResponseParser)
                .withMaxRetries(getMaxRetries().get());
    }

    // TODO 3d models, revisions and nodes


    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setMaxRetries(ValueProvider<Integer> value);
        public Builder setMaxRetries(int value) {
            return this.setMaxRetries(ValueProvider.StaticValueProvider.of(value));
        }
        public abstract Builder setMaxBatchSize(int value);
        public abstract Builder setAppIdentifier(String value);
        public abstract Builder setSessionIdentifier(String value);
        /*
        public abstract Builder setHttpClient(OkHttpClient value);
        public abstract Builder setExecutorService(ExecutorService value);

         */

        abstract ConnectorServiceV1 autoBuild();

        public ConnectorServiceV1 build() {
            ConnectorServiceV1 service = autoBuild();
            Preconditions.checkState(service.getAppIdentifier().length() < 40
                    , "App identifier out of range. Length must be < 40.");
            Preconditions.checkState(service.getSessionIdentifier().length() < 40
                    , "Session identifier out of range. Length must be < 40.");
            return service;
        }
    }

    /**
     * Base class for read and write requests.
     */
    abstract static class ConnectorBase implements Serializable {
        // Default response parsers
        static final ResponseParser<String> DEFAULT_RESPONSE_PARSER = JsonItemResponseParser.create();
        static final ResponseParser<String> DEFAULT_DUPLICATES_RESPONSE_PARSER = JsonErrorItemResponseParser.builder()
                .setErrorSubPath("duplicated")
                .build();
        static final ResponseParser<String> DEFAULT_MISSING_RESPONSE_PARSER = JsonErrorItemResponseParser.builder()
                .setErrorSubPath("missing")
                .build();

        static final OkHttpClient DEFAULT_CLIENT = new OkHttpClient.Builder()
                .connectTimeout(90, TimeUnit.SECONDS)
                .readTimeout(90, TimeUnit.SECONDS)
                .writeTimeout(90, TimeUnit.SECONDS)
                //.addNetworkInterceptor(new SSLHandshakeInterceptor())
                //.connectionSpecs(Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS,
                //        new ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
                //                .allEnabledCipherSuites()
                //                .allEnabledTlsVersions()
                //                .build()))
                .build();

        static final RequestExecutor DEFAULT_REQUEST_EXECUTOR = RequestExecutor.of(DEFAULT_CLIENT)
                .withMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES);

        final Logger LOG = LoggerFactory.getLogger(this.getClass());

        abstract RequestProvider getRequestProvider();
        abstract int getMaxRetries(); // TODO: remove after refactor
        abstract RequestExecutor getRequestExecutor();

        abstract static class Builder<B extends Builder<B>> {
            abstract B setRequestProvider(RequestProvider value);
            abstract B setMaxRetries(int value); // TODO: remove after refactor
            abstract B setRequestExecutor(RequestExecutor value);
        }
    }

    /**
     * Iterator for paging through requests based on response cursors.
     *
     * This iterator is based on async request, and will return a {@link CompletableFuture} on each
     * {@code next()} call.
     *
     * However, {@code hasNext()} needs to wait for the current request to complete before being
     * able to evaluate if it carries a cursor to the next page.
     *
     * @param <T>
     */
    @AutoValue
    public abstract static class ResultFutureIterator<T>
            extends ConnectorBase implements Iterator<CompletableFuture<ResponseItems<T>>> {
        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "ResultFutureIterator [" + randomIdString + "] -";

        private CompletableFuture<ResponseItems<T>> currentResponseFuture = null;

        private static <T> Builder<T> builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_ResultFutureIterator.Builder<T>()
                    .setMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                    .setRequestExecutor(DEFAULT_REQUEST_EXECUTOR);
        }

        public static <T> ResultFutureIterator<T> of(RequestProvider requestProvider,
                                                     ResponseParser<T> responseParser) {
            return ResultFutureIterator.<T>builder()
                    .setRequestProvider(requestProvider)
                    .setResponseParser(responseParser)
                    .build();
        }

        abstract Builder<T> toBuilder();
        abstract ResponseParser<T> getResponseParser();

        /**
         * Set the max number of retries when executing requests against the Cognite API
         *
         * @param maxRetries the max number of retries.
         * @return The iterator configured with the new max retries.
         */
        ResultFutureIterator<T> withMaxRetries(int maxRetries) {
            Preconditions.checkArgument(maxRetries <= ConnectorConstants.MAX_MAX_RETRIES
                            && maxRetries >= ConnectorConstants.MIN_MAX_RETRIES
                    , "Max retries out of range. Must be between "
                            + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);
            return toBuilder()
                    .setMaxRetries(maxRetries)
                    .setRequestExecutor(getRequestExecutor().withMaxRetries(maxRetries))
                    .build();
        }

        /**
         * Sets the http client to use for api requests. Returns a {@link ResultFutureIterator} with
         * the setting applied.
         *
         * @param client The {@link OkHttpClient} to use.
         * @return a {@link ResultFutureIterator} object with the configuration applied.
         */
        public ResultFutureIterator<T> withHttpClient(OkHttpClient client) {
            Preconditions.checkNotNull(client, "The http client cannot be null.");
            return toBuilder()
                    .setRequestExecutor(getRequestExecutor().withHttpClient(client))
                    .build();
        }

        /**
         * Sets the {@link ExecutorService} to use for multi-threaded api requests. Returns
         * a {@link ResultFutureIterator} with the setting applied.
         *
         * @param executorService The {@link ExecutorService} to use.
         * @return a {@link ResultFutureIterator} object with the configuration applied.
         */
        public ResultFutureIterator<T> withExecutorService(ExecutorService executorService) {
            Preconditions.checkNotNull(executorService, "The executor service cannot be null");
            return toBuilder()
                    .setRequestExecutor(getRequestExecutor().withExecutor(executorService))
                    .build();
        }

        @Override
        public boolean hasNext() {
            // must wrap the logic in a try-catch since <hasNext> does not allow us to just re-throw the exceptions.
            try {
                if (currentResponseFuture == null) {
                    // Have not issued any request yet, so always return true to perform the first request.
                    return true;
                } else {
                    // We are past the first request. Check the current item if it has a next cursor
                    return getResponseParser().extractNextCursor(
                            currentResponseFuture.join().getResponseBinary().getResponseBodyBytes().toByteArray()
                            ).isPresent();
                }
            } catch (Exception e) {
                LOG.error(loggingPrefix + "Error when executing check for <hasNext>.", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public CompletableFuture<ResponseItems<T>> next() throws NoSuchElementException {
            if (!this.hasNext()) {
                LOG.warn(loggingPrefix + "Client calling next() when no more elements are left to iterate over");
                throw new NoSuchElementException("No more elements to iterate over.");
            }
            try {
                Optional<String> nextCursor = Optional.empty();
                if (null != currentResponseFuture) {
                    nextCursor = getResponseParser().extractNextCursor(
                            currentResponseFuture.join().getResponseBinary().getResponseBodyBytes().toByteArray()
                            );
                    LOG.debug(loggingPrefix + "More items to iterate over. Building next api request based on cursor: {}",
                            nextCursor.orElse("could not identify cursor--should be investigated"));

                    // should not happen, but let's check just in case.
                    if (!nextCursor.isPresent()) {
                        String message = loggingPrefix + "Invalid state. <hasNext()> indicated one more element,"
                                + " but no next cursor is found.";
                        LOG.error(message);
                        throw new Exception(message);
                    }
                } else {
                    LOG.debug(loggingPrefix + "Building first api request of the iterator.");
                }
                Request request = this.getRequestProvider().buildRequest(nextCursor);
                LOG.debug(loggingPrefix + "Built request for URL: {}", request.url().toString());

                // Execute the request and get the response future
                CompletableFuture<ResponseItems<T>> responseItemsFuture = getRequestExecutor()
                        .executeRequestAsync(request)
                        .thenApply(responseBinary ->
                            ResponseItems.of(getResponseParser(), responseBinary));

                currentResponseFuture = responseItemsFuture;
                return responseItemsFuture;

            } catch (Exception e) {
                String message = loggingPrefix + "Failed to get more elements when requesting new batch from Fusion: ";
                LOG.error(message, e);
                throw new NoSuchElementException(message + System.lineSeparator() + e.getMessage());
            }
        }

        @AutoValue.Builder
        abstract static class Builder<T> extends ConnectorBase.Builder<Builder<T>> {
            abstract Builder<T> setResponseParser(ResponseParser<T> value);

            abstract ResultFutureIterator<T> build();
        }
    }

    /**
     * Reads items from the Cognite API.
     *
     * This reader targets API endpoints which complete its operation in a single request.
     */
    @AutoValue
    public static abstract class SingleRequestItemReader<T> extends ConnectorBase implements ItemReader<T> {
        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "SingleRequestItemReader [" + randomIdString + "] -";

        private final RequestExecutor baseRequestExecutor = RequestExecutor.of(DEFAULT_CLIENT)
                .withValidResponseCodes(ImmutableList.of(400, 401, 409, 422))
                .withMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES);

        static <T> Builder<T> builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_SingleRequestItemReader.Builder<T>()
                    .setMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                    .setRequestExecutor(DEFAULT_REQUEST_EXECUTOR);
        }

        /**
         * Creates an instance of this reader. You must provide a {@link RequestProvider} and a
         * {@link ResponseParser}.
         *
         * @param requestProvider
         * @param responseParser
         * @param <T>
         * @return
         */
        static <T> SingleRequestItemReader<T> of(RequestProvider requestProvider,
                                                 ResponseParser<T> responseParser) {
            return SingleRequestItemReader.<T>builder()
                    .setRequestProvider(requestProvider)
                    .setResponseParser(responseParser)
                    .build();
        }

        abstract Builder<T> toBuilder();
        abstract ResponseParser<T> getResponseParser();

        /**
         * Set the max number of retries when executing requests against the Cognite API
         *
         * @param maxRetries
         * @return
         */
        SingleRequestItemReader<T> withMaxRetries(int maxRetries) {
            return toBuilder().setMaxRetries(maxRetries).build();
        }

        /**
         * Executes a request to get items and blocks the thread until all items have been downloaded.
         *
         *
         * @param items
         * @return
         * @throws Exception
         */
        public ResponseItems<T> getItems(RequestParameters items) throws Exception {
            return this.getItemsAsync(items).join();
        }

        /**
         * Executes an item-based request to get items asynchronously.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<T>> getItemsAsync(RequestParameters items) throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");

            RequestExecutor requestExecutor = baseRequestExecutor
                    .withMaxRetries(getMaxRetries());

            // Execute the request and get the response future
            CompletableFuture<ResponseItems<T>> responseItemsFuture = requestExecutor
                    .executeRequestAsync(getRequestProvider()
                            .withRequestParameters(items)
                            .buildRequest(Optional.empty()))
                    .thenApply(responseBinary ->
                        ResponseItems.of(getResponseParser(), responseBinary));

            return responseItemsFuture;
        }

        @AutoValue.Builder
        abstract static class Builder<T> extends ConnectorBase.Builder<Builder<T>> {
            abstract Builder<T> setResponseParser(ResponseParser<T> value);

            abstract SingleRequestItemReader<T> autoBuild();

            SingleRequestItemReader<T> build() {
                SingleRequestItemReader<T> reader = autoBuild();
                Preconditions.checkState(reader.getMaxRetries() <= ConnectorConstants.MAX_MAX_RETRIES
                                && reader.getMaxRetries() >= ConnectorConstants.MIN_MAX_RETRIES
                        , "Max retries out of range. Must be between "
                                + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);

                return reader;
            }
        }
    }

    /**
     * Reads items from the Cognite API.
     *
     * This reader targets API endpoints which complete its operation over two requests. I.e. async api
     * pattern where the first request starts the job and the second request collects the result.
     *
     * The reader works in two steps:
     * 1. Issue a "start job" request.
     * 2. Start a polling loop issuing "get job results/status" requests.
     *
     * The "start job" step is executed using the input {@link RequestParameters}, the configured
     * {@code RequestProvider} "jobStartRequestProvider" and the configured {@code JobStartResponseParser}.
     * This request will typically yield a response which contains the async job identification
     * parameters ({@code jobId}, {@code modelId}, etc.).
     * These identification parameters are again used as input for step 2, "get job results".
     *
     * The output from step 1 is a {@link RequestParameters} object representing the parameters to be used
     * as input to step 2, "get job results/update". The {@link RequestParameters} is interpreted by the
     * {@code JobResultRequestProvider} and the api will regularly be polled for job results. When the api provides
     * job results, this response is routed to the {@code ResponseParser} and returned.
     *
     */
    @AutoValue
    public static abstract class AsyncJobReader<T> extends ConnectorBase implements Connector<T>, ItemReader<T> {
        // parsers for various job status attributes
        private static final JsonStringAttributeResponseParser jobStatusResponseParser =
                JsonStringAttributeResponseParser.create().withAttributePath("status");
        private static final JsonStringAttributeResponseParser errorMessageResponseParser =
                JsonStringAttributeResponseParser.create().withAttributePath("errorMessage");

        // default request and response parser / providers
        private static final RequestParametersResponseParser DEFAULT_JOB_START_RESPONSE_PARSER =
                RequestParametersResponseParser.of(ImmutableMap.of(
                        "modelId", "modelId",
                        "jobId", "jobId"
                ));

        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "AsyncJobReader [" + randomIdString + "] -";
        private final ObjectMapper objectMapper = JsonUtil.getObjectMapperInstance();

        // Instantiate a base request executor in order to get a stable thread pool for executing repeated requests
        // via the same reader.
        private final RequestExecutor baseRequestExecutor = RequestExecutor.of(DEFAULT_CLIENT)
                .withValidResponseCodes(ImmutableList.of(400, 401, 409, 422))
                .withMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES);

        static <T> Builder<T> builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_AsyncJobReader.Builder<T>()
                    .setMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                    .setJobStartResponseParser(DEFAULT_JOB_START_RESPONSE_PARSER)
                    .setJobTimeoutDuration(DEFAULT_ASYNC_API_JOB_TIMEOUT)
                    .setPollingInterval(DEFAULT_ASYNC_API_JOB_POLLING_INTERVAL)
                    .setRequestExecutor(DEFAULT_REQUEST_EXECUTOR);
        }

        /**
         * Creates an instance of this reader. You must provide a {@link RequestProvider} and a
         * {@link ResponseParser}.
         *
         * @param jobStartRequestProvider
         * @param jobResultRequestProvider
         * @param responseParser
         * @param <T>
         * @return
         */
        static <T> AsyncJobReader<T> of(RequestProvider jobStartRequestProvider,
                                        RequestProvider jobResultRequestProvider,
                                        ResponseParser<T> responseParser) {
            return AsyncJobReader.<T>builder()
                    .setRequestProvider(jobStartRequestProvider)
                    .setJobResultRequestProvider(jobResultRequestProvider)
                    .setResponseParser(responseParser)
                    .build();
        }

        abstract Builder<T> toBuilder();
        abstract ResponseParser<T> getResponseParser();
        abstract ResponseParser<RequestParameters> getJobStartResponseParser();
        abstract RequestProvider getJobResultRequestProvider();
        abstract Duration getJobTimeoutDuration();
        abstract Duration getPollingInterval();

        /**
         * Set the max number of retries when executing requests against the Cognite API
         *
         * @param maxRetries
         * @return
         */
        AsyncJobReader<T> withMaxRetries(int maxRetries) {
            return toBuilder().setMaxRetries(maxRetries).build();
        }

        /**
         * Sets the {@link ResponseParser} for the first/initial api request. This request will typically be
         * the job start/register request. This {@link ResponseParser} is responsible for generating the
         * {@link RequestParameters} for the second api request--the request that retrieves the job results.
         *
         * The job results request is built by the {@code JobResultRequestProvider}.
         *
         * @param responseParser
         * @return
         */
        AsyncJobReader<T> withJobStartResponseParser(ResponseParser<RequestParameters> responseParser) {
            return toBuilder().setJobStartResponseParser(responseParser).build();
        }

        /**
         * Set the {@code RequestProvider} for the second api request. This request will typically
         * retrieve the job results. This request provider receives its {@link RequestParameters} from
         * the {@code JobStartResponseParser}.
         *
         * @param requestProvider
         * @return
         */
        AsyncJobReader<T> withJobResultRequestProvider(RequestProvider requestProvider) {
            return toBuilder().setJobResultRequestProvider(requestProvider).build();
        }

        /**
         * Sets the timeout for the api job. This reader will wait for up to the timeout duration for the api
         * to complete the job.
         *
         * When the timeout is triggered, the reader will respond with the current job status (most likely
         * "Queued" or "Running").
         *
         * The default timeout is 10 minutes.
         *
         * @param timeout
         * @return
         */
        AsyncJobReader<T> withJobTimeout(Duration timeout) {
            return toBuilder().setJobTimeoutDuration(timeout).build();
        }

        /**
         * Sets the polling interval. The polling interval determines how often the reader requests a status
         * update from the api. This will affect the perceived responsiveness of the reader.
         *
         * Don't set the polling interval too low, or you risk overloading the api.
         *
         * The default polling interval is 1 sec.
         *
         * @param interval
         * @return
         */
        AsyncJobReader<T> withPollingInterval(Duration interval) {
            return toBuilder().setPollingInterval(interval).build();
        }

        /**
         * Executes a request to get items and blocks the thread until all items have been downloaded.
         *
         *
         * @param items
         * @return
         * @throws Exception
         */
        public ResponseItems<T> execute(RequestParameters items) throws Exception {
            return this.getItemsAsync(items).join();
        }

        /**
         * Executes an item-based request to get items asynchronously.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<T>> executeAsync(RequestParameters items) throws Exception {
            return this.getItemsAsync(items);
        }

        /**
         * Executes a request to get items and blocks the thread until all items have been downloaded.
         *
         *
         * @param items
         * @return
         * @throws Exception
         */
        public ResponseItems<T> getItems(RequestParameters items) throws Exception {
            return this.getItemsAsync(items).join();
        }

        /**
         * Executes an item-based request to get items asynchronously.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<T>> getItemsAsync(RequestParameters items) throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");

            RequestExecutor requestExecutor = baseRequestExecutor
                    .withMaxRetries(getMaxRetries());

            LOG.info(loggingPrefix + "Starting async api job.");

            // Execute the request and get the response future
            CompletableFuture<ResponseItems<T>> responseItemsFuture = requestExecutor
                    .executeRequestAsync(getRequestProvider()
                            .withRequestParameters(items)
                            .buildRequest(Optional.empty()))
                    .thenApply(responseBinary ->
                            ResponseItems.of(getJobStartResponseParser(), responseBinary)
                                    .withErrorMessageResponseParser(errorMessageResponseParser)
                                    .withStatusResponseParser(jobStatusResponseParser))
                    .thenCompose(responseItems -> {
                        try {
                            List<String> responseStatus = responseItems.getStatus();
                            List<RequestParameters> resultsItems = responseItems.getResultsItems();

                            if (responseItems.isSuccessful()
                                    && !responseStatus.isEmpty()
                                    && !responseStatus.get(0).equalsIgnoreCase("Failed")
                                    && !resultsItems.isEmpty()) {
                                // We successfully started the async job
                                LOG.info(loggingPrefix + "The async api job successfully started with status: {},"
                                        + "Id: {}, JobId: {}",
                                        responseStatus.get(0),
                                        ItemParser.parseString(responseItems.getResponseBodyAsString(), "id")
                                                .orElse("[no id]"),
                                        ItemParser.parseString(responseItems.getResponseBodyAsString(), "jobId")
                                                .orElse("[no JobId]"));

                                // Activate the get results loop and return the end result.
                                // Must copy the project config (auth details) from the original request.
                                return getJobResultAsync(resultsItems.get(0).withProjectConfig(items.getProjectConfig()),
                                        items);
                            } else {
                                // The async job did not start successfully
                                LOG.warn(loggingPrefix + "The async api job failed to start. Status: {}. Response payload: {} "
                                        + " Response headers: {}",
                                        !responseStatus.isEmpty() ? responseStatus.get(0) : "<no status available>",
                                        responseItems.getResponseBodyAsString(),
                                        responseItems.getResponseBinary().getResponse().headers());

                                return CompletableFuture.completedFuture(
                                        ResponseItems.of(getResponseParser(), responseItems.getResponseBinary())
                                );
                            }
                        } catch (Exception e) {
                            String message = String.format(loggingPrefix + "The async api job failed to start. Response headers: %s%n"
                                    + "Response summary: %s%n"
                                    + " Exception: %s",
                                    responseItems.getResponseBinary().getResponse().headers().toString(),
                                    responseItems.getResponseBodyAsString(),
                                    e.getMessage());
                            LOG.error(message);
                            CompletableFuture<ResponseItems<T>> exceptionally = new CompletableFuture<>();
                            exceptionally.completeExceptionally(new Throwable(message, e));

                            return exceptionally;
                        }
                    })
                    ;

            return responseItemsFuture;
        }

        /*
        Get the job results via a polling loop.
         */
        private CompletableFuture<ResponseItems<T>> getJobResultAsync(RequestParameters request,
                                                                      RequestParameters jobStartRequest) {
            Preconditions.checkNotNull(request, "Input cannot be null.");

            RequestExecutor requestExecutor = baseRequestExecutor
                    .withMaxRetries(getMaxRetries());

            // Execute the request and get the response future
            CompletableFuture<ResponseItems<T>> responseItemsFuture = CompletableFuture
                    .supplyAsync(() -> {
                        long timeStart = System.currentTimeMillis();
                        long timeMax = System.currentTimeMillis() + getJobTimeoutDuration().toMillis();
                        Map<String, Object> jobResultItems = null;
                        ResponseBinary jobResultResponse = null;
                        boolean jobComplete = false;

                        LOG.info(loggingPrefix + "Start polling the async api for results.");
                        while (!jobComplete && System.currentTimeMillis() < timeMax) {
                            try {
                                Thread.sleep(getPollingInterval().toMillis());

                                jobResultResponse = requestExecutor
                                        .executeRequestAsync(getJobResultRequestProvider()
                                                .withRequestParameters(request)
                                                .buildRequest(Optional.empty()))
                                        .join();

                                String payload = jobResultResponse.getResponseBodyBytes().toStringUtf8();
                                jobResultItems = objectMapper
                                        .readValue(payload, new TypeReference<Map<String, Object>>() {});

                                if (jobResultItems.containsKey("errorMessage")
                                        && ((String) jobResultItems.get("status")).equalsIgnoreCase("Failed")) {
                                    // The api job has failed.
                                    String message = String.format(loggingPrefix
                                            + "Error occurred when completing the async api job. "
                                            + "Status: Failed. Error message: %s %n"
                                            + "Request: %s"
                                            + "Response headers: %s"
                                            + "Job start request: %s",
                                            jobResultItems.get("errorMessage"),
                                            request.getRequestParametersAsJson(),
                                            jobResultResponse.getResponse().headers(),
                                            jobStartRequest.getRequestParametersAsJson().substring(0,
                                                    Math.min(300, jobStartRequest.getRequestParametersAsJson().length())));
                                    LOG.error(message);
                                    throw new Exception(message);
                                }

                                if (((String) jobResultItems.get("status")).equalsIgnoreCase("Completed")) {
                                    jobComplete = true;
                                } else {
                                    LOG.debug(loggingPrefix + "Async job not completed. Status : {}",
                                            jobResultItems.get("status"));
                                }
                            } catch (Exception e) {
                                LOG.error(loggingPrefix + "Failed to get async api job results. Exception: {}. ",
                                        e.getMessage());
                                throw new CompletionException(e);
                            }
                        }

                        if (jobComplete) {
                            LOG.info(loggingPrefix + "Job results received.");
                            // Try to register job queue time
                            if (null != jobResultItems
                                    && jobResultItems.containsKey("requestTimestamp")
                                    && null != jobResultItems.get("requestTimestamp")
                                    && jobResultItems.containsKey("startTimestamp")
                                    && null != jobResultItems.get("startTimestamp")) {
                                long requestTimestamp = (Long) jobResultItems.get("requestTimestamp");
                                long startTimestamp = (Long) jobResultItems.get("startTimestamp");
                                jobResultResponse = jobResultResponse
                                        .withApiJobQueueDuration(startTimestamp - requestTimestamp);
                            }
                            // Try to register job execution time
                            if (null != jobResultItems
                                    && jobResultItems.containsKey("startTimestamp")
                                    && null != jobResultItems.get("startTimestamp")
                                    && jobResultItems.containsKey("statusTimestamp")
                                    && null != jobResultItems.get("statusTimestamp")) {
                                long startTimestamp = (Long) jobResultItems.get("startTimestamp");
                                long statusTimestamp = (Long) jobResultItems.get("statusTimestamp");
                                jobResultResponse = jobResultResponse
                                        .withApiJobDuration(statusTimestamp - startTimestamp);
                            } else {
                                // register job execution time using the local time registrations
                                jobResultResponse = jobResultResponse
                                        .withApiJobDuration(System.currentTimeMillis() - timeStart);
                            }
                        } else {
                            String message = "The api job did not complete within the given timeout. Request parameters: "
                                    + request.getRequestParameters().toString();
                            LOG.error(loggingPrefix + message);
                            throw new CompletionException(
                                    new Throwable(message + " Job status: " + jobResultItems.get("status")));
                        }

                        ResponseItems<T> responseItems = ResponseItems.of(getResponseParser(), jobResultResponse)
                                .withStatusResponseParser(jobStatusResponseParser)
                                .withErrorMessageResponseParser(errorMessageResponseParser);

                        return responseItems;
                    });

            return responseItemsFuture;
        }

        @AutoValue.Builder
        abstract static class Builder<T> extends ConnectorBase.Builder<Builder<T>> {
            abstract Builder<T> setResponseParser(ResponseParser<T> value);
            abstract Builder<T> setJobStartResponseParser(ResponseParser<RequestParameters> value);
            abstract Builder<T> setJobResultRequestProvider(RequestProvider value);
            abstract Builder<T> setJobTimeoutDuration(Duration value);
            abstract Builder<T> setPollingInterval(Duration value);

            abstract AsyncJobReader<T> autoBuild();

            AsyncJobReader<T> build() {
                AsyncJobReader<T> reader = autoBuild();
                Preconditions.checkState(reader.getMaxRetries() <= ConnectorConstants.MAX_MAX_RETRIES
                                && reader.getMaxRetries() >= ConnectorConstants.MIN_MAX_RETRIES
                        , "Max retries out of range. Must be between "
                                + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);

                return reader;
            }
        }
    }

    @AutoValue
    public static abstract class ItemWriter extends ConnectorBase {

        static Builder builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_ItemWriter.Builder()
                    .setMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                    .setRequestExecutor(DEFAULT_REQUEST_EXECUTOR
                            .withValidResponseCodes(ImmutableList.of(400, 409, 422)))
                    .setDuplicatesResponseParser(DEFAULT_DUPLICATES_RESPONSE_PARSER);
        }

        static ItemWriter of(RequestProvider requestProvider) {
            return ItemWriter.builder()
                    .setRequestProvider(requestProvider)
                    .build();
        }

        abstract Builder toBuilder();

        public abstract ResponseParser<String> getDuplicatesResponseParser();

        /**
         * Configures the duplicates response parser to use.
         *
         * The default duplicates response parser looks for duplicates at the {@code error.duplicates}
         * node in the response.
         * @param parser
         * @return
         */
        public ItemWriter withDuplicatesResponseParser(ResponseParser<String> parser) {
            return toBuilder().setDuplicatesResponseParser(parser).build();
        }

        /**
         * Sets the maximum number retries before failing a request. The default max number of
         * retries is 3.
         *
         * @param retries
         * @return
         */
        public ItemWriter withMaxRetries(int retries) {
            return toBuilder().setMaxRetries(retries).build();
        }

        /**
         * Sets the http client to use for api requests. Returns a {@link ResultFutureIterator} with
         * the setting applied.
         *
         * @param client The {@link OkHttpClient} to use.
         * @return a {@link ItemWriter} object with the configuration applied.
         */
        public ItemWriter withHttpClient(OkHttpClient client) {
            Preconditions.checkNotNull(client, "The http client cannot be null.");
            return toBuilder()
                    .setRequestExecutor(getRequestExecutor().withHttpClient(client))
                    .build();
        }

        /**
         * Sets the {@link ExecutorService} to use for multi-threaded api requests. Returns
         * a {@link ResultFutureIterator} with the setting applied.
         *
         * @param executorService The {@link ExecutorService} to use.
         * @return a {@link ItemWriter} object with the configuration applied.
         */
        public ItemWriter withExecutorService(ExecutorService executorService) {
            Preconditions.checkNotNull(executorService, "The executor service cannot be null");
            return toBuilder()
                    .setRequestExecutor(getRequestExecutor().withExecutor(executorService))
                    .build();
        }

        /**
         * Executes an item-based write request.
         *
         * This method will block until the response is ready. The async version of this method is
         * {@code writeItemsAsync}.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public ResponseItems<String> writeItems(RequestParameters items) throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");
            return this.writeItemsAsync(items).join();
        }

        /**
         * Executes an item-based write request asynchronously.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<String>> writeItemsAsync(RequestParameters items) throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");

            return getRequestExecutor().executeRequestAsync(getRequestProvider()
                    .withRequestParameters(items)
                    .buildRequest(Optional.empty()))
                    .thenApply(responseBinary -> ResponseItems.of(DEFAULT_RESPONSE_PARSER, responseBinary)
                            .withDuplicateResponseParser(getDuplicatesResponseParser()));
        }

        @AutoValue.Builder
        abstract static class Builder extends ConnectorBase.Builder<Builder> {
            abstract Builder setDuplicatesResponseParser(ResponseParser<String> value);

            abstract ItemWriter autoBuild();

            ItemWriter build() {
                ItemWriter writer = autoBuild();
                Preconditions.checkState(writer.getMaxRetries() <= ConnectorConstants.MAX_MAX_RETRIES
                                && writer.getMaxRetries() >= ConnectorConstants.MIN_MAX_RETRIES
                        , "Max retries out of range. Must be between "
                                + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);

                return writer;
            }
        }
    }

    /**
     * Reads file binaries from the Cognite API.
     */
    @AutoValue
    public static abstract class FileBinaryReader extends ConnectorBase {
        private static int DEFAULT_MAX_BATCH_SIZE_FILE_READ = 100;
        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "FileBinaryReader [" + randomIdString + "] -";
        private final ObjectMapper objectMapper = new ObjectMapper();

        static Builder builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_FileBinaryReader.Builder()
                    .setAppIdentifier(DEFAULT_APP_IDENTIFIER)
                    .setSessionIdentifier(DEFAULT_SESSION_IDENTIFIER)
                    .setMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                    .setRequestExecutor(DEFAULT_REQUEST_EXECUTOR)
                    .setForceTempStorage(false)
                    .setRequestProvider(PostJsonRequestProvider.builder()
                            .setEndpoint("files/downloadlink")
                            .setRequestParameters(RequestParameters.create())
                            .build());
        }

        abstract Builder toBuilder();

        abstract String getAppIdentifier();
        abstract String getSessionIdentifier();
        abstract boolean isForceTempStorage();

        @Nullable
        abstract URI getTempStoragePath();

        /**
         * Forces the use of temp storage for all binaries--not just the >200MiB ones.
         *
         * @param enable
         * @return
         */
        public FileBinaryReader enableForceTempStorage(boolean enable) {
            return toBuilder().setForceTempStorage(enable).build();
        }

        /**
         * Sets the temporary storage path for storing large file binaries. If the binary is >200 MiB it will be
         * stored in temp storage instead of in memory.
         *
         * The following storage providers are supported:
         * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
         * - Local (network) storage.
         *
         * @param path The URI to the temp storage
         * @return a {@FileBinaryReader} with temp storage configured.
         */
        public FileBinaryReader withTempStoragePath(URI path) {
            Preconditions.checkArgument(null != path,
                    "Temp storage path cannot be null or empty.");
            return toBuilder().setTempStoragePath(path).build();
        }

        /**
         * Executes an item-based request to get file binaries and blocks the thread until all files have been downloaded.
         *
         * We recommend reading a limited set of files per request, between 1 and 10--depending on the file size.
         * If the request fails due to missing items, it will return a single <code>ResultsItems</code>.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public List<ResponseItems<FileBinary>> readFileBinaries(RequestParameters items) throws Exception {
            return this.readFileBinariesAsync(items).join();
        }

        /**
         * Executes an item-based request to get file binaries asynchronously.
         *
         * We recommend reading a limited set of files per request, between 1 and 10--depending on the file size.
         * If the request fails due to missing items, it will return a single <code>ResultsItems</code>.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<List<ResponseItems<FileBinary>>> readFileBinariesAsync(RequestParameters items)
                throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");
            LOG.debug(loggingPrefix + "Received {} file items requested to download.", items.getItems().size());

            PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                    .setEndpoint("files/downloadlink")
                    .setRequestParameters(RequestParameters.create())
                    .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                    .setAppIdentifier(getAppIdentifier())
                    .setSessionIdentifier(getSessionIdentifier())
                    .build();

            RequestExecutor requestExecutor = RequestExecutor.of(DEFAULT_CLIENT)
                    .withValidResponseCodes(ImmutableList.of(400, 409, 422))
                    .withMaxRetries(getMaxRetries());

            // Get the file download links
            ResponseItems<String> fileItemsResponse = requestExecutor
                    .executeRequestAsync(requestProvider
                            .withRequestParameters(items)
                            .buildRequest(Optional.empty()))
                    .thenApply(responseBinary -> ResponseItems.of(JsonItemResponseParser.create(), responseBinary))
                    .join();

            if (!fileItemsResponse.isSuccessful()) {
                LOG.warn(loggingPrefix + "Unable to retrieve the file download URLs");
                return CompletableFuture.completedFuture(ImmutableList.of(
                        ResponseItems.of(FileBinaryResponseParser.create(), fileItemsResponse.getResponseBinary()))
                );
            }

            List<String> fileItems = fileItemsResponse.getResultsItems();
            LOG.info(loggingPrefix + "Received {} file download URLs.", fileItems.size());

            // Start download the file binaries on separate threads
            List<CompletableFuture<ResponseItems<FileBinary>>> futuresList = new ArrayList<>(fileItems.size());

            for (String fileItem : fileItems) {
                LOG.debug(loggingPrefix + "Building download request for file item: {}", fileItem);
                Map<String, Object> fileRequestItem = objectMapper.readValue(fileItem, new TypeReference<Map<String, Object>>(){});
                Preconditions.checkState(fileRequestItem != null && fileRequestItem.containsKey("downloadUrl"),
                        "File response does not contain a valid *downloadUrl* item.");
                Preconditions.checkState(fileRequestItem.containsKey("externalId") || fileRequestItem.containsKey("id"),
                        "File response does not contain a file id nor externalId.");

                // build file id and url
                String fileExternalId = (String) fileRequestItem.getOrDefault("externalId", "");
                long fileId = (Long) fileRequestItem.getOrDefault("id", -1);
                String downloadUrl = (String) fileRequestItem.get("downloadUrl");

                CompletableFuture<ResponseItems<FileBinary>> future = DownloadFileBinary
                        .downloadFileBinaryFromURL(downloadUrl, getTempStoragePath(), isForceTempStorage())
                        .thenApply(fileBinary -> {
                            // add the file id
                            if (!fileExternalId.isEmpty()) {
                                return fileBinary.toBuilder()
                                        .setExternalId(fileExternalId)
                                        .build();
                            } else {
                                return fileBinary.toBuilder()
                                        .setId(fileId)
                                        .build();
                            }
                        })
                        .thenApply(fileBinary -> {
                            // add a response items wrapper
                            return ResponseItems.of(FileBinaryResponseParser.create(), fileItemsResponse.getResponseBinary())
                                    .withResultsItemsList(ImmutableList.of(fileBinary));
                        });

                futuresList.add(future);
            }

            // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(futuresList.toArray(
                    new CompletableFuture[futuresList.size()]));

            CompletableFuture<List<ResponseItems<FileBinary>>> responseItemsFuture = allFutures
                    .thenApply(future ->
                            futuresList.stream()
                                    .map(CompletableFuture::join)
                                    .collect(Collectors.toList())
                    );

            return responseItemsFuture;
        }

        @AutoValue.Builder
        abstract static class Builder extends ConnectorBase.Builder<Builder> {
            abstract Builder setAppIdentifier(String value);
            abstract Builder setSessionIdentifier(String value);
            abstract Builder setForceTempStorage(boolean value);
            abstract Builder setTempStoragePath(URI value);

            abstract FileBinaryReader autoBuild();

            FileBinaryReader build() {
                FileBinaryReader reader = autoBuild();
                Preconditions.checkState(reader.getMaxRetries() <= ConnectorConstants.MAX_MAX_RETRIES
                                && reader.getMaxRetries() >= ConnectorConstants.MIN_MAX_RETRIES
                        , "Max retries out of range. Must be between "
                                + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);

                return reader;
            }
        }
    }

    /**
     * Writes files (header + binary) to the Cognite API.
     *
     */
    @AutoValue
    public static abstract class FileWriter extends ConnectorBase {
        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "FileWriter [" + randomIdString + "] -";
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final int CPU_MULTIPLIER = 8;
        private final int MAX_CPU = 20;

        private final RequestExecutor baseRequestExecutor = RequestExecutor.of(DEFAULT_CLIENT)
                .withValidResponseCodes(ImmutableList.of(400))
                .withMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                .withExecutor(new ForkJoinPool(
                        Math.min(Runtime.getRuntime().availableProcessors() * CPU_MULTIPLIER, MAX_CPU)));

        private final FileBinaryRequestExecutor baseFileBinaryRequestExecutor = FileBinaryRequestExecutor.of(DEFAULT_CLIENT)
                .withMaxRetries(DEFAULT_MAX_RETRIES);

        static Builder builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_FileWriter.Builder()
                    .setAppIdentifier(DEFAULT_APP_IDENTIFIER)
                    .setSessionIdentifier(DEFAULT_SESSION_IDENTIFIER)
                    .setMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                    .setRequestExecutor(DEFAULT_REQUEST_EXECUTOR)
                    .setRequestProvider(PostJsonRequestProvider.builder()
                            .setEndpoint("files")
                            .setRequestParameters(RequestParameters.create())
                            .build())
                    .setDeleteTempFile(true);
        }

        abstract Builder toBuilder();

        abstract String getAppIdentifier();
        abstract String getSessionIdentifier();
        abstract boolean isDeleteTempFile();

        /**
         * Configure how to treat a temp blob after an upload. This setting only affects behavior when uploading
         * file binaries to the Cognite API--it has no effect on downloading file binaries.
         *
         * When set to {@code true}, the temp file (if present) will be removed after a successful upload. If the file
         * binary is memory-based (which is the default for small and medium sized files), this setting has no effect.
         *
         * When set to {@code false}, the temp file (if present) will not be deleted.
         *
         * The default setting is {@code true}.
         *
         * @param enable
         * @return
         */
        public FileWriter enableDeleteTempFile(boolean enable) {
            return toBuilder().setDeleteTempFile(enable).build();
        }

        /**
         * Writes file metadata and binaries and blocks the call until the file write completes.
         *
         * The <code>RequestParameter</code> input must contain a proto payload with a <code>FileContainer</code> object
         * which contains both the file metadata and the file binary.
         *
         * @param fileContainerRequest
         * @return
         * @throws Exception
         */
        public ResponseItems<String> writeFile(RequestParameters fileContainerRequest) throws Exception {
            return this.writeFileAsync(fileContainerRequest).join();
        }

        /**
         * Writes file metadata and binaries asynchronously.
         *
         * The <code>RequestParameter</code> input must contain a proto payload with a <code>FileContainer</code> object
         * which contains both the file metadata and the file binary.
         *
         * @param fileContainerRequest
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<String>> writeFileAsync(RequestParameters fileContainerRequest)
                throws Exception {
            final String uploadUrlKey = "uploadUrl";    // key for extracting the upload URL from the api json payload

            Preconditions.checkNotNull(fileContainerRequest, "Input cannot be null.");
            Preconditions.checkNotNull(fileContainerRequest.getProtoRequestBody(),
                    "No protobuf request body found.");
            Preconditions.checkArgument(fileContainerRequest.getProtoRequestBody() instanceof FileContainer,
                    "The protobuf request body is not of type FileContainer");
            Preconditions.checkArgument(!((FileContainer) fileContainerRequest.getProtoRequestBody())
                    .getFileMetadata().getName().getValue().isEmpty(),
                    "The request body must contain a file name in the file header section.");
            FileContainer fileContainer = (FileContainer) fileContainerRequest.getProtoRequestBody();
            boolean hasExtraAssetIds = false;

            LOG.info(loggingPrefix + "Received file container to write. Name: {}, Binary type: {}, Size: {}MB, "
                    + "Binary URI: {}, Number of asset links: {}, Number of metadata fields: {}",
                    fileContainer.getFileMetadata().getName().getValue(),
                    fileContainer.getFileBinary().getBinaryTypeCase().toString(),
                    String.format("%.3f", fileContainer.getFileBinary().getBinary().size() / (1024d * 1024d)),
                    fileContainer.getFileBinary().getBinaryUri(),
                    fileContainer.getFileMetadata().getAssetIdsCount(),
                    fileContainer.getFileMetadata().getMetadataCount());

            FilesUploadRequestProvider requestProvider = FilesUploadRequestProvider.builder()
                    .setEndpoint("files")
                    .setRequestParameters(RequestParameters.create())
                    .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                    .setAppIdentifier(getAppIdentifier())
                    .setSessionIdentifier(getSessionIdentifier())
                    .build();

            RequestExecutor requestExecutor = baseRequestExecutor
                    .withMaxRetries(getMaxRetries());

            // Build the file metadata request
            FileMetadata fileMetadata = fileContainer.getFileMetadata();
            FileMetadata fileMetadataExtraAssets = null;
            if (fileMetadata.getAssetIdsCount() > 1000) {
                LOG.info(loggingPrefix + "File contains >1k asset links. Will upload the file first and patch the assets afterwards");
                hasExtraAssetIds = true;
                fileMetadataExtraAssets = fileMetadata.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsCount()))
                        .build();
                fileMetadata = fileMetadata.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0, 1000))
                        .build();
            }

            RequestParameters postFileMetaRequest = fileContainerRequest
                    .withRequestParameters(FileParser.toRequestInsertItem(fileMetadata));

            // Post the file metadata and get the file upload links
            ResponseBinary fileUploadResponse = requestExecutor
                    .executeRequestAsync(requestProvider
                            .withRequestParameters(postFileMetaRequest)
                            .buildRequest(Optional.empty()))
                    .join();

            if (!fileUploadResponse.getResponse().isSuccessful()) {
                LOG.warn(loggingPrefix + "Unable to retrieve the file upload URLs");
                return CompletableFuture.completedFuture(
                        ResponseItems.of(JsonItemResponseParser.create(), fileUploadResponse)
                );
            }

            // Parse the upload response
            String jsonResponsePayload = fileUploadResponse.getResponseBodyBytes().toStringUtf8();
            Map<String, Object> fileUploadResponseItem = objectMapper
                    .readValue(jsonResponsePayload, new TypeReference<Map<String, Object>>(){});
            LOG.info(loggingPrefix + "Posted file metadata for [{}]. Received file upload URL response.",
                    fileContainer.getFileMetadata().getName().getValue());

            Preconditions.checkState(fileUploadResponseItem.containsKey(uploadUrlKey),
                    "Unable to retrieve upload URL from the CogniteAPI: " + fileUploadResponseItem.toString());
            LOG.debug(loggingPrefix + "[{}] upload URL: {}",
                    fileContainer.getFileMetadata().getName().getValue(),
                    fileUploadResponseItem.getOrDefault(uploadUrlKey, "No upload URL"));

            // Start upload of the file binaries on a separate thread
            URL fileUploadURL = new URL((String) fileUploadResponseItem.get(uploadUrlKey));
            FileBinaryRequestExecutor fileBinaryRequestExecutor = baseFileBinaryRequestExecutor
                    .withMaxRetries(getMaxRetries());

            CompletableFuture<ResponseItems<String>> future;
            if (fileContainer.getFileBinary().getBinaryTypeCase() == FileBinary.BinaryTypeCase.BINARY
                    && fileContainer.getFileBinary().getBinary().isEmpty()) {
                LOG.warn(loggingPrefix + "Binary is empty for file {}. File externalId = [{}]. Will skip upload.",
                        fileContainer.getFileMetadata().getName().getValue(),
                        fileContainer.getFileMetadata().getExternalId().getValue());

                future = CompletableFuture.completedFuture(
                        ResponseItems.of(JsonResponseParser.create(), fileUploadResponse));
            } else {
                future = fileBinaryRequestExecutor.uploadBinaryAsync(fileContainer.getFileBinary(), fileUploadURL)
                        .thenApply(responseBinary -> {
                            long requestDuration = System.currentTimeMillis() - responseBinary.getResponse().sentRequestAtMillis();
                            LOG.info(loggingPrefix + "Upload complete for file [{}], size {}MB in {}s at {}mb/s",
                                    fileContainer.getFileMetadata().getName().getValue(),
                                    String.format("%.2f", fileContainer.getFileBinary().getBinary().size() / (1024d * 1024d)),
                                    String.format("%.2f", requestDuration / 1000d),
                                    String.format("%.2f",
                                            ((fileContainer.getFileBinary().getBinary().size() / (1024d * 1024d)) * 8)
                                                    / (requestDuration / 1000d))
                            );

                            return ResponseItems.of(JsonResponseParser.create(), fileUploadResponse);
                        });
            }

            // Patch the file with extra asset ids.
            if (hasExtraAssetIds) {
                LOG.info(loggingPrefix + "Start patching asset ids.");
                this.patchFileAssetLinks(fileMetadataExtraAssets, fileContainerRequest, loggingPrefix);
            }

            return future;
        }

        /*
         Write extra assets id links as separate updates. The api only supports 1k assetId links per file object
        per api request. If a file contains a large number of assetIds, we need to split them up into an initial
        file create/update (all the code in the writeFilesAsync) and subsequent update requests which add the remaining
        assetIds (this method).
         */
        private void patchFileAssetLinks(FileMetadata file,
                                         RequestParameters originalRequest,
                                         String loggingPrefix) throws Exception {
            // Split the assets into n request items with max 1k assets per request
            FileMetadata tempMeta = FileMetadata.newBuilder()
                    .setExternalId(file.getExternalId())
                    .addAllAssetIds(file.getAssetIdsList())
                    .build();

            List<FileMetadata> metaRequests = new ArrayList<>();
            while (tempMeta.getAssetIdsCount() > 1000) {
                metaRequests.add(tempMeta.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(tempMeta.getAssetIdsList().subList(0,1000))
                        .build());
                tempMeta = tempMeta.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(tempMeta.getAssetIdsList().subList(1000, tempMeta.getAssetIdsCount()))
                        .build();
            }
            metaRequests.add(tempMeta);

            ItemWriter updateWriter = ConnectorServiceV1.create(getMaxRetries(), getAppIdentifier(), getSessionIdentifier())
                     .updateFileHeaders();

            for (FileMetadata metadata : metaRequests) {
                RequestParameters request = RequestParameters.create()
                        .withProjectConfig(originalRequest.getProjectConfig())
                        .withItems(ImmutableList.of(FileParser.toRequestAddAssetIdsItem(metadata)));

                ResponseItems<String> responseItems = updateWriter.writeItems(request);
                if (!responseItems.isSuccessful()) {
                    String message = loggingPrefix + "Failed to add assetIds for file. "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }
                LOG.info(loggingPrefix + "Posted {} assetIds as patch updates to the file.",
                        metadata.getAssetIdsCount());
            }
        }

        @AutoValue.Builder
        abstract static class Builder extends ConnectorBase.Builder<Builder> {
            abstract Builder setAppIdentifier(String value);
            abstract Builder setSessionIdentifier(String value);
            abstract Builder setDeleteTempFile(boolean value);

            abstract FileWriter autoBuild();

            FileWriter build() {
                FileWriter writer = autoBuild();
                Preconditions.checkState(writer.getMaxRetries() <= ConnectorConstants.MAX_MAX_RETRIES
                                && writer.getMaxRetries() >= ConnectorConstants.MIN_MAX_RETRIES
                        , "Max retries out of range. Must be between "
                                + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);

                return writer;
            }
        }
    }

    public static class DownloadFileBinary {
        final static Logger LOG = LoggerFactory.getLogger(DownloadFileBinary.class);
        final static String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        final static String loggingPrefix = "DownloadFileBinary [" + randomIdString + "] -";
        final static OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(90, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.MINUTES)
                .build();

        public static CompletableFuture<FileBinary> downloadFileBinaryFromURL(String downloadUrl) {
            return downloadFileBinaryFromURL(downloadUrl,
                    ConnectorConstants.DEFAULT_MAX_RETRIES,
                    null,
                    false);
        }

        public static CompletableFuture<FileBinary> downloadFileBinaryFromURL(String downloadUrl,
                                                                              URI tempStorageURI) {
            return downloadFileBinaryFromURL(downloadUrl,
                    ConnectorConstants.DEFAULT_MAX_RETRIES,
                    tempStorageURI,
                    false);
        }

        public static CompletableFuture<FileBinary> downloadFileBinaryFromURL(String downloadUrl,
                                                                              URI tempStorageURI,
                                                                              boolean forceTempStorage) {
            return downloadFileBinaryFromURL(downloadUrl,
                    ConnectorConstants.DEFAULT_MAX_RETRIES,
                    tempStorageURI,
                    forceTempStorage);
        }

        public static CompletableFuture<FileBinary> downloadFileBinaryFromURL(String downloadUrl,
                                                                              int maxRetries,
                                                                              @Nullable URI tempStorageURI,
                                                                              boolean forceTempStorage) {
            LOG.debug(loggingPrefix + "Download URL received: {}. Max retries: {}. Temp storage: {}. "
                    + "Force temp storage: {}",
                    downloadUrl,
                    maxRetries,
                    tempStorageURI,
                    forceTempStorage);
            Preconditions.checkArgument(!(null == tempStorageURI && forceTempStorage),
                    "Cannot force the use of temp storage without a valid temp storage URI.");
            HttpUrl url = HttpUrl.parse(downloadUrl);
            Preconditions.checkState(null != url, "Download URL not valid: " + downloadUrl);

            Request FileBinaryBuilder = new Request.Builder()
                    .url(url)
                    .build();

            FileBinaryRequestExecutor requestExecutor = FileBinaryRequestExecutor.of(client)
                    .withMaxRetries(maxRetries)
                    .enableForceTempStorage(forceTempStorage);

            if (null != tempStorageURI) {
                LOG.debug(loggingPrefix + "Temp storage URI detected: {}. "
                        + "Configuring request executor with temp storage.",
                        tempStorageURI);
                requestExecutor = requestExecutor.withTempStoragePath(tempStorageURI);
            }

            long startTimeMillies = Instant.now().toEpochMilli();

            CompletableFuture<FileBinary> future = requestExecutor.downloadBinaryAsync(FileBinaryBuilder)
                    .thenApply(fileBinary -> {
                        long requestDuration = System.currentTimeMillis() - startTimeMillies;
                        long contentLength = fileBinary.getContentLength().getValue();
                        double contentLengthMb = -1;
                        if (contentLength > 0) {
                            contentLengthMb = contentLength / (1024d * 1024d);
                        }
                        double downloadSpeed = -1;
                        if (contentLengthMb > 0) {
                            downloadSpeed = (contentLengthMb * 8) / (requestDuration / 1000d);
                        }
                        LOG.info(loggingPrefix + "Download complete for file, size {}MB in {}s at {}Mb/s",
                                String.format("%.2f", contentLengthMb),
                                String.format("%.2f", requestDuration / 1000d),
                                String.format("%.2f", downloadSpeed)
                        );
                        return fileBinary;
                    });
            return future;
        }
    }
}