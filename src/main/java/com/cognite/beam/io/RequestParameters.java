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
import com.cognite.client.Request;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

import static com.google.common.base.Preconditions.*;

/**
 * This class represents the query / request parameters. The available parameters depend on which resource type
 * (assets, events, files, time series, etc.) you are requesting.
 *
 * The parameters mirrors what's available in the api. For example, which filters you can use.
 */
@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class RequestParameters implements Serializable {

    private static Builder builder() {
        return new AutoValue_RequestParameters.Builder()
                .setRequest(Request.create()
                        .withProtoRequestBody(Struct.newBuilder().build()))
                .setProjectConfig(ProjectConfig.create());
    }

    public static RequestParameters create() {
        return RequestParameters.builder().build();
    }

    /**
     * Returns the core {@link Request} object encapsulating the key parameters and body for the Cognite API
     * request.
     *
     * @return The core request parameters and body.
     */
    public abstract Request getRequest();

    /**
     * Returns the project configuration for a request. The configuration includes host (optional),
     * project/tenant and key.
     *
     * @return
     */
    public abstract ProjectConfig getProjectConfig();

    abstract Builder toBuilder();

    /**
     * Returns the object representation of the composite request body. It is similar to the Cognite API Json
     * request body, with {@code Map<String, Object>} as the Json container, {@code List} as the
     * Json array.
     *
     * @return
     */
    public ImmutableMap<String, Object> getRequestParameters() {
        return getRequest().getRequestParameters();
    }

    /**
     * For internal use only.
     *
     * Returns the protobuf request body.
     * @return
     */
    @Nullable
    public Message getProtoRequestBody() {
        return getRequest().getProtoRequestBody();
    }

    public String getRequestParametersAsJson() throws JsonProcessingException {
        return getRequest().getRequestParametersAsJson();
    }

    /**
     * Returns the collection of filter parameters as a Map. The filter parameter must have a key of type
     * String in order to be included in the return collection.
     *
     * Please note that all entries (with a key of type {@code String}) under the filter node are returned, potentially
     * including nested collections (such as metadata).
     *
     * @return
     */
    public ImmutableMap<String, Object> getFilterParameters() {
        return getRequest().getFilterParameters();
    }

    /**
     * Returns the collection of metadata specific filter parameters as a Map. The parameters must have both a key and
     * value of type String in order to be included in the return collection.
     *
     * @return
     */
    public ImmutableMap<String, String> getMetadataFilterParameters() {
        return getRequest().getMetadataFilterParameters();
    }

    /**
     * Returns the list of items. This is typically the main payload of a write request (create, update or delete).
     *
     * @return
     */
    public ImmutableList<ImmutableMap<String, Object>> getItems() {
        return getRequest().getItems();
    }

    /**
     * Adds the complete request parameter structure based on Java objects. Calling this method will overwrite
     * any previously added parameters.
     *
     * - All keys must be String.
     * - Values can be primitives or containers
     * - Valid primitives are String, Integer, Double, Float, Long, Boolean.
     * - Valid containers are Map (Json Object) and List (Json array).
     *
     * @param requestParameters
     * @return
     */
    public RequestParameters withRequestParameters(Map<String, Object> requestParameters) {
        return toBuilder()
                .setRequest(getRequest().withRequestParameters(requestParameters))
                .build();
    }

    /**
     * Adds the complete request body as a protobuf object.
     *
     * @param requestBody
     * @return
     */
    public RequestParameters withProtoRequestBody(Message requestBody) {
        return toBuilder()
                .setRequest(getRequest().withProtoRequestBody(requestBody))
                .build();
    }

    /**
     * Adds the complete request parameter structure based on Json. Calling this method will overwrite
     * any previously added parameters.
     *
     * @param json
     * @return
     */
    public RequestParameters withRequestJson(String json) throws Exception {
        return toBuilder()
                .setRequest(getRequest().withRequestJson(json))
                .build();
    }

    /**
     * Adds a new parameter to the root level.
     *
     * @param key
     * @param value
     * @return
     */
    public RequestParameters withRootParameter(String key, Object value) {
        return toBuilder()
                .setRequest(getRequest().withRootParameter(key, value))
                .build();
    }

    /**
     * Adds a new parameter to the filter node.
     *
     * @param key
     * @param value
     * @return
     */
    public RequestParameters withFilterParameter(String key, Object value) {
        return toBuilder()
                .setRequest(getRequest().withFilterParameter(key, value))
                .build();
    }

    /**
     * Adds a new parameter to the filter.metadata node.
     *
     * @param key
     * @param value
     * @return
     */
    public RequestParameters withFilterMetadataParameter(String key, String value) {
        return toBuilder()
                .setRequest(getRequest().withFilterMetadataParameter(key, value))
                .build();
    }

    /**
     * Sets the items array to the specified input list. The list represents an array of objects via
     * Java Map. That is, an instance of Map equals a Json object.
     *
     * For most write operations the maximum number of items per request is 1k. For time series datapoints
     * the maximum number of items is 10k, with a maximum of 100k data points.
     *
     * @param items
     * @return
     */
    public RequestParameters withItems(List<? extends Map<String, Object>> items) {
        return toBuilder()
                .setRequest(getRequest().withItems(items))
                .build();
    }

    /**
     * Convenience method for setting the external id for requesting an item.
     *
     * You can use this method when requesting a data item by id, for example when requesting the data points
     * from a time series.
     *
     * @param externalId
     * @return
     */
    public RequestParameters withItemExternalId(String externalId) {
        return toBuilder()
                .setRequest(getRequest().withItemExternalId(externalId))
                .build();
    }

    /**
     * Convenience method for setting the external id for requesting an item.
     *
     * You can use this method when requesting a data item by id, for example when requesting the data points
     * from a time series.
     *
     * @param internalId
     * @return
     */
    public RequestParameters withItemInternalId(long internalId) {
        return toBuilder()
                .setRequest(getRequest().withItemInternalId(internalId))
                .build();
    }

    /**
     * Convenience method for adding the database name when reading from Cognite.Raw.
     *
     * @param dbName The name of the database to read from.
     * @return
     */
    public RequestParameters withDbName(String dbName) {
        return toBuilder()
                .setRequest(getRequest().withDbName(dbName))
                .build();
    }

    /**
     * Convenience method for adding the table name when reading from Cognite.Raw.
     *
     * @param tableName The name of the database to read from.
     * @return
     */
    public RequestParameters withTableName(String tableName) {
        return toBuilder()
                .setRequest(getRequest().withTableName(tableName))
                .build();
    }

    /**
     * Sets the project configuration for a request. The configuration includes host (optional),
     * project/tenant and key.
     *
     * You can use this method if you need detailed, per-request control over project config. In most cases, however,
     * you can set this once on the reader / writer via parameters or a config file.
     *
     * @param config
     * @return
     */
    public RequestParameters withProjectConfig(ProjectConfig config) {
        return toBuilder().setProjectConfig(config).build();
    }

    public RequestParameters withRequest(Request request) {
        return toBuilder().setRequest(request).build();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setProjectConfig(ProjectConfig value);
        abstract Builder setRequest(Request value);

        public abstract RequestParameters build();
    }
}
