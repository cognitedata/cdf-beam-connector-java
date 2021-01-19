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

package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ResourceType;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.TimeseriesParser;
import com.cognite.v1.timeseries.proto.*;
import com.google.auto.value.AutoValue;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite timeseries api endpoint.
 *
 * It provides methods for reading and writing {@link TimeseriesMetadata}.
 */
@AutoValue
public abstract class DataPoints extends ApiBase {

    private static Builder builder() {
        return new AutoValue_DataPoints.Builder();
    }

    /**
     * Construct a new {@link DataPoints} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static DataPoints of(CogniteClient client) {
        return DataPoints.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link TimeseriesPoint} object that matches the filters set in the {@link RequestParameters}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The timeseries are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving timeseries.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<TimeseriesPoint>> list(RequestParameters requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link TimeseriesPoint} objects that matches the filters set in the {@link RequestParameters} for
     * the specified partitions. This method is intended for advanced use cases where you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the timeseries.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<TimeseriesPoint>> list(RequestParameters requestParameters, String... partitions) throws Exception {
        // todo: implement
        return new Iterator<List<TimeseriesPoint>>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public List<TimeseriesPoint> next() {
                return null;
            }
        };
    }

    /**
     * Retrieves timeseries by id.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved timeseries.
     * @throws Exception
     */
    public List<TimeseriesPoint> retrieve(List<Item> items) throws Exception {
        // todo: implement
        return Collections.emptyList();
    }

    /**
     * Creates or update a set of {@link TimeseriesPoint} objects.
     *
     * If it is a new {@link TimeseriesPoint} object (based on the {@code id / externalId}, then it will be created.
     *
     * If an {@link TimeseriesPoint} object already exists in Cognite Data Fusion, it will be updated. The update
     * behaviour is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param timeseries The timeseries to upsert
     * @return The upserted timeseries
     * @throws Exception
     */
    public List<TimeseriesPoint> upsert(List<TimeseriesPoint> timeseries) throws Exception {
        // todo: implement
        return Collections.emptyList();
    }

    public List<Item> delete(List<Item> timeseries) throws Exception {
        // todo: implement

        return Collections.emptyList();
    }

    /**
     * Builds a proto request object for upserting a collection of time series data points.
     *
     * @param externalIdInsertMap Data points linked to an external id.
     * @param internalIdInsertMap Data points linked to an internal id.
     * @return The proto request object.
     * @throws Exception
     */
    private DataPointInsertionRequest toRequestProto(Map<String, Map<Long, TimeseriesPointPost>> externalIdInsertMap,
                                                     Map<Long, Map<Long, TimeseriesPointPost>> internalIdInsertMap) {
        DataPointInsertionRequest.Builder requestBuilder = DataPointInsertionRequest.newBuilder();
        for (Map.Entry<String, Map<Long, TimeseriesPointPost>> element : externalIdInsertMap.entrySet()) {
            requestBuilder.addItems(this.toRequestProtoItem(element.getValue().values()));
        }
        for (Map.Entry<Long, Map<Long, TimeseriesPointPost>> element : internalIdInsertMap.entrySet()) {
            requestBuilder.addItems(this.toRequestProtoItem(element.getValue().values()));
        }

        return requestBuilder.build();
    }

    /**
     * Convert a collection of time series point post object to a Cognite API request proto object.
     * All data points in the input collection must belong to the same time series (externalId / id).
     *
     * @param elements The time series point to build insert object for.
     * @return The proto insert object.
     */
    private DataPointInsertionItem toRequestProtoItem(Collection<TimeseriesPointPost> elements) {
        TimeseriesPointPost[] points = elements.toArray(new TimeseriesPointPost[0]);
        DataPointInsertionItem.Builder itemBuilder = DataPointInsertionItem.newBuilder();

        // set ids, identify points type
        if (points[0].getIdTypeCase() == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID) {
            itemBuilder.setExternalId(points[0].getExternalId());
        } else {
            itemBuilder.setId(points[0].getId());
        }

        if(points[0].getValueTypeCase() == TimeseriesPointPost.ValueTypeCase.VALUE_NUM) {
            NumericDatapoints.Builder numPointsBuilder = NumericDatapoints.newBuilder();
            for (TimeseriesPointPost point : points) {
                numPointsBuilder.addDatapoints(NumericDatapoint.newBuilder()
                        .setTimestamp(point.getTimestamp())
                        .setValue(point.getValueNum())
                        .build());
            }
            itemBuilder.setNumericDatapoints(numPointsBuilder.build());
        } else {
            StringDatapoints.Builder stringPointsBuilder = StringDatapoints.newBuilder();
            for (TimeseriesPointPost point : points) {
                stringPointsBuilder.addDatapoints(StringDatapoint.newBuilder()
                        .setTimestamp(point.getTimestamp())
                        .setValue(point.getValueString())
                        .build());
            }
            itemBuilder.setStringDatapoints(stringPointsBuilder.build());
        }
        return itemBuilder.build();
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private TimeseriesMetadata parseTimeseries(String json) {
        try {
            return TimeseriesParser.parseTimeseriesMetadata(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(TimeseriesMetadata item) {
        try {
            return TimeseriesParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(TimeseriesMetadata item) {
        try {
            return TimeseriesParser.toRequestUpdateItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(TimeseriesMetadata item) {
        try {
            return TimeseriesParser.toRequestReplaceItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an event. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getTimeseriesId(TimeseriesMetadata item) {
        if (item.hasExternalId()) {
          return Optional.of(item.getExternalId().getValue());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId().getValue()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract DataPoints build();
    }
}
