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

package com.cognite.client.servicesV1.request;

import com.cognite.client.servicesV1.ConnectorConstants;
import com.cognite.client.servicesV1.RequestParameters;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Deletes rows from a CFD.Raw table.
 */
@AutoValue
@DefaultCoder(AvroCoder.class)
public abstract class RawDeleteRowsRequestProvider extends GenericRequestProvider{

    public static Builder builder() {
        return new com.cognite.client.servicesV1.request.AutoValue_RawDeleteRowsRequestProvider.Builder()
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract Builder toBuilder();

    public RawDeleteRowsRequestProvider withRequestParameters(RequestParameters parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("dbName")
                && parameters.getRequestParameters().get("dbName") instanceof String,
                "Request parameters must include dbName with a string value");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("tableName")
                        && parameters.getRequestParameters().get("tableName") instanceof String,
                "Request parameters must include tableName");
        return toBuilder().setRequestParameters(parameters).build();
    }

    public Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        // Build path
        urlBuilder.addPathSegment((String) getRequestParameters().getRequestParameters().get("dbName"));
        urlBuilder.addPathSegment("tables");
        urlBuilder.addPathSegment((String) getRequestParameters().getRequestParameters().get("tableName"));
        urlBuilder.addPathSegment("rows");
        urlBuilder.addPathSegment("delete");

        requestBuilder.url(urlBuilder.build());

        // Build a "clean" request parameter object containing only the items from the input.
        RequestParameters requestParameters = RequestParameters.create()
                .withItems(getRequestParameters().getItems());

        String outputJson = requestParameters.getRequestParametersAsJson();
        return requestBuilder.post(RequestBody.Companion.create(outputJson, MediaType.get("application/json"))).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder> {
        public abstract RawDeleteRowsRequestProvider build();
    }
}