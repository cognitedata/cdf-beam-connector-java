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

package com.cognite.beam.io.servicesV1.request;

import com.cognite.beam.io.servicesV1.ConnectorConstants;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import okhttp3.HttpUrl;
import okhttp3.Request;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

@AutoValue
@DefaultCoder(AvroCoder.class)
public abstract class GetSimpleListRequestProvider extends GenericRequestProvider{

    public static Builder builder() {
        return new com.cognite.beam.io.servicesV1.request.AutoValue_GetSimpleListRequestProvider.Builder()
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setEndpoint(ConnectorConstants.DEFAULT_ENDPOINT)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract Builder toBuilder();

    public GetSimpleListRequestProvider withRequestParameters(RequestParameters parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        return toBuilder().setRequestParameters(parameters).build();
    }

    public Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        RequestParameters requestParameters = getRequestParameters();
        Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();
        ImmutableList<String> rootParameters = ImmutableList.of("limit", "cursor", "divisions");
        ImmutableList<Class> validClasses = ImmutableList.of(String.class, Integer.class, Long.class,
                Float.class, Double.class, Boolean.class);

        // Check for limit
        if (!requestParameters.getRequestParameters().containsKey("limit")) {
            requestParameters = requestParameters.withRootParameter("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE);
        }

        if (cursor.isPresent()) {
            requestParameters = requestParameters.withRootParameter("cursor", cursor.get());
        }

        // add the root parameters.
        requestParameters.getRequestParameters().entrySet().stream()
                .filter(entry -> rootParameters.contains(entry.getKey())
                        && validClasses.contains(entry.getValue().getClass()))
                .forEach(entry -> urlBuilder.addQueryParameter(entry.getKey(), String.valueOf(entry.getValue())));

        // add filter parameters.
        requestParameters.getFilterParameters().entrySet().stream()
                .filter(entry -> validClasses.contains(entry.getValue().getClass()))
                .forEach(entry -> urlBuilder.addQueryParameter(entry.getKey(), String.valueOf(entry.getValue())));

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder> {
        public abstract GetSimpleListRequestProvider build();
    }
}
