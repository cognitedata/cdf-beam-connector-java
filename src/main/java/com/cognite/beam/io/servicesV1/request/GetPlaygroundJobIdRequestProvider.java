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
import okhttp3.HttpUrl;
import okhttp3.Request;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Builds request to get results from an async api job based on a jobId.
 *
 * Used by various context api services as most context services are based on an async api pattern.
 *
 * Job id is specified via the {@link RequestParameters}.
 */
@AutoValue
@DefaultCoder(AvroCoder.class)
public abstract class GetPlaygroundJobIdRequestProvider extends GenericPlaygroundRequestProvider{

    static Builder builder() {
        return new AutoValue_GetPlaygroundJobIdRequestProvider.Builder()
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER);
    }

    /**
     * Returns a request provider that will get job results from a jobId endpoint.
     *
     * @return
     */
    public static GetPlaygroundJobIdRequestProvider of(String endpoint) {
        return GetPlaygroundJobIdRequestProvider.builder()
                .setEndpoint(endpoint)
                .build();
    }

    public abstract Builder toBuilder();

    public GetPlaygroundJobIdRequestProvider withRequestParameters(RequestParameters parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        Preconditions.checkArgument(parameters.getRequestParameters().containsKey("jobId")
                && (parameters.getRequestParameters().get("jobId") instanceof Integer
                        || parameters.getRequestParameters().get("jobId") instanceof Long),
                "Request parameters must include jobId with an int/long value");
        return toBuilder().setRequestParameters(parameters).build();
    }

    public Request buildRequest(Optional<String> cursor) throws URISyntaxException {
        RequestParameters requestParameters = getRequestParameters();
        Request.Builder requestBuilder = buildGenericRequest();
        HttpUrl.Builder urlBuilder = buildGenericUrl();

        // Build path
        urlBuilder
                .addPathSegment(String.valueOf(requestParameters.getRequestParameters().get("jobId")));

        requestBuilder.url(urlBuilder.build());

        return requestBuilder.url(urlBuilder.build()).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericPlaygroundRequestProvider.Builder<Builder> {
        public abstract GetPlaygroundJobIdRequestProvider build();
    }
}