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
import okhttp3.Request;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

@AutoValue
@DefaultCoder(AvroCoder.class)
public abstract class GetLoginRequestProvider extends GenericRequestProvider{
    private static final String DEFAULT_API_ENDPOINT = "status";

    public static Builder builder() {
        return new AutoValue_GetLoginRequestProvider.Builder()
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setEndpoint(DEFAULT_API_ENDPOINT)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract Builder toBuilder();

    public GetLoginRequestProvider withRequestParameters(RequestParameters parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        return toBuilder().setRequestParameters(parameters).build();
    }

    public Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Request.Builder requestBuilder = buildGenericRequest();
        //HttpUrl.Builder urlBuilder = buildGenericUrl();

        //return requestBuilder.url(urlBuilder.build()).build();
        return requestBuilder.build();
    }

    /*
    Overloaded to build the request without checking for valid project parameters.
     */
    protected Request.Builder buildGenericRequest() throws URISyntaxException {
        Preconditions.checkState(this.getAppIdentifier().length() < 40
                , "App identifier out of range. Length must be < 40.");
        Preconditions.checkState(this.getSdkIdentifier().length() < 40
                , "SDK identifier out of range. Length must be < 40.");
        Preconditions.checkState(this.getSessionIdentifier().length() < 40
                , "Session identifier out of range. Length must be < 40.");

        // build standard part of the request.
        return new Request.Builder()
                .header("Accept", "application/json")
                .header("api-key", this.getRequestParameters().getProjectConfig().getApiKey().get())
                .header("x-cdp-sdk", this.getSdkIdentifier())
                .header("x-cdp-app", this.getAppIdentifier())
                .header("x-cdp-clienttag", this.getSessionIdentifier())
                .url(buildGenericUrl().build());
    }

    /*
    Overloaded to build a URL specific to the login service.
     */
    protected HttpUrl.Builder buildGenericUrl() throws URISyntaxException {
        URI uri = null;
        uri = new URI(this.getRequestParameters().getProjectConfig().getHost().get());
        return new HttpUrl.Builder()
                .scheme(uri.getScheme())
                .host(uri.getHost())
                .addPathSegment("login")
                .addPathSegments(this.getEndpoint());
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder> {
        public abstract GetLoginRequestProvider build();
    }
}
