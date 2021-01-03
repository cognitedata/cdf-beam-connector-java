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
import com.cognite.beam.io.RequestParameters;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

@AutoValue
public abstract class PostPlaygroundJsonListRequestProvider extends GenericPlaygroundRequestProvider {

    public static Builder builder() {
        return new AutoValue_PostPlaygroundJsonListRequestProvider.Builder()
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setEndpoint(ConnectorConstants.DEFAULT_ENDPOINT);
    }

    public abstract Builder toBuilder();

    public PostPlaygroundJsonListRequestProvider withRequestParameters(RequestParameters parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");
        return toBuilder().setRequestParameters(parameters).build();
    }

    public Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        RequestParameters requestParameters = getRequestParameters();
        Request.Builder requestBuilder = buildGenericRequest();

        // Check for limit
        if (!requestParameters.getRequestParameters().containsKey("limit")) {
            requestParameters = requestParameters.withRootParameter("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE);
        }

        if (cursor.isPresent()) {
            requestParameters = requestParameters.withRootParameter("cursor", cursor.get());
        }

        String outputJson = requestParameters.getRequestParametersAsJson();
        return requestBuilder.post(RequestBody.Companion.create(outputJson, MediaType.get("application/json"))).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericPlaygroundRequestProvider.Builder<Builder>{
        public abstract PostPlaygroundJsonListRequestProvider build();
    }
}
