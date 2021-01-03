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
import com.cognite.v1.timeseries.proto.DataPointInsertionRequest;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

@AutoValue
public abstract class TSPointsWriteProtoRequestProvider extends GenericRequestProvider {
    private static final String logMessagePrefix = "Build data points write request - ";

    public static Builder builder() {
        return new com.cognite.client.servicesV1.request.AutoValue_TSPointsWriteProtoRequestProvider.Builder()
                .setRequestParameters(RequestParameters.create())
                .setSdkIdentifier(ConnectorConstants.SDK_IDENTIFIER)
                .setAppIdentifier(ConnectorConstants.DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(ConnectorConstants.DEFAULT_SESSION_IDENTIFIER)
                .setBetaEnabled(ConnectorConstants.DEFAULT_BETA_ENABLED);
    }

    public abstract Builder toBuilder();

    public TSPointsWriteProtoRequestProvider withRequestParameters(RequestParameters parameters) {
        Preconditions.checkNotNull(parameters, "Request parameters cannot be null.");

        return toBuilder().setRequestParameters(parameters).build();
    }

    public Request buildRequest(Optional<String> cursor) throws IOException, URISyntaxException {
        Preconditions.checkNotNull(getRequestParameters().getProtoRequestBody(),
                "No protobuf request body found.");
        Preconditions.checkArgument(getRequestParameters().getProtoRequestBody() instanceof DataPointInsertionRequest,
                "The protobuf request body is not of type DataPointInsertionRequest");
        DataPointInsertionRequest requestPayload = (DataPointInsertionRequest) getRequestParameters().getProtoRequestBody();
        LOG.info(logMessagePrefix + "Building write request for {} time series",
                requestPayload.getItemsCount());
        Request.Builder requestBuilder = buildGenericRequest();

        return requestBuilder.post(RequestBody.Companion.create(requestPayload.toByteArray(),
                MediaType.get("application/protobuf"))).build();
    }

    @AutoValue.Builder
    public static abstract class Builder extends GenericRequestProvider.Builder<Builder>{
        public abstract TSPointsWriteProtoRequestProvider build();
    }
}
