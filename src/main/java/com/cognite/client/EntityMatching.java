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

import com.cognite.client.dto.EntityMatchResult;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;


/**
 * This class represents the Cognite entity matching api endpoint
 *
 * It provides methods for interacting with the entity matching services.
 */
@AutoValue
public abstract class EntityMatching extends ApiBase {

    private static Builder builder() {
        return new AutoValue_EntityMatching.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(EntityMatching.class);

    /**
     * Construct a new {@link EntityMatching} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static EntityMatching of(CogniteClient client) {
        return EntityMatching.builder()
                .setClient(client)
                .build();
    }

    public List<EntityMatchResult> predict() throws Exception {

        return Collections.emptyList();
    }


    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract EntityMatching build();
    }
}
