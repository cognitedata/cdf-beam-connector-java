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

package com.cognite.beam.io.fn.statestore;

import com.cognite.beam.io.config.ConfigBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.client.statestore.StateStore;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;

/**
 * Function for deleting a state from the state store. It takes the state key to delete as input and .
 */
public abstract class RawStateStoreDeleteStateFn extends RawStateStoreBaseFn<Iterable<String>, List<String>> {

    public RawStateStoreDeleteStateFn(Hints hints,
                                      ConfigBase configBase,
                                      String dbName,
                                      String tableName,
                                      PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, configBase, dbName, tableName, projectConfigView);
    }

    /**
     * {@inheritDoc}
     */
    protected List<String> apply(StateStore stateStore, Iterable<String> input) throws Exception {
        List<String> output = new ArrayList<>();
        for (String element : input) {
            if (null == element || element.isBlank()) {
                LOG.warn("Invalid input to the state store. Key and/or value is null or blank. Will skip this input.");
            } else {
                stateStore.deleteState(element);
                output.add(element);
            }
        }
        stateStore.commit();

        return output;
    }
}
