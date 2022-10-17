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
 * Function for expanding the high watermark state to a RAW state store. It takes a {@code KV<String, Long>} representing
 * the key and high watermark value as input and outputs the same element after it has been committed to the
 * state store.
 *
 * Expand high will only set a new value it the supplied value is higher than the current state for the key.
 */
public abstract class RawStateStoreExpandHighFn extends
        RawStateStoreBaseFn<Iterable<KV<String, Long>>, List<KV<String, Long>>> {

    public RawStateStoreExpandHighFn(Hints hints,
                                     ConfigBase configBase,
                                     String dbName,
                                     String tableName,
                                     PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, configBase, dbName, tableName, projectConfigView);
    }

    /**
     * {@inheritDoc}
     */
    protected List<KV<String, Long>> apply(StateStore stateStore, Iterable<KV<String, Long>> input) throws Exception {
        List<KV<String, Long>> output = new ArrayList<>();
        for (KV<String, Long> element : input) {
            if (null == element.getKey() || null == element.getValue()) {
                LOG.warn("Invalid input to the state store. Key and/or value is null {}. Will skip this input.",
                        element.toString());
            } else {
                stateStore.expandHigh(element.getKey(), element.getValue());
                output.add(element);
            }
        }
        stateStore.commit();

        return output;
    }
}
