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

import java.util.List;

/**
 * Base class for using a CDF Raw state store.
 *
 */
public abstract class RawStateStoreGetHighFn extends RawStateStoreBaseFn<String, KV<String, Long>> {

    public RawStateStoreGetHighFn(Hints hints,
                                  ConfigBase configBase,
                                  String dbName,
                                  String tableName,
                                  PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, configBase, dbName, tableName, projectConfigView);
    }

    /**
     * Process the input.
     *
     * @param stateStore The {@link StateStore} to use for reading and writing the items.
     * @param input The input to process.
     * @return R The result of the processing.
     * @throws Exception
     */
    protected KV<String, Long> apply(StateStore stateStore, String input) throws Exception {
        Long value = null;
        if (stateStore.getHigh(input).isPresent()) {
            value = stateStore.getHigh(input).getAsLong();
        }
        return KV.of(input, value);
    }
}
