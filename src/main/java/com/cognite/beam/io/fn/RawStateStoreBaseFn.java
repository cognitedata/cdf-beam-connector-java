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

package com.cognite.beam.io.fn;

import com.cognite.beam.io.config.ConfigBase;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.client.statestore.RawStateStore;
import com.cognite.client.statestore.StateStore;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.RandomStringUtils;
import java.util.List;

/**
 * Base class for using a CDF Raw state store.
 *
 */
public abstract class RawStateStoreBaseFn<T, R> extends IOBaseFn<T, R> {
    final ConfigBase configBase;
    final String dbName;
    final String tableName;
    final PCollectionView<List<ProjectConfig>> projectConfigView;

    private transient RawStateStore stateStore = null; // Mark transient to prevent serialization

    public RawStateStoreBaseFn(Hints hints,
                               ConfigBase configBase,
                               String dbName,
                               String tableName,
                               PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints);
        Preconditions.checkArgument(null != dbName && !dbName.isBlank(),
                "DB name must be a valid string.");
        Preconditions.checkArgument(null != tableName && !tableName.isBlank(),
                "Table name must be a valid string.");

        this.configBase = configBase;
        this.projectConfigView = projectConfigView;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    @Setup
    public void setup() {
    }

    @ProcessElement
    public void processElement(@Element T element,
                               OutputReceiver<R> outputReceiver,
                               ProcessContext context) throws Exception {
        final String batchLogPrefix = "Batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";

        // Identify the project config to use
        ProjectConfig projectConfig;
        if (context.sideInput(projectConfigView).size() > 0) {
            projectConfig = context.sideInput(projectConfigView).get(0);
        } else {
            String message = batchLogPrefix + "Cannot identify project config. Empty side input.";
            LOG.error(message);
            throw new Exception(message);
        }

        // Perform the state store processing step
        try {
            R result = apply(getRawStateStore(projectConfig, configBase, dbName, tableName), element);
            outputReceiver.output(result);

            LOG.debug(batchLogPrefix + "Input {} with result{}.",
                    element,
                    result);
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error when reading from Cognite Data Fusion: {}",
                    e.toString());
            throw new Exception(batchLogPrefix + "Error when reading from Cognite Data Fusion.", e);
        }
    }

    /**
     * Returns the {@link RawStateStore}. Will instantiate a new store and cache it.
     *
     * @param projectConfig the {@link ProjectConfig} to configure auth credentials.
     * @param configBase Carries the app and session identifiers.
     * @param dbName The CDF.Raw db hosting the state store.
     * @param tableName The CDF.Raw table hosting the state store.
     * @return The state store.
     * @throws Exception
     */
    protected RawStateStore getRawStateStore(ProjectConfig projectConfig,
                                             ConfigBase configBase,
                                             String dbName,
                                             String tableName) throws Exception {
        if (null == stateStore) {
            // State store is not configured. Either because this class has been serialized or it is the first method call
            stateStore = RawStateStore.of(getClient(projectConfig, configBase), dbName, tableName);
            stateStore.load();
        }

        return stateStore;
    }

    /**
     * Process the input.
     *
     * @param stateStore The {@link StateStore} to use for reading and writing the items.
     * @param input The input to process.
     * @return R The result of the processing.
     * @throws Exception
     */
    protected abstract R apply(StateStore stateStore, T input) throws Exception;
}
