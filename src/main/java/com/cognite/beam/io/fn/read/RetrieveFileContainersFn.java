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

package com.cognite.beam.io.fn.read;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.dto.FileBinary;
import com.cognite.client.dto.FileContainer;
import com.cognite.client.dto.Item;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;

/**
 * Retrieves file containers (metadata/header + binary) from Cognite Data Fusion
 *
 */
public class RetrieveFileContainersFn extends RetrieveItemsBaseFn<FileContainer> {
    private final ValueProvider<String> tempStorageURI;
    private final boolean forceTempStorage;

    public RetrieveFileContainersFn(Hints hints,
                                    ReaderConfig readerConfig,
                                    @Nullable ValueProvider<String> tempStorageURI,
                                    boolean forceTempStorage,
                                    PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, readerConfig, projectConfigView);
        this.tempStorageURI = tempStorageURI;
        this.forceTempStorage = forceTempStorage;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up RetrieveFileContainersFn. Temp storage URI: {}, Force temp storage: {}",
                null != tempStorageURI ? tempStorageURI.get() : "null",
                forceTempStorage);
    }

    @Override
    protected List<FileContainer> retrieveItems(CogniteClient client,
                                             List<Item> items) throws Exception {
        URI tempStorage = null;
        if (null != tempStorageURI && tempStorageURI.isAccessible()) {
            tempStorage = new URI(tempStorageURI.get());
        }

        return client.files().download(items, tempStorage, forceTempStorage);
    }
}
