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
import com.cognite.client.dto.Asset;
import com.cognite.client.dto.Item;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Lists / reads assets from Cognite Data Fusion
 *
 */
public class RetrieveAssetsFn extends RetrieveItemsBaseFn<Asset> {

    public RetrieveAssetsFn(Hints hints,
                            ReaderConfig readerConfig,
                            PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, readerConfig, projectConfigView);
    }

    @Override
    protected List<Asset> retrieveItems(CogniteClient client,
                                             List<Item> items) throws Exception {
        return client.assets().retrieve(items);
    }
}
