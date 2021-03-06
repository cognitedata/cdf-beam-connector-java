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

package com.cognite.beam.io.fn.write;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.dto.Asset;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;

/**
 * Upsert a sorted collection of assets to CDF.clean.
 *
 * This function will first try to insert the items as new assets. In case of conflict, it will retry as an update. The
 * update behavior is specified by the WriterConfig.
 */
public class UpsertAssetFn extends UpsertItemBaseFn<Asset> {
    public UpsertAssetFn(Hints hints,
                         WriterConfig writerConfig,
                         PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, writerConfig, projectConfigView);
    }

    @Override
    protected List<Asset> upsertItems(CogniteClient client, List<Asset> inputItems) throws Exception {
        return client.assets().upsert(inputItems);
    }
}
