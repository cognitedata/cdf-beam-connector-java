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
import com.cognite.client.dto.FileMetadata;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;

/**
 * Writes file headers to CDF.Clean.
 *
 * This function will first try to write the file headers as updates. In case the headers don't exist in CDF,
 * they will be created via the files upload endpoint. Effectively this results in an upsert.
 *
 */
public class UpsertFileHeaderFn extends UpsertItemBaseFn<FileMetadata> {
    public UpsertFileHeaderFn(Hints hints,
                         WriterConfig writerConfig,
                         PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, writerConfig, projectConfigView);
    }

    @Override
    protected List<FileMetadata> upsertItems(CogniteClient client, List<FileMetadata> inputItems) throws Exception {
        return client.files().upsert(inputItems);
    }
}
