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

import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.dto.FileMetadata;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Iterator;
import java.util.List;

/**
 * Lists / reads file (headers) from Cognite Data Fusion
 *
 */
public class ListFilesFn extends ListItemsBaseFn<FileMetadata> {
    public ListFilesFn(Hints hints,
                       ReaderConfig readerConfig,
                       PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, readerConfig, projectConfigView);
    }

    @Override
    protected Iterator<List<FileMetadata>> listItems(CogniteClient client,
                                          RequestParameters requestParameters,
                                          String... partitions) throws Exception {
        return client.files().list(requestParameters, partitions);
    }
}
