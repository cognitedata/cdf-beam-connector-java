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
import com.cognite.client.dto.RawRow;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes raw rows to CDF.Raw.
 *
 * This function writes rows to a raw table. In case the row exists from before (based on the row key), the
 * existing row will be overwritten.
 *
 * The input collection of rows must all belong to the same db and table. That is, each {@code Iterable<RawRow>}
 * must contain rows for the same raw destination table.
 *
 */
public class UpsertRawRowFn extends UpsertItemBaseFn<RawRow> {
    public UpsertRawRowFn(Hints hints,
                          WriterConfig writerConfig,
                          PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, writerConfig, projectConfigView);
    }

    @Override
    protected List<RawRow> upsertItems(CogniteClient client, List<RawRow> inputItems) throws Exception {
        try {
            return client.raw().rows().upsert(inputItems, true);
        } catch (Exception e) {
            LOG.warn("Failed to write RAW rows {}", inputItems.stream()
                    .map(row -> row.getDbName() + "|" + row.getTableName())
                    .distinct()
                    .collect(Collectors.joining(","))
            );
            throw e;
        }
    }
}
