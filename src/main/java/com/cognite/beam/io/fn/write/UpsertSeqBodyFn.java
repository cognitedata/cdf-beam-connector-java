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
import com.cognite.client.dto.*;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.*;

/**
 * Writes sequences body (rows and columns) to CDF.Clean.
 *
 * This function writes sequences rows to Cognite. In case the rows exists from before (based on the externalId/id
 * and row number), the existing value will be overwritten.
 *
 * In case the sequence metadata / header does not exists, this module will create a minimum header.
 * The created header depends on the columns information included in the sequences body object.
 *
 * In any case, we recommend that you create the sequence header before starting to write sequence rows to it.
 */
public class UpsertSeqBodyFn extends UpsertItemBaseNewFn<SequenceBody> {
    final Distribution seqRowsBatch = Metrics.distribution("cognite", "sequenceRowsPerBatch");

    public UpsertSeqBodyFn(Hints hints,
                         WriterConfig writerConfig,
                         PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, writerConfig, projectConfigView);
    }

    @Override
    protected List<SequenceBody> upsertItems(CogniteClient client, List<SequenceBody> inputItems) throws Exception {
        List<SequenceBody> results = client.sequences().rows().upsert(inputItems);

        if (writerConfig.isMetricsEnabled()) {
            int countRows = results.stream()
                    .mapToInt(body -> body.getRowsCount())
                    .sum();

            seqRowsBatch.update(countRows);
        }

        return results;
    }
}
