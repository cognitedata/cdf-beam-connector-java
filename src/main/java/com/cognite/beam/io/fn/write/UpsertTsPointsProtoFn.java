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
import com.cognite.client.dto.TimeseriesPointPost;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.*;

/**
 * Writes time series data points to CDF.Clean.
 *
 * This function writes TS data points to Cognite. In case the datapoint exists from before (based on the externalId/id
 * and timestamp), the existing value will be overwritten.
 *
 * In case the TS metadata / header does not exists, this module will create a minimum header. The created header supports
 * both numeric and string values, step and non-step time series. In addition, only externalId
 * is supported. That is, if you try to post TS datapoints to an internalId that does not exist the write will fail.
 *
 * In any case, we recommend that you create the TS header before starting to write TS data points to it.
 *
 */
public class UpsertTsPointsProtoFn extends UpsertItemBaseFn<TimeseriesPointPost> {
    final Distribution tsBatch = Metrics.distribution("cognite", "TsPerBatch");

    public UpsertTsPointsProtoFn(Hints hints,
                                 WriterConfig writerConfig,
                                 PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, writerConfig, projectConfigView);
    }

    @Override
    protected List<TimeseriesPointPost> upsertItems(CogniteClient client,
                                                    List<TimeseriesPointPost> inputItems) throws Exception {
        List<TimeseriesPointPost> results = client.timeseries().dataPoints().upsert(inputItems);

        if (writerConfig.isMetricsEnabled()) {
            long countTs = results.stream()
                    .map(point -> point.getExternalId())
                    .distinct()
                    .count();

            tsBatch.update(countTs);
        }

        return results;
    }
}
