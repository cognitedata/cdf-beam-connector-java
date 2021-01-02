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

package com.cognite.beam.io.transform.internal;

import com.cognite.beam.io.dto.TimeseriesPointPost;
import com.cognite.beam.io.fn.GroupIntoBatchesDatapointsCollectionsFn;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

@AutoValue
public abstract class GroupIntoBatchesDatapointsCollections
        extends PTransform<PCollection<KV<String, Iterable<TimeseriesPointPost>>>,
                PCollection<KV<String, Iterable<TimeseriesPointPost>>>> {
    private static final Duration DEFAULT_MAX_LATENCY = Duration.ofSeconds(5);

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public static GroupIntoBatchesDatapointsCollections.Builder builder() {
        return new AutoValue_GroupIntoBatchesDatapointsCollections.Builder()
                .setMaxLatency(DEFAULT_MAX_LATENCY);
    }

    abstract Duration getMaxLatency();

    public abstract GroupIntoBatchesDatapointsCollections.Builder toBuilder();

    @Override
    public PCollection<KV<String, Iterable<TimeseriesPointPost>>> expand(PCollection<KV<String, Iterable<TimeseriesPointPost>>> input) {
        LOG.info("Setting up the TS datapoints batching component.");
        LOG.info(String.format("Max latency: %s", getMaxLatency()));

        Duration allowedLateness = Duration.ofMillis(input.getWindowingStrategy().getAllowedLateness().getMillis());

        PCollection<KV<String, Iterable<TimeseriesPointPost>>> outputCollection = input
                .apply("Group into batches", ParDo.of(
                        new GroupIntoBatchesDatapointsCollectionsFn(getMaxLatency(), allowedLateness))
                );

        return outputCollection;
    }

    @AutoValue.Builder
    public static abstract class Builder {
        public abstract Builder setMaxLatency(Duration value);

        public abstract GroupIntoBatchesDatapointsCollections build();
    }
}
