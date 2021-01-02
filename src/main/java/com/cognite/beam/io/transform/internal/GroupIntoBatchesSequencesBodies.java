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

import com.cognite.beam.io.dto.SequenceBody;
import com.cognite.beam.io.dto.TimeseriesPointPost;
import com.cognite.beam.io.fn.GroupIntoBatchesDatapointsFn;
import com.cognite.beam.io.fn.GroupIntoBatchesSequencesBodiesFn;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Transform that collects {@link SequenceBody} items into batches.
 *
 * This transform is typically used as a performance optimization component together with
 * a downstream {@link SequenceBody} writer.
 *
 * This transform supports both batch and streaming pipelines. You can configure various parameters
 * that control how items are batched, such as worker parallelization (default is 4)
 * and max latency (default is 5 secs--impacts streaming pipelines).
 *
 * The worker parallelization setting controls the number of parallel requests per worker--not the
 * number of workers. If you set this number too low, the worker will be underutilized, and a too
 * high setting will saturate the worker. The default setting (of 4) is appropriate for most
 * situations.
 */
@AutoValue
public abstract class GroupIntoBatchesSequencesBodies
        extends PTransform<PCollection<KV<String, SequenceBody>>,
                PCollection<KV<String, Iterable<SequenceBody>>>> {
    private static final Duration DEFAULT_MAX_LATENCY = Duration.ofSeconds(5);
    private static final int DEFAULT_WORKER_PARALLELIZATION_FACTOR = 4;

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static GroupIntoBatchesSequencesBodies.Builder builder() {
        return new AutoValue_GroupIntoBatchesSequencesBodies.Builder()
                .setMaxLatency(DEFAULT_MAX_LATENCY)
                .setWorkerParallelizationFactor(DEFAULT_WORKER_PARALLELIZATION_FACTOR);
    }

    /**
     * Returns a {@link GroupIntoBatchesSequencesBodies} transform with a default worker parallelization
     * of 4 and max latency of 5 seconds.
     * @return
     */
    public static GroupIntoBatchesSequencesBodies create() {
        return GroupIntoBatchesSequencesBodies.builder().build();
    }

    abstract Duration getMaxLatency();
    abstract int getWorkerParallelizationFactor();

    public abstract GroupIntoBatchesSequencesBodies.Builder toBuilder();

    /**
     * Sets the maximum latency before emitting a batch. This setting will only have an effect for
     * a streaming pipeline.
     *
     * The default setting is 5 seconds.
     * @param latency
     * @return
     */
    public GroupIntoBatchesSequencesBodies withMaxLatency(Duration latency) {
        return toBuilder().setMaxLatency(latency).build();
    }

    /**
     * Sets the worker parallelization factor.
     *
     * The worker parallelization setting controls the number of parallel requests per worker--not the
     * number of workers. If you set this number too low, the worker will be underutilized, and a too
     * high setting will saturate the worker. The default setting (of 4) is appropriate for most
     * situations.
     * @param parallelization
     * @return
     */
    public GroupIntoBatchesSequencesBodies withWorkerParallelizationFactor(int parallelization) {
        return toBuilder().setWorkerParallelizationFactor(parallelization).build();
    }

    @Override
    public PCollection<KV<String, Iterable<SequenceBody>>> expand(PCollection<KV<String, SequenceBody>> input) {
        LOG.info("Setting up the sequences rows batching component.");
        LOG.info(String.format("Max latency: %s", getMaxLatency()));

        Duration allowedLateness = Duration.ofMillis(input.getWindowingStrategy().getAllowedLateness().getMillis());

        PCollection<KV<String, Iterable<SequenceBody>>> outputCollection = input
                .apply("Group into batches", ParDo.of(
                        new GroupIntoBatchesSequencesBodiesFn(
                                getMaxLatency(), allowedLateness, getWorkerParallelizationFactor()))
                );

        return outputCollection;
    }

    @AutoValue.Builder
    public static abstract class Builder {
        abstract Builder setMaxLatency(Duration value);
        abstract Builder setWorkerParallelizationFactor(int value);

        public abstract GroupIntoBatchesSequencesBodies build();
    }
}
