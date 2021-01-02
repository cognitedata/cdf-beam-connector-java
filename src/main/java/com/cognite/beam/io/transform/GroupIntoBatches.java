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

package com.cognite.beam.io.transform;

import com.cognite.beam.io.fn.GroupIntoBatchesFn;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Transform that collects the input into batches of a configurable size.
 *
 * This transform is typically used as a performance optimization component together with
 * a downstream I/O transform. For example, by gathering a set of items into a batch before
 * performing I/O on the entire batch vs. single items.
 *
 * This transform supports both batch and streaming pipelines. You can configure various parameters
 * that control how items are batched, such as max batch size (default is 1k)
 * and max latency (default is 5 secs--impacts streaming pipelines).
 *
 * @param <K>
 * @param <InputT>
 */
@AutoValue
public abstract class GroupIntoBatches<K, InputT>
        extends PTransform<PCollection<KV<K,InputT>>, PCollection<KV<K, Iterable<InputT>>>> {
    private static final int DEFAULT_MAX_BATCH_SIZE = 1000;
    private static final Duration DEFAULT_MAX_LATENCY = Duration.ofSeconds(5);

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    /**
     * Returns a {@link GroupIntoBatches} transform with a default max batch size of 1k
     * and max latency of 5 seconds.
     *
     * @param <K>
     * @param <InputT>
     * @return
     */
    private static <K, InputT> GroupIntoBatches.Builder<K, InputT> builder() {
        return new com.cognite.beam.io.transform.AutoValue_GroupIntoBatches.Builder<K, InputT>()
                .setMaxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .setMaxLatency(DEFAULT_MAX_LATENCY);
    }

    public static <K, InputT> GroupIntoBatches<K, InputT> of(KvCoder kvCoder) {
        return GroupIntoBatches.<K, InputT>builder()
                .setKvCoder(kvCoder)
                .build();
    }

    abstract int getMaxBatchSize();
    abstract Duration getMaxLatency();
    abstract KvCoder getKvCoder();

    /**
     * Sets the maximum latency before emitting a batch. This setting will only have an effect for
     * a streaming pipeline.
     *
     * The default setting is 5 seconds.
     * @param latency
     * @return
     */
    public GroupIntoBatches<K, InputT> withMaxLatency(Duration latency) {
        Preconditions.checkNotNull(latency, "Latency cannot be null");
        return toBuilder().setMaxLatency(latency).build();
    }

    /**
     * Sets the maximum batch size of the output. The default is 1000.
     *
     * @param batchSize
     * @return
     */
    public GroupIntoBatches<K, InputT> withMaxBatchSize(int batchSize) {
        Preconditions.checkNotNull(batchSize, "Latency cannot be null");
        return toBuilder().setMaxBatchSize(batchSize).build();
    }

    public abstract GroupIntoBatches.Builder<K, InputT> toBuilder();

    @Override
    public PCollection<KV<K, Iterable<InputT>>> expand(PCollection<KV<K, InputT>> input) {
        LOG.info("Setting up the batching component.");
        LOG.info(String.format("Max batch size: %s%nMax latency: %s", getMaxBatchSize(), getMaxLatency()));

        Duration allowedLateness = Duration.ofMillis(input.getWindowingStrategy().getAllowedLateness().getMillis());

        PCollection<KV<K, Iterable<InputT>>> outputCollection = input
                .apply("Group into batches", ParDo.of(new GroupIntoBatchesFn<K, InputT>(getMaxBatchSize(),
                        getMaxLatency(), allowedLateness, getKvCoder()))
                );

        return outputCollection;
    }

    @AutoValue.Builder
    public static abstract class Builder<K, InputT> {
        public abstract Builder<K, InputT> setMaxBatchSize(int value);
        public abstract Builder<K, InputT> setMaxLatency(Duration value);
        public abstract Builder<K, InputT> setKvCoder(KvCoder value);

        public abstract GroupIntoBatches<K, InputT> build();
    }
}
