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

package com.cognite.beam.io.config;

import com.cognite.client.config.ClientConfig;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.ValueProvider;

import java.io.Serializable;
import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Context object for carrying hints to guide the execution of various
 * connector operations.
 * <p>
 * Offering hints is optional, and only intended for advanced users with specific requirements. The
 * connectors default settings are appropriate for most scenarios.
 */
@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class Hints implements Serializable {
    // Number of read shards. Will be used to set the # of cursors for parallel retrieval
    private static final int DEFAULT_READ_SHARDS = 100;
    private static final int MIN_READ_SHARDS = 1;
    private static final int MAX_READ_SHARDS = 10000;

    // Number of read shards per worker. Will influence the CPU saturation of a worker
    private static final int DEFAULT_READ_SHARDS_PER_WORKER = 4;
    private static final int MIN_READ_SHARDS_PER_WORKER = 1;
    private static final int MAX_READ_SHARDS_PER_WORKER = 10;

    // Number of shards when writing. Will affect the batching parallelization potential
    private static final int DEFAULT_WRITE_SHARDS = 10;
    private static final int MIN_WRITE_SHARDS = 1;
    private static final int MAX_WRITE_SHARDS = 50;

    // Max batch size when writing to Raw
    private static final int DEFAULT_WRITE_RAW_MAX_BATCH_SIZE = 2000;
    private static final int MIN_WRITE_RAW_MAX_BATCH_SIZE = 1;
    private static final int MAX_WRITE_RAW_MAX_BATCH_SIZE = 10000;

    // Max latency for internal batching operations
    private static final Duration DEFAULT_WRITE_MAX_LATENCY = Duration.ofSeconds(5);
    private static final Duration MIN_WRITE_MAX_LATENCY = Duration.ofMillis(10);
    private static final Duration MAX_WRITE_MAX_LATENCY = Duration.ofHours(12);

    // Connection retries
    private static final int DEFAULT_RETRIES = 3;
    private static final int MAX_RETRIES = 20;
    private static final int MIN_RETRIES = 1;

    // Max batch size for context operations
    private static final int DEFAULT_CONTEXT_MAX_BATCH_SIZE = 1000;
    private static final int MIN_CONTEXT_MAX_BATCH_SIZE = 1;
    private static final int MAX_CONTEXT_MAX_BATCH_SIZE = 20000;

    // Hints for write TS batch optimization
    private static final UpdateFrequency DEFAULT_WRITE_TS_POINTS_UPDATE_FREQUENCY = UpdateFrequency.SECOND;

    // File binary batching
    private static final int DEFAULT_READ_FILE_BINARY_BATCH_SIZE = 8;
    private static final int MIN_READ_FILE_BINARY_BATCH_SIZE = 1;
    private static final int MAX_READ_FILE_BINARY_BATCH_SIZE = 10;

    private static final int DEFAULT_WRITE_FILE_BATCH_SIZE = 8;
    private static final int MIN_WRITE_FILE_BATCH_SIZE = 1;
    private static final int MAX_WRITE_FILE_BATCH_SIZE = 10;

    // Timeouts for async api jobs (i.e. the context api)
    private static final Duration MIN_ASYNC_API_JOB_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration DEFAULT_ASYNC_API_JOB_TIMEOUT = Duration.ofMinutes(20);
    private static final Duration MAX_ASYNC_API_JOB_TIMEOUT = Duration.ofHours(24);

    private static final ClientConfig.FeatureFlag CLIENT_FEATURE_FLAG = ClientConfig.FeatureFlag.create();

    private static Builder builder() {
        return new com.cognite.beam.io.config.AutoValue_Hints.Builder()
                .setReadShards(DEFAULT_READ_SHARDS)
                .setReadShardsPerWorker(DEFAULT_READ_SHARDS_PER_WORKER)
                .setWriteShards(DEFAULT_WRITE_SHARDS)
                .setWriteMaxBatchLatency(DEFAULT_WRITE_MAX_LATENCY)
                .setWriteRawMaxBatchSize(DEFAULT_WRITE_RAW_MAX_BATCH_SIZE)
                .setMaxRetries(DEFAULT_RETRIES)
                .setWriteTsPointsUpdateFrequency(DEFAULT_WRITE_TS_POINTS_UPDATE_FREQUENCY)
                .setContextMaxBatchSize(DEFAULT_CONTEXT_MAX_BATCH_SIZE)
                .setReadFileBinaryBatchSize(DEFAULT_READ_FILE_BINARY_BATCH_SIZE)
                .setWriteFileBatchSize(DEFAULT_WRITE_FILE_BATCH_SIZE)
                .setAsyncApiJobTimeout(DEFAULT_ASYNC_API_JOB_TIMEOUT)
                .setClientFeatureFlag(CLIENT_FEATURE_FLAG);
    }

    public static Hints create() {
        return Hints.builder().build();
    }

    public abstract ValueProvider<Integer> getReadShards();

    public abstract int getReadShardsPerWorker();

    public abstract int getWriteShards();

    public abstract int getWriteRawMaxBatchSize();

    public abstract int getContextMaxBatchSize();

    public abstract Duration getWriteMaxBatchLatency();

    public abstract ValueProvider<Integer> getMaxRetries();

    public abstract UpdateFrequency getWriteTsPointsUpdateFrequency();

    public abstract int getReadFileBinaryBatchSize();

    public abstract int getWriteFileBatchSize();

    public abstract Duration getAsyncApiJobTimeout();

    public abstract ClientConfig.FeatureFlag getClientFeatureFlag();

    abstract Builder toBuilder();

    /**
     * Sets the number of batches/shards to chunk the results data set into. In general, the higher the number of shards
     * the higher the potential for parallelization of the read process. At the same time, you should balance this with
     * the number of items in each shard. If the shards contain too few items (less than 10k) then the overhead of sharding
     * outweighs the benefits of parallelization.
     *
     * The default number of read shards is 100.
     *
     * @param value
     * @return
     */
    @Deprecated
    public Hints withReadShards(int value) {
        return toBuilder().setReadShards(value).build();
    }
    // todo: remove when new partitions Fn is implemented

    /**
     * Sets the number of batches/shards to chunk the results data set into. In general, the higher the number of shards
     * the higher the potential for parallelization of the read process. At the same time, you should balance this with
     * the number of items in each shard. If the shards contain too few items (less than 10k) then the overhead of sharding
     * outweighs the benefits of parallelization.
     *
     * The default number of read shards is 100.
     *
     * @param value
     * @return
     */
    @Deprecated
    public Hints withReadShards(ValueProvider<Integer> value) {
        return toBuilder().setReadShards(value).build();
    }
    // todo: remove when new partitions Fn is implemented

    /**
     * Sets the (max) number of parallel read shards per worker.
     *
     * This will influence the saturation of a worker. In general, I/O operations should be done in parallel in
     * order to feed the worker with enough data to saturate it.
     *
     * The default number of read shards per worker is 4.
     *
     * @param value
     * @return
     */
    public Hints withReadShardsPerWorker(int value) {
        return toBuilder().setReadShardsPerWorker(value).build();
    }

    /**
     * Sets the number of shards for the write stream. In general, the higher the number of shards
     * the higher the potential for parallelization of the writer process.
     *
     * The default number of write shards is 10.
     *
     * @param value
     * @return
     */
    public Hints withWriteShards(int value) {
        checkArgument(value >= MIN_WRITE_SHARDS && value <= MAX_WRITE_SHARDS,
                String.format("Number of splits must be between %d and %d", MIN_WRITE_SHARDS, MAX_WRITE_SHARDS));

        return toBuilder().setWriteShards(value).build();
    }

    /**
     * Sets the max latency for the batching module of the writer pipeline.
     *
     * The batcher optimizes write performance by batching together individual items before writing them to CDF. This
     * setting regulates the maximum duration the batcher will wait before writing the batch.
     *
     * This setting is mostly relevant for low-latency scenarios. The default value is set to 5 seconds.
     *
     * @param value
     * @return
     */
    public Hints withWriteMaxBatchLatency(Duration value) {
        checkArgument(value != null
                        && value.compareTo(MIN_WRITE_MAX_LATENCY) > 0
                        && value.compareTo(MAX_WRITE_MAX_LATENCY) < 0,
                String.format("Number of retries must be between %s and %s",
                        MIN_WRITE_MAX_LATENCY.toString(), MAX_WRITE_MAX_LATENCY.toString()));

        return toBuilder().setWriteMaxBatchLatency(value).build();
    }

    /**
     * Sets the max batch size when writing to Raw.
     *
     * In case you write very large rows to Raw, you may want to lower the batch size in order to better balance
     * the total workloads between workers.
     *
     * The default batch size is set to 2000 items.
     *
     * @param value
     * @return
     */
    public Hints withWriteRawMaxBatchSize(int value) {
        checkArgument(value >= MIN_WRITE_RAW_MAX_BATCH_SIZE
                        && value <= MAX_WRITE_RAW_MAX_BATCH_SIZE,
                String.format("Max write raw batch size must be between %d and %d", MIN_WRITE_RAW_MAX_BATCH_SIZE,
                        MAX_WRITE_RAW_MAX_BATCH_SIZE));

        return toBuilder().setWriteRawMaxBatchSize(value).build();
    }

    /**
     * Sets the max batch size when reading binary files.
     *
     * In some cases you may have to adjust this batch size if you see signs of api saturation.
     *
     * The default batch size is set to 8 items.
     *
     * @param value
     * @return
     */
    public Hints withReadFileBinaryBatchSize(int value) {
        checkArgument(value >= MIN_READ_FILE_BINARY_BATCH_SIZE
                        && value <= MAX_READ_FILE_BINARY_BATCH_SIZE,
                String.format("Max read file binary batch size must be between %d and %d", MIN_READ_FILE_BINARY_BATCH_SIZE,
                        MAX_READ_FILE_BINARY_BATCH_SIZE));

        return toBuilder().setReadFileBinaryBatchSize(value).build();
    }

    /**
     * Sets the max batch size when writing files to CDF.
     *
     * In some cases you may have to adjust this batch size if you see signs of api saturation.
     *
     * The default batch size is set to 8 items.
     *
     * @param value
     * @return
     */
    public Hints withWriteFileBatchSize(int value) {
        checkArgument(value >= MIN_WRITE_FILE_BATCH_SIZE
                        && value <= MAX_WRITE_FILE_BATCH_SIZE,
                String.format("Max write file batch size must be between %d and %d", MIN_WRITE_FILE_BATCH_SIZE,
                        MAX_WRITE_FILE_BATCH_SIZE));

        return toBuilder().setWriteFileBatchSize(value).build();
    }

    /**
     * Sets the max batch size when executing contextualization operations.
     *
     * The default batch size is set to 1000 items.
     *
     * @param value
     * @return
     */
    public Hints withContextMaxBatchSize(int value) {
        checkArgument(value >= MIN_CONTEXT_MAX_BATCH_SIZE
                        && value <= MAX_CONTEXT_MAX_BATCH_SIZE,
                String.format("Max context batch size must be between %d and %d", MIN_CONTEXT_MAX_BATCH_SIZE,
                        MAX_CONTEXT_MAX_BATCH_SIZE));

        return toBuilder().setContextMaxBatchSize(value).build();
    }


    /**
     * Sets the maximum number of retries for low-level operation towards the Cognite API.
     *
     * The default setting is 3. This should be sufficient for most scenarios.
     *
     * @param value
     * @return
     */
    public Hints withRetries(int value) {
        return toBuilder().setMaxRetries(value).build();
    }

    /**
     * Sets the maximum number of retries for low-level operation towards the Cognite API.
     *
     * The default setting is 3. This should be sufficient for most scenarios.   *
     *
     * @param value
     * @return
     */
    public Hints withRetries(ValueProvider<Integer> value) {
        return toBuilder().setMaxRetries(value).build();
    }

    /**
     * Sets the average time series data points update frequency. This hint is used to optimize the data points writer
     * for high throughput in a batch write scenario.
     *
     * @param value
     * @return
     */
    public Hints withWriteTsPointsUpdateFrequency(UpdateFrequency value) {
        return toBuilder().setWriteTsPointsUpdateFrequency(value).build();
    }

    /**
     * Sets the timeout for waiting for async api jobs to finish. Async api jobs includes the CDF context api endpoints
     * like entity matching and engineering diagram parsing.
     *
     * The default timeout is 20 minutes.
     *
     * @param timeout The async timeout expressed as {@link Duration}.
     * @return the {@link Hints} with the setting applied.
     */
    public Hints withAsyncApiJobTimeout(Duration timeout) {
        checkArgument(timeout.compareTo(MIN_ASYNC_API_JOB_TIMEOUT) >= 0
                        && timeout.compareTo(MAX_ASYNC_API_JOB_TIMEOUT) <= 0,
                String.format("Async job timeout must be between %s and %s", MIN_ASYNC_API_JOB_TIMEOUT,
                        MAX_ASYNC_API_JOB_TIMEOUT));

        return toBuilder().setAsyncApiJobTimeout(timeout).build();
    }

    /**
     * Sets the feature flags for CogniteClient
     * The default flags is no flags
     *
     * @param flags The flags {@link ClientConfig.FeatureFlag}.
     * @return the {@link Hints} with the setting applied
     */
    public Hints withClientFeatureFlags(ClientConfig.FeatureFlag flags) {
        return toBuilder().setClientFeatureFlag(flags).build();
    }

    public void validate() {
        checkState(getReadShards() != null && getReadShards().isAccessible()
                        && getReadShards().get() >= MIN_READ_SHARDS && getReadShards().get() <= MAX_READ_SHARDS,
                String.format("Number of splits must be between %d and %d", MIN_READ_SHARDS, MAX_READ_SHARDS));

        checkState(getWriteShards() >= MIN_WRITE_SHARDS && getWriteShards() <= MAX_WRITE_SHARDS,
                String.format("Number of splits must be between %d and %d", MIN_WRITE_SHARDS, MAX_WRITE_SHARDS));

        checkState(getMaxRetries() != null && getMaxRetries().isAccessible()
                        && getMaxRetries().get() >= MIN_RETRIES && getMaxRetries().get() <= MAX_RETRIES,
                String.format("Number of retries must be between %d and %d", MIN_RETRIES, MAX_RETRIES));

        checkState(getWriteMaxBatchLatency() != null
                        && getWriteMaxBatchLatency().compareTo(MIN_WRITE_MAX_LATENCY) > 0
                        && getWriteMaxBatchLatency().compareTo(MAX_WRITE_MAX_LATENCY) < 0,
                String.format("Max write latency must be between %s and %s",
                        MIN_WRITE_MAX_LATENCY.toString(), MAX_WRITE_MAX_LATENCY.toString()));
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setReadShards(ValueProvider<Integer> value);

        abstract Builder setReadShardsPerWorker(int value);

        abstract Builder setWriteShards(int value);

        abstract Builder setWriteRawMaxBatchSize(int value);

        abstract Builder setContextMaxBatchSize(int value);

        abstract Builder setMaxRetries(ValueProvider<Integer> value);

        abstract Builder setWriteMaxBatchLatency(Duration value);

        abstract Builder setWriteTsPointsUpdateFrequency(UpdateFrequency value);

        abstract Builder setReadFileBinaryBatchSize(int value);

        abstract Builder setWriteFileBatchSize(int value);

        abstract Builder setAsyncApiJobTimeout(Duration value);

        abstract Builder setClientFeatureFlag(ClientConfig.FeatureFlag value);

        abstract Hints autoBuild();

        Hints build() {
            // Can only check the non-ValueType fields at build time.
            Hints hints = autoBuild();
            checkState(hints.getWriteShards() >= MIN_WRITE_SHARDS
                            && hints.getWriteShards() <= MAX_WRITE_SHARDS,
                    String.format("Number of splits must be between %d and %d", MIN_WRITE_SHARDS, MAX_WRITE_SHARDS));

            checkState(hints.getWriteMaxBatchLatency() != null
                            && hints.getWriteMaxBatchLatency().compareTo(MIN_WRITE_MAX_LATENCY) > 0
                            && hints.getWriteMaxBatchLatency().compareTo(MAX_WRITE_MAX_LATENCY) < 0,
                    String.format("Number of retries must be between %s and %s",
                            MIN_WRITE_MAX_LATENCY.toString(), MAX_WRITE_MAX_LATENCY.toString()));

            checkState(hints.getWriteRawMaxBatchSize() >= MIN_WRITE_RAW_MAX_BATCH_SIZE
                            && hints.getWriteRawMaxBatchSize() <= MAX_WRITE_RAW_MAX_BATCH_SIZE,
                    String.format("Max write batch size must be between %d and %d", MIN_WRITE_RAW_MAX_BATCH_SIZE,
                            MAX_WRITE_RAW_MAX_BATCH_SIZE));

            checkArgument(hints.getReadShardsPerWorker() >= MIN_READ_SHARDS_PER_WORKER
                            && hints.getReadShardsPerWorker() <= MAX_READ_SHARDS_PER_WORKER
                    , String.format("Number of read shards per worker must be between %d and %d",
                            MIN_READ_SHARDS_PER_WORKER, MAX_READ_SHARDS_PER_WORKER));

            return hints;
        }

        public Builder setReadShards(int value) {
            checkArgument(value >= MIN_READ_SHARDS && value <= MAX_READ_SHARDS
                    , String.format("Number of shards must be between %d and %d", MIN_READ_SHARDS, MAX_READ_SHARDS));
            return setReadShards(ValueProvider.StaticValueProvider.of(value));
        }

        public Builder setMaxRetries(int value) {
            checkArgument(value >= MIN_RETRIES && value <= MAX_RETRIES
                    , String.format("Number of retries must be between %d and %d", MIN_RETRIES, MAX_RETRIES));
            return setMaxRetries(ValueProvider.StaticValueProvider.of(value));
        }

    }
}
