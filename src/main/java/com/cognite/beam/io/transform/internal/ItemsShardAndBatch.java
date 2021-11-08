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

import com.cognite.client.dto.Item;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkState;

@AutoValue
public abstract class ItemsShardAndBatch extends PTransform<PCollection<Item>, PCollection<Iterable<Item>>> {
    private static final int DEFAULT_MAX_BATCH_SIZE = 4000;
    private static final int MIN_MAX_BATCH_SIZE = 1;
    private static final int MAX_MAX_BATCH_SIZE = 10000;

    private static final int DEFAULT_WRITE_SHARDS = 10;
    private static final int MIN_WRITE_SHARDS = 1;
    private static final int MAX_WRITE_SHARDS = 50;

    private static final Duration DEFAULT_MAX_LATENCY = Duration.ofSeconds(5);
    private static final Duration MIN_MAX_LATENCY = Duration.ofMillis(10);
    private static final Duration MAX_MAX_LATENCY = Duration.ofSeconds(60);

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public static ItemsShardAndBatch.Builder builder() {
        return new com.cognite.beam.io.transform.internal.AutoValue_ItemsShardAndBatch.Builder()
                .setMaxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .setMaxLatency(DEFAULT_MAX_LATENCY)
                .setWriteShards(DEFAULT_WRITE_SHARDS);
    }

    abstract int getMaxBatchSize();
    abstract Duration getMaxLatency();
    abstract int getWriteShards();

    public abstract ItemsShardAndBatch.Builder toBuilder();

    @Override
    public PCollection<Iterable<Item>> expand(PCollection<Item> input) {
        LOG.info("Starting Item shard and batch transform.");
        KvCoder<String, Item> keyValueCoder = KvCoder.of(StringUtf8Coder.of(), ProtoCoder.of(Item.class));

        PCollection<Iterable<Item>> outputCollection = input
                .apply("Shard items", WithKeys.of((Item inputItem) ->
                        (Math.floorMod(inputItem.hashCode(), getWriteShards())) + ""
                )).setCoder(keyValueCoder)
                .apply("Batch items", GroupIntoBatches.<String, Item>of(keyValueCoder)
                        .withMaxBatchSize(getMaxBatchSize())
                        .withMaxLatency(getMaxLatency()))
                .apply("Remove key", Values.<Iterable<Item>>create());


        return outputCollection;
    }

    @AutoValue.Builder
    public static abstract class Builder {
        public abstract Builder setMaxBatchSize(int value);
        public abstract Builder setMaxLatency(Duration value);
        public abstract Builder setWriteShards(int value);

        abstract ItemsShardAndBatch autoBuild();

        public ItemsShardAndBatch build() {
            ItemsShardAndBatch itemsShardAndBatch = autoBuild();

            checkState(itemsShardAndBatch.getMaxBatchSize() >= MIN_MAX_BATCH_SIZE
                            && itemsShardAndBatch.getMaxBatchSize() <= MAX_MAX_BATCH_SIZE,
                    String.format("Batch size must be between %d and %d", MIN_MAX_BATCH_SIZE, MAX_MAX_BATCH_SIZE));

            checkState(itemsShardAndBatch.getWriteShards() >= MIN_WRITE_SHARDS
                            && itemsShardAndBatch.getWriteShards() <= MAX_WRITE_SHARDS,
                    String.format("Number of splits must be between %d and %d", MIN_WRITE_SHARDS, MAX_WRITE_SHARDS));

            checkState(itemsShardAndBatch.getMaxLatency() != null
                            && itemsShardAndBatch.getMaxLatency().compareTo(MIN_MAX_LATENCY) > 0
                            && itemsShardAndBatch.getMaxLatency().compareTo(MAX_MAX_LATENCY) < 0,
                    String.format("Number of retries must be between %s and %s",
                            MIN_MAX_LATENCY.toString(), MAX_MAX_LATENCY.toString()));

            return itemsShardAndBatch;
        }
    }
}
