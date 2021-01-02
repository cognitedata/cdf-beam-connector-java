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

package com.cognite.beam.io.fn;

import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * DoFn for grouping items into batches.
 *
 * This module can be configured with a maximum batch size as well as a maximum latency (in processing time) for
 * outputting the batch.
 *
 * Items are batched per key and window.
 *
 * @param <K>
 * @param <InputT>
 */
public class GroupIntoBatchesFn<K, InputT> extends DoFn<KV<K, InputT>, KV<K, Iterable<InputT>>> {
    private static final int MIN_MAX_BATCH_SIZE = 1;
    private static final int MAX_MAX_BATCH_SIZE = 100000;

    private static final Duration MIN_MAX_LATENCY = Duration.ofMillis(10);
    private static final Duration MAX_MAX_LATENCY = Duration.ofDays(10);

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final int maxBatchSize;
    private final Duration maxLatency;
    private final Duration allowedLateness;

    @StateId("buffer")
    private final StateSpec<BagState<InputT>> bufferedItems;

    @StateId("key")
    private final StateSpec<ValueState<K>> key;

    @StateId("counter")
    private final StateSpec<ValueState<Integer>> countState = StateSpecs.value(VarIntCoder.of());

    @StateId("staleSet")
    private final StateSpec<ValueState<Boolean>> staleState = StateSpecs.value(BooleanCoder.of());

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("stale")
    private final TimerSpec staleSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    public GroupIntoBatchesFn(int maxBatchSize, Duration maxLatency, Duration allowedLateness,
                              KvCoder kvCoder) {
        Preconditions.checkNotNull(maxLatency, "maxLatency cannot be null");
        Preconditions.checkNotNull(allowedLateness, "allowedLateness cannot be null");
        Preconditions.checkNotNull(kvCoder, "kvCoder cannot be null");
        Preconditions.checkArgument(maxBatchSize <= MAX_MAX_BATCH_SIZE && maxBatchSize >= MIN_MAX_BATCH_SIZE,
                "Max batch size out of range. Must be between "
                        + MIN_MAX_BATCH_SIZE + " and " + MAX_MAX_BATCH_SIZE);
        Preconditions.checkArgument(maxLatency.compareTo(MAX_MAX_LATENCY) <= 0
                        && maxLatency.compareTo(MIN_MAX_LATENCY) >= 0,
                "Max latency out of range. Must be between "
                        + MIN_MAX_LATENCY + " and " + MAX_MAX_LATENCY);

        bufferedItems = StateSpecs.bag(kvCoder.getValueCoder());
        key = StateSpecs.value(kvCoder.getKeyCoder());
        this.maxBatchSize = maxBatchSize;
        this.maxLatency = maxLatency;
        this.allowedLateness = allowedLateness;
    }

    @Setup
    public void setup() {
        LOG.debug("Setting up GroupIntoBatchesFn.");
    }

    @ProcessElement
    public void processElement(@Element KV<K, InputT> element, OutputReceiver<KV<K, Iterable<InputT>>> outputReceiver,
                               BoundedWindow window,
                               @StateId("buffer") BagState<InputT> bufferState,
                               @StateId("key") ValueState<K> keyState,
                               @StateId("counter") ValueState<Integer> countState,
                               @StateId("staleSet") ValueState<Boolean> staleState,
                               @TimerId("expiry") Timer expiryTimer,
                               @TimerId("stale") Timer staleTimer) throws Exception {

        LOG.trace("Received item to batch: {}", element.getValue().toString());

        // add item to buffer
        int count = firstNonNull(countState.read(), 0);
        count++;
        countState.write(count);
        bufferState.add(element.getValue());
        keyState.write(element.getKey());

        // set timer to fire at the end of the current window
        expiryTimer.set(window.maxTimestamp().plus(org.joda.time.Duration.millis(this.allowedLateness.toMillis())));

        // set staleness timer
        boolean staleSet = firstNonNull(staleState.read(), false);
        if (!staleSet) {
            staleTimer.offset(org.joda.time.Duration.millis(this.maxLatency.toMillis())).setRelative();
            staleState.write(true);
        }

        // buffer full, output result
        if (count >= this.maxBatchSize) {
            LOG.info("Buffer full. Writing batch of {} items for key [{}]", count, element.getKey());
            outputReceiver.output(KV.of(element.getKey(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            staleState.write(false);
        }
    }

    @OnTimer("expiry")
    public void onExpiry(@StateId("buffer") BagState<InputT> bufferState,
                         @StateId("key") ValueState<K> keyState,
                         @StateId("counter") ValueState<Integer> countState,
                         OutputReceiver<KV<K, Iterable<InputT>>> outputReceiver) throws Exception {
        if (!bufferState.isEmpty().read()) {
            LOG.info("Window expiring. Writing batch of {} items for key [{}]", countState.read(), keyState.read());
            outputReceiver.output(KV.of(keyState.read(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            keyState.clear();
        }
    }

    @OnTimer("stale")
    public void onStale(@StateId("buffer") BagState<InputT> bufferState,
                        @StateId("key") ValueState<K> keyState,
                        @StateId("counter") ValueState<Integer> countState,
                        @StateId("staleSet") ValueState<Boolean> staleState,
                        OutputReceiver<KV<K, Iterable<InputT>>> outputReceiver) throws Exception {
        staleState.write(false);  //invalidate the stale timer state so it will be reset on the next arriving element.

        if (!bufferState.isEmpty().read()) {
            LOG.info("Latency timer triggered. Writing batch of {} items for key [{}]", countState.read(), keyState.read());
            outputReceiver.output(KV.of(keyState.read(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            keyState.clear();
        }
    }
}
