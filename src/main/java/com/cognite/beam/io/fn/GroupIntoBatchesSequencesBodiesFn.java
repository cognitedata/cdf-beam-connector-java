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

import com.cognite.beam.io.dto.SequenceBody;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.cognite.beam.io.servicesV1.ConnectorConstants.DEFAULT_SEQUENCE_WRITE_MAX_CELLS_PER_BATCH;
import static com.cognite.beam.io.servicesV1.ConnectorConstants.DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * This is a specialized version of {@code GroupIntoBatchesFn} grouping {@link SequenceBody} for write operations.
 *
 * When writing {@link SequenceBody} to Cognite, the writer operates with max 10 items and max 50k cells
 * per request. In order to maximize the I/O bandwidth of the writer, this transform will produce batches with
 * 4x size limits so that the writer executes 4 requests in parallel per batch.
 *
 * This module can be configured a maximum latency (in processing time) for outputting the batch.
 *
 * Items are batched per key and window.
 *
 */
public class GroupIntoBatchesSequencesBodiesFn
        extends DoFn<KV<String, SequenceBody>, KV<String, Iterable<SequenceBody>>> {
    private static final int DEFAULT_BATCH_MULTIPLIER = 4;

    private static final Duration MIN_MAX_LATENCY = Duration.ofMillis(10);
    private static final Duration MAX_MAX_LATENCY = Duration.ofDays(10);

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final Duration maxLatency;
    private final Duration allowedLateness;
    private final int batchMultiplier;

    @StateId("buffer")
    private final StateSpec<BagState<SequenceBody>> bufferedItems =
            StateSpecs.bag(ProtoCoder.of(SequenceBody.class));

    @StateId("key")
    private final StateSpec<ValueState<String>> key = StateSpecs.value(StringUtf8Coder.of());

    @StateId("counter")
    private final StateSpec<ValueState<Integer>> countState = StateSpecs.value(VarIntCoder.of());

    @StateId("cellCounter")
    private final StateSpec<ValueState<Integer>> cellCounter = StateSpecs.value(VarIntCoder.of());

    @StateId("staleSet")
    private final StateSpec<ValueState<Boolean>> staleState = StateSpecs.value(BooleanCoder.of());

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("stale")
    private final TimerSpec staleSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    public GroupIntoBatchesSequencesBodiesFn(Duration maxLatency,
                                             Duration allowedLateness,
                                             int batchMultiplier) {
        Preconditions.checkNotNull(allowedLateness, "allowedLateness cannot be null");
        Preconditions.checkArgument(maxLatency.compareTo(MAX_MAX_LATENCY) <= 0
                        && maxLatency.compareTo(MIN_MAX_LATENCY) >= 0,
                "Max latency size out of range. Must be between "
                        + MIN_MAX_LATENCY + " and " + MAX_MAX_LATENCY);

        this.maxLatency = maxLatency;
        this.allowedLateness = allowedLateness;
        this.batchMultiplier = batchMultiplier;
    }

    public GroupIntoBatchesSequencesBodiesFn(Duration maxLatency, Duration allowedLateness) {
        this(maxLatency, allowedLateness, DEFAULT_BATCH_MULTIPLIER);
    }

    @Setup
    public void setup() {
        LOG.info("Setting up GroupIntoBatchesSequencesBodiesFn with max items per batch = {} "
                        + "and max cells per batch = {}.",
                DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH * batchMultiplier,
                DEFAULT_SEQUENCE_WRITE_MAX_CELLS_PER_BATCH * batchMultiplier);
    }

    @ProcessElement
    public void processElement(@Element KV<String, SequenceBody> element,
                               OutputReceiver<KV<String, Iterable<SequenceBody>>> outputReceiver,
                               BoundedWindow window,
                               @StateId("buffer") BagState<SequenceBody> bufferState,
                               @StateId("key") ValueState<String> keyState,
                               @StateId("counter") ValueState<Integer> countState,
                               @StateId("cellCounter") ValueState<Integer> cellCounterState,
                               @StateId("staleSet") ValueState<Boolean> staleState,
                               @TimerId("expiry") Timer expiryTimer,
                               @TimerId("stale") Timer staleTimer) throws Exception {

        LOG.trace("Received sequences body item to buffer: {}", element.getValue().getExternalId().getValue());
        Preconditions.checkArgument(element.getValue().hasExternalId() || element.getValue().hasId(),
                "Sequences must have either externalId or id set.");

        // add item to buffer
        int count = firstNonNull(countState.read(), 0);
        count++;
        countState.write(count);
        bufferState.add(element.getValue());
        keyState.write(element.getKey());

        // add cell count
        int cellCounter = firstNonNull(cellCounterState.read(), 0);
        cellCounter += (element.getValue().getRowsCount() * element.getValue().getColumnsCount());
        cellCounterState.write(cellCounter);

        // set timer to fire at the end of the current window
        expiryTimer.set(window.maxTimestamp().plus(org.joda.time.Duration.millis(this.allowedLateness.toMillis())));

        // set staleness timer
        boolean staleSet = firstNonNull(staleState.read(), false);
        if (!staleSet) {
            staleTimer.offset(org.joda.time.Duration.millis(this.maxLatency.toMillis())).setRelative();
            staleState.write(true);
        }

        // buffer full, output result
        if (count >= (DEFAULT_SEQUENCE_WRITE_MAX_ITEMS_PER_BATCH * batchMultiplier)
                || cellCounter >= (DEFAULT_SEQUENCE_WRITE_MAX_CELLS_PER_BATCH * batchMultiplier)) {
            LOG.info("Buffer full. Writing batch of {} items for key [{}]", count, element.getKey());
            outputReceiver.output(KV.of(element.getKey(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            cellCounterState.clear();
            staleState.write(false);
        }
    }

    @OnTimer("expiry")
    public void onExpiry(@StateId("buffer") BagState<SequenceBody> bufferState,
                         @StateId("key") ValueState<String> keyState,
                         @StateId("counter") ValueState<Integer> countState,
                         @StateId("cellCounter") ValueState<Integer> cellCounterState,
                         OutputReceiver<KV<String, Iterable<SequenceBody>>> outputReceiver) throws Exception {
        LOG.debug("Window expiring triggered");
        if (!bufferState.isEmpty().read()) {
            LOG.info("Window expiring. Writing batch of {} items for key [{}]", countState.read(), keyState.read());
            outputReceiver.output(KV.of(keyState.read(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            keyState.clear();
            cellCounterState.clear();
        }
    }

    @OnTimer("stale")
    public void onStale(@StateId("buffer") BagState<SequenceBody> bufferState,
                        @StateId("key") ValueState<String> keyState,
                        @StateId("counter") ValueState<Integer> countState,
                        @StateId("cellCounter") ValueState<Integer> cellCounterState,
                        @StateId("staleSet") ValueState<Boolean> staleState,
                        OutputReceiver<KV<String, Iterable<SequenceBody>>> outputReceiver) throws Exception {
        LOG.debug("Stale state triggered");
        staleState.write(false);  //invalidate the stale timer state so it will be reset on the next arriving element.

        if (!bufferState.isEmpty().read()) {
            LOG.info("Latency timer triggered. Writing batch of {} items for key [{}]", countState.read(), keyState.read());
            outputReceiver.output(KV.of(keyState.read(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            cellCounterState.clear();
            keyState.clear();
        }
    }
}
