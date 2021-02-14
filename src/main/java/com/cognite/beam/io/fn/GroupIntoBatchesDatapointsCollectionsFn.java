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

import com.cognite.client.dto.TimeseriesPointPost;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.SetCoder;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * This is a specialized version of <code>GroupIntoBatchesFn</code> grouping timeseries datapoints collections
 * for write operations. This transform is designed to be used as the successor to <code>GroupIntoBatchesDatapointsFn</code>.
 * The first batching gathers datapoints from a single TS while the second batching groups multiple TS collections together
 * to fill up the write bach.
 *
 * Datapoints have fixed limits of max 10k distinct ids and max 100k datapoints.
 *
 * This module can be configured a maximum latency (in processing time) for outputting the batch.
 *
 * Items are batched per key and window.
 *
 */
public class GroupIntoBatchesDatapointsCollectionsFn
        extends DoFn<KV<String, Iterable<TimeseriesPointPost>>, KV<String, Iterable<TimeseriesPointPost>>> {
    private static final int MAX_DISTINCT_IDS = 10000;
    private static final int MAX_BATCH_SIZE = 200000;

    private static final Duration MIN_MAX_LATENCY = Duration.ofMillis(10);
    private static final Duration MAX_MAX_LATENCY = Duration.ofDays(10);

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final Duration maxLatency;
    private final Duration allowedLateness;

    @StateId("buffer")
    private final StateSpec<BagState<TimeseriesPointPost>> bufferedItems =
            StateSpecs.bag(ProtoCoder.of(TimeseriesPointPost.class));

    @StateId("key")
    private final StateSpec<ValueState<String>> key = StateSpecs.value(StringUtf8Coder.of());

    @StateId("counter")
    private final StateSpec<ValueState<Integer>> countState = StateSpecs.value(VarIntCoder.of());

    @StateId("tsId")
    private final StateSpec<ValueState<Set<String>>> idSet = StateSpecs.value(SetCoder.of(StringUtf8Coder.of()));

    @StateId("idCounter")
    private final StateSpec<ValueState<Integer>> idCounter = StateSpecs.value(VarIntCoder.of());

    @StateId("staleSet")
    private final StateSpec<ValueState<Boolean>> staleState = StateSpecs.value(BooleanCoder.of());

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("stale")
    private final TimerSpec staleSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    public GroupIntoBatchesDatapointsCollectionsFn(Duration maxLatency, Duration allowedLateness) {
        Preconditions.checkNotNull(allowedLateness, "allowedLateness cannot be null");
        Preconditions.checkArgument(maxLatency.compareTo(MAX_MAX_LATENCY) <= 0
                        && maxLatency.compareTo(MIN_MAX_LATENCY) >= 0,
                "Max latency size out of range. Must be between "
                        + MIN_MAX_LATENCY + " and " + MAX_MAX_LATENCY);

        this.maxLatency = maxLatency;
        this.allowedLateness = allowedLateness;
    }

    @Setup
    public void setup() {
        LOG.debug("Setting up GroupIntoBatchesDatapointsFn.");
    }

    @ProcessElement
    public void processElement(@Element KV<String, Iterable<TimeseriesPointPost>> element,
                               OutputReceiver<KV<String, Iterable<TimeseriesPointPost>>> outputReceiver,
                               BoundedWindow window,
                               @StateId("buffer") BagState<TimeseriesPointPost> bufferState,
                               @StateId("key") ValueState<String> keyState,
                               @StateId("counter") ValueState<Integer> countState,
                               @StateId("tsId") ValueState<Set<String>> idSetState,
                               @StateId("idCounter") ValueState<Integer> idCounterState,
                               @StateId("staleSet") ValueState<Boolean> staleState,
                               @TimerId("expiry") Timer expiryTimer,
                               @TimerId("stale") Timer staleTimer) throws Exception {

        LOG.trace("Received item to buffer.");
        List<TimeseriesPointPost> inputList = new ArrayList<>();
        Set<String> inputIdSet = new HashSet<>();

        // Iterate the input collection and check preconditions
        element.getValue().forEach(point -> {
            Preconditions.checkArgument(point.getIdTypeCase()
                            != TimeseriesPointPost.IdTypeCase.IDTYPE_NOT_SET,
                    "TS datapoint must have either externalId or id set.");
            inputList.add(point);
            if (point.getIdTypeCase() == TimeseriesPointPost.IdTypeCase.EXTERNAL_ID) {
                inputIdSet.add(point.getExternalId());
            } else {
                inputIdSet.add(Long.toString(point.getId()));
            }
        });
        LOG.debug("Received {} items across {} keys in the input collection.", inputList.size(), inputIdSet.size());

        // Check if the input can be added to the current buffer
        int count = firstNonNull(countState.read(), 0);
        int tsIdCounter = firstNonNull(idCounterState.read(), 0);

        // buffer full, output result
        if ((count + inputList.size()) >= MAX_BATCH_SIZE || (tsIdCounter + inputIdSet.size()) >= MAX_DISTINCT_IDS) {
            LOG.info("Buffer full. Writing batch of {} items across {} TS for key [{}]", count,
                    idCounterState.read(), element.getKey());
            outputReceiver.output(KV.of(element.getKey(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            idSetState.clear();
            idCounterState.clear();
            staleState.write(false);
            count = 0;
            tsIdCounter = 0;
        }

        // add items to buffer
        for (TimeseriesPointPost point : inputList) {
            count++;
            countState.write(count);
            bufferState.add(point);
            keyState.write(element.getKey());
        }

        // add ids to buffer
        Set<String> tsIds = firstNonNull(idSetState.read(), new HashSet<String>());
        for (String tsId : inputIdSet) {
            if (!tsIds.contains(tsId)) {
                tsIdCounter++;
                idCounterState.write(tsIdCounter);
                tsIds.add(tsId);
                idSetState.write(tsIds);
            }
        }

        // set timer to fire at the end of the current window
        expiryTimer.set(window.maxTimestamp().plus(org.joda.time.Duration.millis(this.allowedLateness.toMillis())));

        // set staleness timer
        boolean staleSet = firstNonNull(staleState.read(), false);
        if (!staleSet) {
            staleTimer.offset(org.joda.time.Duration.millis(this.maxLatency.toMillis())).setRelative();
            staleState.write(true);
        }
    }

    @OnTimer("expiry")
    public void onExpiry(@StateId("buffer") BagState<TimeseriesPointPost> bufferState,
                         @StateId("key") ValueState<String> keyState,
                         @StateId("counter") ValueState<Integer> countState,
                         @StateId("tsId") ValueState<Set<String>> idSetState,
                         @StateId("idCounter") ValueState<Integer> idCounterState,
                         OutputReceiver<KV<String, Iterable<TimeseriesPointPost>>> outputReceiver) throws Exception {
        LOG.debug("Window expiring triggered");
        if (!bufferState.isEmpty().read()) {
            LOG.info("Window expiring. Writing batch of {} items across {} TS for key [{}]",
                    countState.read(), idCounterState.read(), keyState.read());
            outputReceiver.output(KV.of(keyState.read(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            keyState.clear();
            idSetState.clear();
            idCounterState.clear();
        }
    }

    @OnTimer("stale")
    public void onStale(@StateId("buffer") BagState<TimeseriesPointPost> bufferState,
                        @StateId("key") ValueState<String> keyState,
                        @StateId("counter") ValueState<Integer> countState,
                        @StateId("tsId") ValueState<Set<String>> idSetState,
                        @StateId("idCounter") ValueState<Integer> idCounterState,
                        @StateId("staleSet") ValueState<Boolean> staleState,
                        OutputReceiver<KV<String, Iterable<TimeseriesPointPost>>> outputReceiver) throws Exception {
        LOG.debug("Stale state triggered");
        staleState.write(false);  //invalidate the stale timer state so it will be reset on the next arriving element.

        if (!bufferState.isEmpty().read()) {
            LOG.info("Latency timer triggered. Writing batch of {} items across {} TS for key [{}]",
                    countState.read(), idCounterState.read(), keyState.read());
            outputReceiver.output(KV.of(keyState.read(), bufferState.read()));
            bufferState.clear();
            countState.clear();
            idSetState.clear();
            idCounterState.clear();
            keyState.clear();
        }
    }
}
