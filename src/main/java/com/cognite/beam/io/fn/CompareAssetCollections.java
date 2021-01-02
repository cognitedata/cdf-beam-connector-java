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

import com.cognite.beam.io.dto.Asset;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Compares two collections of Assets and identifies upserts and deletes.
 */
public class CompareAssetCollections extends DoFn<KV<String, CoGbkResult>, KV<String, Asset>> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    // counters to present metrics from the change detection
    final Counter newCounter = Metrics.counter(CompareAssetCollections.class,
            "New assets");
    final Counter deleteCounter = Metrics.counter(CompareAssetCollections.class,
            "Assets to delete");
    final Counter changeCounter = Metrics.counter(CompareAssetCollections.class,
            "Updated assets");
    final Counter noChangeCounter = Metrics.counter(CompareAssetCollections.class,
            "Assets not changed");
    final Counter invalidCounter = Metrics.counter(CompareAssetCollections.class,
            "Assets with errors");

    final TupleTag<Asset> cdfAssetTag; // input assets with composite key
    final TupleTag<Asset> inputAssetTag; // cdf assets with composite key
    final TupleTag<KV<String, Asset>> upsertAssetTag; // assets to be upserted to cdf
    final TupleTag<KV<String, Asset>> deleteAssetTag; // assets to de deleted from cdf

    final String delimiter;

    public CompareAssetCollections(TupleTag<Asset> cdfAssetTag, TupleTag<Asset> inputAssetTag,
            TupleTag<KV<String, Asset>> upsertAssetTag, TupleTag<KV<String, Asset>> deleteAssetTag,
            String delimiter) {
        this.cdfAssetTag = cdfAssetTag;
        this.inputAssetTag = inputAssetTag;
        this.upsertAssetTag = upsertAssetTag;
        this.deleteAssetTag = deleteAssetTag;
        this.delimiter = delimiter;
    }

    @ProcessElement
    public void processElement(@Element KV<String, CoGbkResult> item,
                               MultiOutputReceiver outputReceiver) throws Exception {
        List<Asset> cdfAssets = new ArrayList<>(3);
        List<Asset> inputAssets = new ArrayList<>(3);
        String key = item.getKey().split(delimiter)[0];

        for (Asset element : item.getValue().getAll(cdfAssetTag)) {
            cdfAssets.add(element);
        }
        for (Asset element : item.getValue().getAll(inputAssetTag)) {
            inputAssets.add(element);
        }

        if (inputAssets.isEmpty() && cdfAssets.isEmpty()) {
            // should not happen--will just skip this record!
            LOG.warn("No asset exists in CDF nor the input for key: " + item.getKey());
            return;
        }

        if (inputAssets.size() > 1) {
            // Cannot do a meaningful comparison for equality with CDF.
            // Write all duplicates to CDF--no way to determine which one to use.
            String message = "More than one input asset for the composite key: " + item.getKey() + System.lineSeparator()
                    + "Will write all copies to CDF. Please check the input.";
            LOG.warn(message);
            invalidCounter.inc(inputAssets.size());
            for (Asset element : inputAssets) {
                outputReceiver.get(upsertAssetTag).output(KV.of(key, element));
            }
        }

        if (inputAssets.isEmpty() && !cdfAssets.isEmpty()) {
            // The asset doesn't exist in the input--delete from cdf
            for (Asset element : cdfAssets) {
                deleteCounter.inc();
                outputReceiver.get(deleteAssetTag).output(KV.of(key, element));
            }
        } else if (inputAssets.size() == 1 && cdfAssets.size() == 1) {
            // Asset exists in CDF and input. Check for equality
            if (this.equals(inputAssets.get(0), cdfAssets.get(0))) {
                noChangeCounter.inc();
                return;
            } else {
                changeCounter.inc();
                outputReceiver.get(upsertAssetTag).output(KV.of(key, inputAssets.get(0)));
            }
        } else {
            // Write the input to CDF
            for (Asset element : inputAssets) {
                newCounter.inc();
                outputReceiver.get(upsertAssetTag).output(KV.of(key, element));
            }
        }
    }

    boolean equals(Asset one, Asset other) {
        boolean result = true;
        result = result && (one.hasExternalId() == other.hasExternalId());
        if (one.hasExternalId()) {
            result = result && one.getExternalId()
                    .equals(other.getExternalId());
        }
        result = result && one.getName()
                .equals(other.getName());
        result = result && (one.hasParentExternalId() == other.hasParentExternalId());
        if (one.hasParentExternalId()) {
            result = result && one.getParentExternalId()
                    .equals(other.getParentExternalId());
        }
        result = result && (one.hasDescription() == other.hasDescription());
        if (one.hasDescription()) {
            result = result && one.getDescription()
                    .equals(other.getDescription());
        }
        result = result && one.getMetadataMap().equals(
                other.getMetadataMap());
        result = result && (one.hasSource() == other.hasSource());
        if (one.hasSource()) {
            result = result && one.getSource()
                    .equals(other.getSource());
        }
        result = result && (one.hasDataSetId() == other.hasDataSetId());
        if (one.hasDataSetId()) {
            result = result && one.getDataSetId()
                    .equals(other.getDataSetId());
        }
        result = result && (one.getLabelsList().equals(other.getLabelsList()));

        return result;
    }
}
