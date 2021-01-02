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

import com.cognite.beam.io.dto.Asset;
import com.cognite.beam.io.dto.AssetLookup;
import com.google.auto.value.AutoValue;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility transform for enriching an Asset with the root name. This can be useful when performing name-based asset
 * matching.
 *
 * This transform requires that the root asset(s) is a part of the input collection.
 */
@AutoValue
public abstract class BuildAssetLookup extends PTransform<PCollection<Asset>, PCollection<AssetLookup>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static BuildAssetLookup.Builder builder() {
        return new AutoValue_BuildAssetLookup.Builder();
    }
    public static BuildAssetLookup create() {
        return BuildAssetLookup.builder().build();
    }

    public abstract BuildAssetLookup.Builder toBuilder();

    @Override
    public PCollection<AssetLookup> expand(PCollection<Asset> input) {
        LOG.info("Starting build asset lookup transform.");

        // Lookup view with root assets
        PCollectionView<Map<Long, Asset>> rootAssetView = input
                .apply("Filter root assets", Filter.by(
                        asset -> asset.getRootId().getValue() == asset.getId().getValue()))
                .apply("Add key based on id", WithKeys.of(
                        asset -> {
                            LOG.info("Identified root asset: {}", asset);
                            return asset.getId().getValue();
                        }))
                        .setCoder(KvCoder.of(BigEndianLongCoder.of(), ProtoCoder.of(Asset.class)))
                .apply("To map view", View.asMap());

        PCollection<AssetLookup> outputCollection = input
                .apply("Filter: must have id", Filter.by(asset -> asset.hasId()))
                .apply("Add root asset name", ParDo.of(new DoFn<Asset, AssetLookup>() {
                    @ProcessElement
                    public void processElement(@Element Asset element, OutputReceiver<AssetLookup> out,
                                               ProcessContext context) {
                        Map<Long, Asset> rootAssets = context.sideInput(rootAssetView);

                        // build basic dto
                        AssetLookup.Builder outputBuilder = AssetLookup.newBuilder()
                                .setId(element.getId().getValue())
                                .setName(element.getName())
                                .putAllMetadata(element.getMetadataMap())
                                .addAllLabels(element.getLabelsList());
                        if (element.hasExternalId()) {
                            outputBuilder.setExternalId(element.getExternalId());
                        }
                        if (element.hasDescription()) {
                            outputBuilder.setDescription(element.getDescription());
                        }
                        if (element.hasParentId()) {
                            outputBuilder.setParentId(element.getParentId());
                        }
                        if (element.hasParentExternalId()) {
                            outputBuilder.setParentExternalId(element.getParentExternalId());
                        }
                        if (element.hasRootId()) {
                            outputBuilder.setRootId(element.getRootId());
                        }
                        if (element.hasCreatedTime()) {
                            outputBuilder.setCreatedTime(element.getCreatedTime());
                        }
                        if (element.hasLastUpdatedTime()) {
                            outputBuilder.setLastUpdatedTime(element.getLastUpdatedTime());
                        }
                        if (element.hasSource()) {
                            outputBuilder.setSource(element.getSource());
                        }
                        if (element.hasDataSetId()) {
                            outputBuilder.setDataSetId(element.getDataSetId());
                        }
                        if (element.hasAggregates()) {
                            outputBuilder.setAggregates(element.getAggregates());
                        }

                        // add root asset name
                        if (element.hasRootId() && rootAssets.containsKey(element.getRootId().getValue())) {
                            outputBuilder.setRootName(StringValue.of(
                                    rootAssets.get(element.getRootId().getValue()).getName()));
                        } else {
                            LOG.warn("Could not identify the root asset name for externalId [{}]: \r\n"
                                    + "id: [{}] \r\n"
                                    + "name: [{}] \r\n"
                                    + "rootId: [{}]",
                                    element.getExternalId().getValue(),
                                    element.getId().getValue(),
                                    element.getName(),
                                    element.getRootId().getValue());
                        }
                        LOG.debug("Generated AssetLookup object: \r\n {}", outputBuilder);

                        out.output(outputBuilder.build());
                    }
                }).withSideInputs(rootAssetView));

        return outputCollection;
    }

    @AutoValue.Builder
    public static abstract class Builder {
        public abstract BuildAssetLookup build();
    }
}
