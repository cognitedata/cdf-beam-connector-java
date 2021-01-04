package com.cognite.beam.io.transform;

import com.cognite.beam.io.TestConfigProviderV1;
import com.cognite.client.dto.Asset;
import com.google.protobuf.Int64Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BuildAssetLookupTest extends TestConfigProviderV1 {
    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    void buildAssetLookup() {
        Asset root = Asset.newBuilder().setId(Int64Value.of(1)).setName("Root asset").setRootId(Int64Value.of(1)).build();
        Asset child1 = Asset.newBuilder().setId(Int64Value.of(2)).setName("Child_1")
                .setRootId(Int64Value.of(1)).setParentId(Int64Value.of(1)).build();
        Asset child2 = Asset.newBuilder().setId(Int64Value.of(3)).setName("Child_2")
                .setRootId(Int64Value.of(1)).setParentId(Int64Value.of(1)).build();

        Pipeline p = Pipeline.create();
        p.apply("Build start collection", Create.of(root, child1, child2))
                .apply("Build lookup asset", BuildAssetLookup.create())
                .apply("Map into string", MapElements.into(TypeDescriptors.strings())
                        .via(assetLookup -> assetLookup.toString()))
                .apply("Write to file", TextIO.write().to("./unitTest_assetLookup")
                        .withoutSharding()
                        .withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}