package com.cognite.beam.io;

import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.dto.Asset;
import com.cognite.client.dto.Item;
import com.cognite.client.config.UpsertMode;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class AssetsTest extends TestConfigProviderV1 {
    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    @Tag("remoteCDP")
    void writeTwoHierarchiesBatch() {
        Pipeline p = Pipeline.create();

        PCollection<Asset> results = p
                .apply("Create asset collection", Create.of(AssetsTest.generateAssetHierarchies()))
                .apply("Write as single batch", CogniteIO.writeAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")));

        results.apply("Asset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Asset::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_asset_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        p.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void writeAssetSelfReference() {
        List<Asset> assets = AssetsTest.generateAssetHierarchies();
        assets.add(Asset.newBuilder()
                .setExternalId("B_D")
                .setParentExternalId("B_D")
                .setName("ChildB_D")
                .setDescription("Child asset B_D")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build());

        Pipeline p = Pipeline.create();

        PCollection<Asset> results = p
                .apply("Create asset collection", Create.of(assets))
                .apply("Write as single batch", CogniteIO.writeAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")));

        results.apply("Asset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Asset::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_asset_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        Assertions.assertThrows(Exception.class, () -> p.run().waitUntilFinish());
    }

    @Test
    @Tag("remoteCDP")
    void writeAssetNoExternalId() {
        List<Asset> assets = AssetsTest.generateAssetHierarchies();
        assets.add(Asset.newBuilder()
                //.setExternalId(StringValue.of("B_D"))
                .setParentExternalId("B")
                .setName("ChildB_D")
                .setDescription("Child asset B_D")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build());

        Pipeline p = Pipeline.create();

        PCollection<Asset> results = p
                .apply("Create asset collection", Create.of(assets))
                .apply("Write as single batch", CogniteIO.writeAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")));

        results.apply("Asset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Asset::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_asset_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        Assertions.assertThrows(Exception.class, () -> p.run().waitUntilFinish());
    }

    @Test
    @Tag("remoteCDP")
    void writeAssetDuplicates() {
        List<Asset> assets = AssetsTest.generateAssetHierarchies();
        assets.add(Asset.newBuilder()
                .setExternalId("B_A")
                .setParentExternalId("B")
                .setName("ChildB_A")
                .setDescription("Child asset B_A")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build());

        Pipeline p = Pipeline.create();

        PCollection<Asset> results = p
                .apply("Create asset collection", Create.of(assets))
                .apply("Write as single batch", CogniteIO.writeAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")));

        results.apply("Asset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Asset::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_asset_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        Assertions.assertThrows(Exception.class, () -> p.run().waitUntilFinish());
    }

    @Test
    @Tag("remoteCDP")
    void writeAssetCycles() {
        List<Asset> assets = AssetsTest.generateAssetHierarchies();
        assets.add(Asset.newBuilder()
                .setExternalId("B_A_A")
                .setParentExternalId("B_A_A_A")
                .setName("ChildB_A_A")
                .setDescription("Child asset B_A_A")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build());
        assets.add(Asset.newBuilder()
                .setExternalId("B_A_A_A")
                .setParentExternalId("B_A_A")
                .setName("ChildB_A_A")
                .setDescription("Child asset B_A_A")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build());

        Pipeline p = Pipeline.create();

        PCollection<Asset> results = p
                .apply("Create asset collection", Create.of(assets))
                .apply("Write as single batch", CogniteIO.writeAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")));

        results.apply("Asset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Asset::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_asset_writeBatch_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        Assertions.assertThrows(Exception.class, () -> p.run().waitUntilFinish());
    }

    @Test
    @Tag("remoteCDP")
    void editHierarchiesBatchUpdate() {
        Pipeline p = Pipeline.create();

        PCollection<Asset> readResults = p
                .apply("Read assets", CogniteIO.readAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test"))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter("type", TestUtilsV1.sourceValue)));

        PCollection<Asset> editedAssets = readResults
                .apply("Change metadata + parent", MapElements
                        .into(TypeDescriptor.of(Asset.class))
                        .via((Asset input) -> {
                            Asset.Builder outputBuilder = input.toBuilder()
                                    .clearDescription()
                                    .clearMetadata()
                                    .putMetadata("addedMetadata", "newMeta2");
                            if (input.getExternalId().equalsIgnoreCase("A_B")) {
                                outputBuilder.setParentExternalId("A_A");
                                System.out.println("Entering change parentExternalId");
                            }

                            return outputBuilder.build();
                        }));

        PCollection<Asset> writeResults = editedAssets
                .apply("Write assets as single batch", CogniteIO.writeAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withUpsertMode(UpsertMode.UPDATE)));

        writeResults.apply("Asset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Asset::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_asset_editBatchUpdate_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        p.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void editHierarchiesBatchReplace() {
        Pipeline p = Pipeline.create();

        PCollection<Asset> readResults = p
                .apply("Read assets", CogniteIO.readAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test"))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter("type", TestUtilsV1.sourceValue)));

        PCollection<Asset> editedAssets = readResults
                .apply("Change metadata + parent", MapElements
                        .into(TypeDescriptor.of(Asset.class))
                        .via((Asset input) -> {
                            Asset.Builder outputBuilder = input.toBuilder()
                                    .clearDescription()
                                    .putMetadata("addedMetadata", "newMeta");
                            if (input.getExternalId().equalsIgnoreCase("A_B")) {
                                outputBuilder.setParentExternalId("A_A");
                                System.out.println("Entering change parentExternalId");
                            }

                            return outputBuilder.build();
                        }));

        PCollection<Asset> writeResults = editedAssets
                .apply("Write assets as single batch", CogniteIO.writeAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test")
                                .withUpsertMode(UpsertMode.REPLACE)));

        writeResults.apply("Asset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Asset::toString))
                .apply("Write insert output", TextIO.write().to("./UnitTest_asset_editBatchUpdate_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        p.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void synchronizeTwoHierarchiesBatch() {
        Asset rootA = Asset.newBuilder()
                .setExternalId("A")
                .setName("RootA")
                .setDescription("Root asset A")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build();
        Asset rootB = Asset.newBuilder()
                .setExternalId("B")
                .setName("RootB")
                .setDescription("Root asset B")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build();

        List<KV<String, Asset>> inputAssetCollection = new ArrayList<>();
        inputAssetCollection.add(KV.of("A", rootA));
        for (Asset item : generateAssets("A", 2)) {
            inputAssetCollection.add(KV.of("A", item));
        }
        inputAssetCollection.add(KV.of("B", rootB));
        for (Asset item : generateAssets("B", 1)) {
            inputAssetCollection.add(KV.of("B", item));
        }

        Pipeline p = Pipeline.create();
        Assets.SynchronizeHierarchies synchronizeHierarchies = CogniteIO.synchronizeHierarchies()
                .withProjectConfig(projectConfigClientCredentials)
                .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier("Beam SDK unit test"));

        PCollection<Asset> results = p
                .apply("Create asset collection", Create.of(inputAssetCollection))
                .apply("Synchronize hierarchies", synchronizeHierarchies);

        // Upsert results pipe
        results.apply("Upsert asset to string", MapElements
                .into(TypeDescriptors.strings())
                .via(Asset::toString))
                .apply("Write upsert output", TextIO.write().to("./UnitTest_asset_synchBatchUpsert_output")
                        .withSuffix(".txt")
                        .withoutSharding()
                        .withWindowedWrites());

        p.run().waitUntilFinish();
    }

    @Test
    void testProtoCoder() {
        Class<Asset> protoMessageClass = Asset.class;
        try {
            Asset protoMessageInstance = (Asset) protoMessageClass.getMethod("getDefaultInstance").invoke(null);
            System.out.println("Generated asset instance: " + System.lineSeparator()
                    + protoMessageInstance.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void readAssetsAggregateProperties() {
        Pipeline p = Pipeline.create();

        PCollection<Asset> readResults = p
                .apply("Read assets", CogniteIO.readAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test"))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter("type", TestUtilsV1.sourceValue)
                                .withRootParameter("aggregatedProperties", ImmutableList.of(
                                        "depth", "path", "childCount"
                                ))));

        readResults
                .apply("To string", MapElements.into(TypeDescriptors.strings())
                        .via(Asset::toString))
                .apply("Write read output", TextIO.write().to("./UnitTest_asset_readAggregates_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        p.run().waitUntilFinish();
    }

    @Test
    @Tag("remoteCDP")
    void deleteUnitTestAssets() {
        Pipeline p = Pipeline.create();

        PCollection<Asset> readResults = p
                .apply("Read assets", CogniteIO.readAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("Beam SDK unit test"))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterMetadataParameter("type", TestUtilsV1.sourceValue)));

        PCollection<Item> deleteResults =
                readResults.apply("Map into items", MapElements
                        .into(TypeDescriptor.of(Item.class))
                        .via((Asset input) ->
                                Item.newBuilder()
                                        .setExternalId(input.getExternalId())
                                        .build()
                        ))
                        .apply("Delete items", CogniteIO.deleteAssets()
                                .withProjectConfig(projectConfigClientCredentials)
                                .withWriterConfig(WriterConfig.create()
                                        .withAppIdentifier("Beam SDK unit test"))
                        );

        deleteResults.apply("Item to string", ParDo.of(new ItemToStringFn()))
                .apply("Write delete output", TextIO.write().to("./UnitTest_asset_deleteItems_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        p.run().waitUntilFinish();
    }

    private static List<Asset> generateAssets(String parentExternalId, int numberOfAssets) {
        List<Asset> objects = new ArrayList<>(numberOfAssets);
        for (int i = 0; i < numberOfAssets; i++) {
            objects.add(Asset.newBuilder()
                    .setExternalId(RandomStringUtils.randomAlphanumeric(10))
                    .setName("test_asset_" + RandomStringUtils.randomAlphanumeric(5))
                    .setDescription("test_asset_ " + RandomStringUtils.randomAlphanumeric(50))
                    .setParentExternalId(parentExternalId)
                    .putMetadata("type", TestUtilsV1.sourceValue)
                    .putMetadata("source", TestUtilsV1.sourceValue)
                    .build());
        }
        return objects;
    }

    private static List<Asset> generateAssetHierarchies() {
        List<Asset> outputList = new ArrayList<>();
        Asset rootA = Asset.newBuilder()
                .setExternalId("A")
                .setName("RootA")
                .setDescription("Root asset A")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build();
        outputList.add(rootA);

        Asset childA_A = Asset.newBuilder()
                .setExternalId("A_A")
                .setParentExternalId("A")
                .setName("ChildA_A")
                .setDescription("Child asset A_A")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build();
        outputList.add(childA_A);

        Asset childA_B = Asset.newBuilder()
                .setExternalId("A_B")
                .setParentExternalId("A")
                .setName("ChildA_B")
                .setDescription("Child asset A_B")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build();
        outputList.add(childA_B);

        Asset rootB = Asset.newBuilder()
                .setExternalId("B")
                .setName("RootB")
                .setDescription("Root asset B")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build();
        outputList.add(rootB);

        Asset childB_A = Asset.newBuilder()
                .setExternalId("B_A")
                .setParentExternalId("B")
                .setName("ChildB_A")
                .setDescription("Child asset B_A")
                .putMetadata("type", TestUtilsV1.sourceValue)
                .build();
        outputList.add(childB_A);

        return outputList;
    }

    @Test
    @Tag("remoteCDP")
    void readAssetsStreaming() {
        final String sessionId = RandomStringUtils.randomAlphanumeric(10);
        Pipeline pipeline = Pipeline.create();

        PCollection<Asset> readResults = pipeline
                .apply("Read assets", CogniteIO.readAssets()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withSessionIdentifier(sessionId)
                                .watchForNewItems()
                                .withPollInterval(java.time.Duration.ofSeconds(10))
                                .withPollOffset(java.time.Duration.ofSeconds(10)))
                        .withRequestParameters(RequestParameters.create()
                                .withFilterParameter("type", TestUtilsV1.sourceValue))
                )
                .apply("Set window", Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(20))));

        readResults
                .apply("Count assets received", Combine.globally(Count.<Asset>combineFn())
                        .withoutDefaults())
                .apply("Format count output", MapElements.into(TypeDescriptors.strings())
                        .via(count -> "Timestamp: " + System.currentTimeMillis() + ", Assets: " + count))
                .apply("Write read count output", TextIO.write()
                        .to(new TestFilenamePolicy("./UnitTest_assets_readStreaming_count_output", ".txt"))
                        .withNumShards(2)
                        .withWindowedWrites());

        //PAssert.that(results).containsInAnyOrder("a"); // Not compatible with Junit5
        pipeline.run().waitUntilFinish();
    }

    static class ItemToStringFn extends DoFn<Item, String> {
        @ProcessElement
        public void processElement(@Element Item input, OutputReceiver<String> outputReceiver) {
            outputReceiver.output(input.toString());
        }
    }
}