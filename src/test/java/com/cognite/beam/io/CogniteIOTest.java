package com.cognite.beam.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.cognite.client.dto.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.protobuf.AbstractMessage;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CogniteIOTest extends TestConfigProviderV1 {

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    void readAssetsConfiguration() {
        final String limit = "1";
        final String myMetaKey = "myMetaValue";

        Assets.Read assetReader = CogniteIO.readAssets()
                .withProjectConfig(projectConfigClientCredentials)
                .withRequestParameters(RequestParameters.create()
                        .withRootParameter("limit", limit)
                        .withFilterMetadataParameter("myMetaKey", myMetaKey));


        assertEquals(limit, assetReader.getRequestParameters().getRequestParameters().get("limit"));
        assertEquals(myMetaKey,
                assetReader.getRequestParameters().getMetadataFilterParameters().get("myMetaKey"));
    }

    @Test
    @Tag("remoteCDP")
    void readAssetsDataExtract() throws Exception {
        final OkHttpClient client = new OkHttpClient.Builder().build();
        List<Long> assetIds = new ArrayList<>();

        String jsonString = TestUtilsV1.generateAssetJson();
        //System.out.println("Posting json: ");
        //System.out.println(jsonString);

        // post the events to cdp
        Request request = buildRequest(jsonString, "assets");
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }

            assetIds = TestUtilsV1.extractIds(response.body().string());
            //System.out.println("Extracted ids from post asset response: " + assetIds.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // pause, for the eventual consistency...
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Pipeline pipeline = Pipeline.create();

        final PCollection<Asset> assets = pipeline.apply(CogniteIO.readAssets()
                //.withProjectConfig(projectConfig)
                .withProjectConfigFile(getProjectConfigFileName())
                .withRequestParameters(RequestParameters.create()
                        .withFilterParameter("source", TestUtilsV1.sourceValue))
                );

        assets.apply("Format results", MapElements
                .into(TypeDescriptors.strings())
                .via(AbstractMessage::toString))
                .apply("Write output", TextIO.write().to("./UnitTest_assets_output")
                        .withSuffix(".txt")
                        .withoutSharding());

        pipeline.run();

        //pCollection.toString();
        //PAssert.that(pCollection).satisfies(collection -> StreamSupport.stream(collection.spliterator(), false).count());

        // clean up the cdp assets
        jsonString = TestUtilsV1.generateDeleteJson(assetIds);
        //System.out.println("Delete assets json: " + jsonString);
        request = buildRequest(jsonString, "assets/delete");

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void readEventsDataExtract() throws Exception {
        final OkHttpClient client = new OkHttpClient.Builder().build();
        List<Long> assetIds = new ArrayList<>();

        String jsonString = TestUtilsV1.generateEventJson();
        //System.out.println("Posting json: ");
        //System.out.println(jsonString);

        // post the events to cdp
        Request request = buildRequest(jsonString, "events");
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }

            assetIds = TestUtilsV1.extractIds(response.body().string());
            //System.out.println("Extracted ids from post asset response: " + assetIds.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // pause, for the eventual consistency...
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Pipeline pipeline = Pipeline.create();

        final PCollection<Event> events = pipeline.apply(CogniteIO.readEvents()
                .withProjectConfig(projectConfigClientCredentials)
                .withRequestParameters(RequestParameters.create()
                        .withFilterParameter("source", TestUtilsV1.sourceValue))

        );

        events.apply("Format results", MapElements
                .into(TypeDescriptors.strings())
                .via((Event element) -> element.toString()))
                .apply("Write output", TextIO.write().to("./UnitTest_events_output").withSuffix(".txt"));

        pipeline.run();

        //pCollection.toString();
        //PAssert.that(pCollection).satisfies(collection -> StreamSupport.stream(collection.spliterator(), false).count());

        // clean up the cdp assets
        jsonString = TestUtilsV1.generateDeleteJson(assetIds);
        //System.out.println("Delete assets json: " + jsonString);
        request = buildRequest(jsonString, "events/delete");

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void readEventsWithCursorDataExtract() throws Exception {
        final OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
        List<Long> assetIds = new ArrayList<>();

    /*
    String jsonString = TestUtilsV1.generateEventJson(1000);
    //System.out.println("Posting json: ");
    //System.out.println(jsonString);

    // post the events to cdp
    Request request = buildRequest(jsonString, "events");
    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
      }

      assetIds.addAll(TestUtilsV1.extractIds(response.body().string()));
      //System.out.println("Extracted ids from post asset response: " + assetIds.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

    // pause, for the eventual consistency...
    try {
      Thread.sleep(5000);
    } catch (Exception e) {
      e.printStackTrace();
    } */

        final Pipeline pipeline = Pipeline.create();

        final PCollection<Event> events = pipeline.apply(CogniteIO.readEvents()
                .withProjectConfig(projectConfigClientCredentials)
                .withRequestParameters(RequestParameters.create()
                        .withFilterParameter("source", TestUtilsV1.sourceValue))
        );

        events.apply("Format results", MapElements
                .into(TypeDescriptors.strings())
                .via((Event element) -> element.toString()))
                .apply("Write output", TextIO.write().to("./UnitTest_events_output").withSuffix(".txt"));

        pipeline.run();

        //pCollection.toString();
        //PAssert.that(pCollection).satisfies(collection -> StreamSupport.stream(collection.spliterator(), false).count());
/*
    // clean up the cdp assets
    jsonString = TestUtilsV1.generateDeleteJson(assetIds);
    //System.out.println("Delete assets json: " + jsonString);
    request = buildRequest(jsonString, "events/delete");

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }*/
    }

    @Test
    @Tag("remoteCDP")
    void readTsMetaDataExtract() {
        final Pipeline thePipeline = Pipeline.create();

        final PCollection<TimeseriesMetadata> tsmeta = thePipeline
                .apply(CogniteIO.readTimeseriesMetadata()
                        .withProjectConfig(projectConfigClientCredentials));

        tsmeta
                .apply("Format results",
                        MapElements.into(TypeDescriptors.strings())
                                .via(((TimeseriesMetadata element) -> element.toString())))
                .apply("Write output",
                        TextIO.write().to("./UnitTest_timeseries_output").withSuffix(".txt"));

        thePipeline.run();

        //pCollection.toString();
        //PAssert.that(pCollection).satisfies(collection -> StreamSupport.stream(collection.spliterator(), false).count());
    }

    @Test
    @Tag("remoteCDP")
    void readTsPointsDataExtract() {
        final Pipeline thePipeline = Pipeline.create();
        final RequestParameters filter = RequestParameters.create()
                .withRootParameter("start",
                        String.valueOf(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31)))
                .withRootParameter("limit", "100")
                .withItems(ImmutableList.of(ImmutableMap.of("id", 2)));

        final PCollection<TimeseriesPoint> tspoints = thePipeline
                .apply(CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withRequestParameters(filter));

        tspoints
                .apply("Format results",
                        MapElements.into(TypeDescriptors.strings())
                                .via(((TimeseriesPoint element) -> element.toString())))
                .apply(Window
                        .into(FixedWindows.of(Duration.standardMinutes(2))))
                .apply("Write output",
                        TextIO.write()
                                .to("./UnitTest_timeseriespoints_output")
                                .withSuffix(".txt")
                                .withWindowedWrites()
                                .withNumShards(1));

        thePipeline.run();
    }

    @Test
    @Tag("remoteCDP")
    void readTsMetadataAndTsPointsDataExtract() {
        final Pipeline thePipeline = Pipeline.create();
        final RequestParameters tsMetadataFilter = RequestParameters.create()
                .withFilterParameter("name", "test_ts_1");

        final PCollection<TimeseriesMetadata> tsmeta = thePipeline
                .apply(CogniteIO.readTimeseriesMetadata()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withRequestParameters(tsMetadataFilter)
                );

        tsmeta
                .apply("Format results",
                        MapElements.into(TypeDescriptors.strings())
                                .via(((TimeseriesMetadata element) -> element.toString())))
                .apply("Write output",
                        TextIO.write().to("./UnitTest_timeseries_output").withSuffix(".txt"));

        final RequestParameters tsPointsFilter = RequestParameters.create()
                .withRootParameter("start",
                        String.valueOf(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(5)))
                .withRootParameter("limit", "100");

        final PCollection<TimeseriesPoint> tspoints = thePipeline
                .apply(CogniteIO.readTimeseriesPoints()
                        .withProjectConfig(projectConfigClientCredentials)
                        .withRequestParameters(tsPointsFilter));

        tspoints
                .apply("Format results",
                        MapElements.into(TypeDescriptors.strings())
                                .via(((TimeseriesPoint element) -> element.toString())))
                .apply(Window
                        .into(FixedWindows.of(Duration.standardMinutes(2))))
                .apply("Write output",
                        TextIO.write()
                                .to("./UnitTest_timeseriespoints_output")
                                .withSuffix(".txt")
                                .withWindowedWrites()
                                .withNumShards(1));

        thePipeline.run();
    }

    /*
    @Test
    @Tag("remoteCDP")
    void read3dMetaDataExtract() {
        final Pipeline thePipeline = Pipeline.create();

        final PCollection<ThreedMetadata> threedMeta = thePipeline
                .apply(CogniteIO.readThreedMetadata()
                        .withProjectConfig(projectConfig));

        threedMeta
                .apply("Format results",
                        MapElements.into(TypeDescriptors.strings())
                                .via(((ThreedMetadata element) -> element.toString())))
                .apply("Write output",
                        TextIO.write().to("./UnitTest_threed_output").withSuffix(".txt"));

        thePipeline.run();
    }

    @Ignore
    @Test
    @Tag("remoteCDP")
    void read3dNodesDataExtract() {
        final Pipeline thePipeline = Pipeline.create();

        final PCollection<ThreedMetadata> threedMeta = thePipeline
                .apply(CogniteIO.readThreedMetadata().withProjectConfig(projectConfig));

        // this test can be slow running on the direct runner, try specifying a revision and nodeId
        //    final PCollection<ThreedMetadata> threedMeta = thePipeline.apply(
        //        Create.of(ThreedMetadata.newBuilder()
        //            .setModelId(Int64Value.of(0L))
        //            .setRevisionId(Int64Value.of(0L)).build()));
        //    final PCollection<ThreedNode> threedNodes = threedMeta
        //        .apply(CogniteIO.readThreedNodes().withProjectConfig(projectConfig).withNodeParameters(
        //            FilterParameters.builder().addParameter("nodeId", "123456789").build()));

        final PCollection<ThreedNode> threedNodes = threedMeta
                .apply(CogniteIO.readThreedNodes().withProjectConfig(projectConfig).withNodeParameters(
                        FilterParameters.builder().addParameter("nodeId", "123456789").build()));

        threedNodes
                .apply("Format results",
                        MapElements.into(TypeDescriptors.strings())
                                .via(((ThreedNode element) -> element.toString())))
                .apply("Write output",
                        TextIO.write().to("./UnitTest_threednodes_output").withSuffix(".txt"));

        thePipeline.run();
    }*/

}