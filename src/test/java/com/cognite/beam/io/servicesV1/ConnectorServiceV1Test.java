package com.cognite.beam.io.servicesV1;

import com.cognite.beam.io.TestConfigProviderV1;
import com.cognite.beam.io.TestUtilsV1;
import com.cognite.beam.io.dto.FileBinary;
import com.cognite.beam.io.dto.PnIDResponse;
import com.cognite.beam.io.servicesV1.parser.PnIDResponseParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.BytesValue;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class ConnectorServiceV1Test extends TestConfigProviderV1 {
    Logger LOG = LoggerFactory.getLogger(this.getClass());

    @BeforeAll
    static void tearup() {
        init();
    }

    @Test
    void builder() {
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();

        assertEquals(3, connectorService.getMaxRetries().get());

        connectorService = connectorService.toBuilder().setMaxRetries(10).build();
        assertEquals(10, connectorService.getMaxRetries().get());

    }

    @Test
    @Tag("remoteCDP")
    void readAssetsWithNoResults() {
        final OkHttpClient client = new OkHttpClient.Builder().build();
        List<Long> assetIds = new ArrayList<>();

        // Read the assets with the connector service.
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();

        RequestParameters requestParameters = RequestParameters.create()
                .withRootParameter("limit", 1) // force pagination
                .withFilterParameter("source", "NoAssetWillMatchThisValueEver")
                .withProjectConfig(projectConfig);

        try {
            //System.out.println("Entering the read asset section");

            Iterator<CompletableFuture<ResponseItems<String>>> resultsIterator
                    = connectorService.readAssets(requestParameters);

            List<String> resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In first while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            //System.out.println("Exiting first while loop");
            assertEquals(0, resultsList.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void readAssetsWithResults() {
        final OkHttpClient client = new OkHttpClient.Builder().build();
        List<String> assetExternalIds = new ArrayList<>();

        String jsonString = TestUtilsV1.generateAssetJson();
        //System.out.println("Posting json: ");
        //System.out.println(jsonString);

        // post the assets to cdp
        Request request = buildRequest(jsonString, "assets");

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }

            assetExternalIds = TestUtilsV1.extractExternalIds(response.body().string());
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

        // Read the assets with the connector service.
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();

        RequestParameters requestParameters = RequestParameters.create()
                .withRootParameter("limit", 1) // force pagination
                .withFilterMetadataParameter("source", TestUtilsV1.sourceValue)
                .withProjectConfig(projectConfig);

        try {
            //System.out.println("Entering the read asset section");

            Iterator<CompletableFuture<ResponseItems<String>>> resultsIterator
                    = connectorService.readAssets(requestParameters);

            List<String> resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In first while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            //System.out.println("Exiting first while loop");
            assertEquals(2, resultsList.size());

            // read without pagination
            requestParameters = requestParameters
                    .withRootParameter("limit", 100);

            resultsIterator = connectorService.readAssets(requestParameters);
            resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In second while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            //System.out.println("Exiting second while loop");
            assertEquals(2, resultsList.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // clean up the cdp assets
        jsonString = TestUtilsV1.generateDeleteJsonExternalId(assetExternalIds);
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
    void readAssetsFilterWithResults() {
        final OkHttpClient client = new OkHttpClient.Builder().build();
        List<String> assetExternalIds = new ArrayList<>();

        String jsonString = TestUtilsV1.generateAssetJson();
        //System.out.println("Posting json: ");
        //System.out.println(jsonString);

        // post the assets to cdp
        Request request = buildRequest(jsonString, "assets");

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }

            assetExternalIds = TestUtilsV1.extractExternalIds(response.body().string());
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

        // Read the assets with the connector service.
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();

        RequestParameters requestParameters = RequestParameters.create()
                .withRootParameter("limit", 1) // force pagination
                .withFilterParameter("source", TestUtilsV1.sourceValue)
                .withProjectConfig(projectConfig);

        try {
            //System.out.println("Entering the read asset section");

            Iterator<CompletableFuture<ResponseItems<String>>> resultsIterator
                    = connectorService.readAssets(requestParameters);

            List<String> resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In first while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            //System.out.println("Exiting first while loop");
            assertEquals(2, resultsList.size());

            // read without pagination
            requestParameters = requestParameters
                    .withRootParameter("limit", 100);

            resultsIterator = connectorService.readAssets(requestParameters);
            resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In second while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            //System.out.println("Exiting second while loop");
            assertEquals(2, resultsList.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // clean up the cdp assets
        jsonString = TestUtilsV1.generateDeleteJsonExternalId(assetExternalIds);
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
    void readEventsWithResults() {
        final OkHttpClient client = new OkHttpClient.Builder().build();
        List<String> externalIds = new ArrayList<>();

        String jsonString = TestUtilsV1.generateEventJson();
        //System.out.println("Posting json: ");
        //System.out.println(jsonString);

        // post the events to cdp
        Request request = buildRequest(jsonString, "events");
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }

            externalIds = TestUtilsV1.extractExternalIds(response.body().string());
            LOG.info("Extracted ids from post asset response: {}", externalIds.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // pause, for the eventual consistency...
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Read the events with the connector service.
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();

        RequestParameters requestParameters = RequestParameters.create()
                .withRootParameter("limit", 1) // force pagination
                .withFilterMetadataParameter("source", TestUtilsV1.sourceValue)
                .withProjectConfig(projectConfig);

        try {
            //System.out.println("Entering the read event section");

            Iterator<CompletableFuture<ResponseItems<String>>> resultsIterator
                    = connectorService.readEvents(requestParameters);

            List<String> resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In first while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            //System.out.println("Exiting first while loop");
            assertEquals(2, resultsList.size());

            // read without pagination
            requestParameters = requestParameters
                    .withRootParameter("limit", 100);

            resultsIterator = connectorService.readEvents(requestParameters);
            resultsList = new ArrayList<>();
            String resultItem;

            while (resultsIterator.hasNext()) {
                //System.out.println("In second while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());

            }
            //System.out.println("Exiting second while loop");
            assertTrue(resultsList.size() > 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // clean up the cdp assets
        jsonString = TestUtilsV1.generateDeleteJsonExternalId(externalIds);
        //System.out.println("Delete events json: " + jsonString);
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
    void readSeqMetadataWithResults() {

    }

    @Test
    @Tag("remoteCDP")
    void readTsMetadataWithResults() {
        final OkHttpClient client = new OkHttpClient.Builder().build();
        List<Long> ids = new ArrayList<>();

        String jsonString = TestUtilsV1.generateTsMetaJson();
        //System.out.println("Posting json: ");
        //System.out.println(jsonString);

        // post the TS headers to cdp
        Request request = buildRequest(jsonString, "timeseries");
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }

            ids = TestUtilsV1.extractIds(response.body().string());
            //System.out.println("Extracted ids from post timeseries response: " + ids.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // pause, for the eventual consistency...
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Read the assets with the connector service.
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();

        RequestParameters requestParameters = RequestParameters.create()
                .withRootParameter("limit", 1)
                .withFilterMetadataParameter("source", TestUtilsV1.sourceValue)
                .withProjectConfig(projectConfig);

        try {
            // System.out.println("Entering the read tsmetadata section");

            Iterator<CompletableFuture<ResponseItems<String>>> resultsIterator
                    = connectorService.readTsHeaders(requestParameters);

            List<String> resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In first while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            // System.out.println("Exiting first while loop");
            //     assertEquals(2, resultsList.size());

            // read without pagination
            requestParameters = requestParameters
                    .withRootParameter("limit", 100)
                    .withFilterMetadataParameter("source", TestUtilsV1.sourceValue);
            resultsIterator = connectorService.readTsHeaders(requestParameters);

            resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                // System.out.println("In second while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            // System.out.println("Exiting second while loop");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // clean up the cdp assets
        jsonString = TestUtilsV1.generateDeleteJson(ids);
        // System.out.println("Delete timeseries json: " + jsonString);
        request = buildRequest(jsonString, "timeseries/delete");

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP status " + response.code() + ", " + response.body().string());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Disabled
    @Tag("remoteCDP")
    void writeEvents() throws Exception {
        int numberOfItems = 500;
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ConnectorServiceV1.ItemWriter itemWriter = connectorService.writeEvents();
        List<Long> ids = new ArrayList<>();
        Optional<Long> parsedId;
        List<Map<String, Object>> deleteItems = new ArrayList<>();
        Map<String, Object> deleteItem;

        // Build request parameters with events payload
        RequestParameters postEventsParameters = RequestParameters.create()
                .withItems(TestUtilsV1.generateEventItems(numberOfItems))
                .withProjectConfig(projectConfig);

        ResponseItems<String> responseItems = itemWriter.writeItems(postEventsParameters);
        //System.out.println(itemWriter.getResponseBody());

        assertTrue(responseItems.isSuccessful());
        assertEquals(numberOfItems, responseItems.getResultsItems().size());
        System.out.println("Number of results items: " + responseItems.getResultsItems().size());

        //Get ids
        for (String item : responseItems.getResultsItems()) {
            parsedId = TestUtilsV1.extractIdFromItem(item);
            assertTrue(parsedId.isPresent(), "Could not parse id for item: " + item);
            ids.add(parsedId.get());
        }

        // Build delete payload based on the ids
        for (Long id : ids) {
            deleteItem = new HashMap<>();
            deleteItem.put("id", id);

            deleteItems.add(deleteItem);
        }

        // Build request parameters with events payload
        postEventsParameters = RequestParameters.create()
                .withItems(deleteItems);

        assertTrue(connectorService.deleteEvents().writeItems(postEventsParameters).isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void writeFilterUpdateAndDeleteEvents() throws Exception {
        int numberOfItems = 1000;
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ConnectorServiceV1.ItemWriter itemWriter = connectorService.writeEvents();
        List<Long> ids = new ArrayList<>();
        Optional<Long> parsedId;
        List<Map<String, Object>> deleteItems = new ArrayList<>();
        Map<String, Object> deleteItem;

        // Build request parameters with events payload
        RequestParameters postEventsParameters = RequestParameters.create()
                .withItems(TestUtilsV1.generateEventItems(numberOfItems))
                .withProjectConfig(projectConfig);

        ResponseItems<String> responseItems = itemWriter.writeItems(postEventsParameters);
        //System.out.println(itemWriter.getResponseBody());

        assertTrue(responseItems.isSuccessful());
        assertEquals(numberOfItems, responseItems.getResultsItems().size());
        LOG.info("Number of results items from write: {}", responseItems.getResultsItems().size());

        //Get ids
        for (String item : responseItems.getResultsItems()) {
            parsedId = TestUtilsV1.extractIdFromItem(item);
            assertTrue(parsedId.isPresent(), "Could not parse id for item: " + item);
            ids.add(parsedId.get());
        }

        // wait for eventual consistency.
        Thread.sleep(6000);

        // get ids based on advanced filter.
        RequestParameters filterEvents = RequestParameters.create()
                .withFilterMetadataParameter("type", TestUtilsV1.sourceValue)
                .withProjectConfig(projectConfig);

        Iterator<CompletableFuture<ResponseItems<String>>> resultsIterator
                = connectorService.readEvents(filterEvents);

        List<String> resultsList = new ArrayList<>();
        while (resultsIterator.hasNext()) {
            //System.out.println("In first while loop. Calling next()");
            resultsList.addAll(resultsIterator.next().join().getResultsItems());
        }
        LOG.info("Number of results items from read w/filter: " + resultsList.size());

        //Build update payload
        List<Map<String,Object>> updateItems = new ArrayList<>();
        for (String item : resultsList) {
            parsedId = TestUtilsV1.extractIdFromItem(item);
            assertTrue(parsedId.isPresent(), "Could not parse id for item: " + item);

            Map<String, Object> root = new HashMap<>();
            root.put("id", parsedId.get());
            root.put("update", ImmutableMap.<String, Object>of(
                    "metadata", ImmutableMap.<String, Object>of(
                            "set", ImmutableMap.<String, String>of(
                                    "type", TestUtilsV1.updatedSourceValue
                            )
                    )
            ));

            updateItems.add(root);
        }

        postEventsParameters = RequestParameters.create()
                .withItems(updateItems)
                .withProjectConfig(projectConfig);
        assertTrue(connectorService.updateEvents().writeItems(postEventsParameters).isSuccessful());


        // Build delete payload based on the ids
        for (Long id : ids) {
            deleteItem = new HashMap<>();
            deleteItem.put("id", id);

            deleteItems.add(deleteItem);
        }

        // Build request parameters with events payload
        postEventsParameters = RequestParameters.create()
                .withItems(deleteItems)
                .withProjectConfig(projectConfig);

        assertTrue(connectorService.deleteEvents().writeItems(postEventsParameters).isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void writeTooManyEvents() throws Exception {
        int numberOfItems = 1001;
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ConnectorServiceV1.ItemWriter itemWriter = connectorService.writeEvents();
        List<Long> ids = new ArrayList<>();
        Optional<Long> parsedId;

        // Build request parameters with events payload
        RequestParameters postEventsParameters = RequestParameters.create()
                .withItems(TestUtilsV1.generateEventItems(numberOfItems))
                .withProjectConfig(projectConfig);

        assertThrows(Exception.class, () -> itemWriter.writeItems(postEventsParameters));
        //System.out.println(itemWriter.getResponseBody());

        //System.out.println("Number of results items: " + itemWriter.getResultsItems().size());
    }

    @Test
    @Tag("remoteCDP")
    void writeFilterUpdateAndDeleteTsHeader() throws Exception {
        int numberOfItems = 1;
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ConnectorServiceV1.ItemWriter itemWriter = connectorService.writeTsHeaders();
        List<String> externalIds = new ArrayList<>();
        List<Long> ids = new ArrayList<>();
        Optional<Long> parsedId;
        Optional<String> parsedExternalId;
        List<Map<String, Object>> deleteItems = new ArrayList<>();
        Map<String, Object> deleteItem;

        // Build request parameters with events payload
        RequestParameters postTsHeaderParameters = RequestParameters.create()
                .withItems(TestUtilsV1.generateTsHeaderItems(numberOfItems, false))
                .withProjectConfig(projectConfig);

        ResponseItems<String> responseItems = itemWriter.writeItems(postTsHeaderParameters);
        //System.out.println(itemWriter.getResponseBody());

        assertTrue(responseItems.isSuccessful());
        assertEquals(numberOfItems, responseItems.getResultsItems().size());
        System.out.println("Number of results items: " + responseItems.getResultsItems().size());

        //Get ids
        for (String item : responseItems.getResultsItems()) {
            System.out.println("Parsing results item: " + item);
            parsedId = TestUtilsV1.extractIdFromItem(item);
            parsedExternalId = TestUtilsV1.extractExternalIdFromItem(item);
            //assertTrue(parsedExternalId.isPresent(), "Could not parse id for item: " + item);
            externalIds.add(parsedExternalId.get());
            ids.add(parsedId.get());
        }

        // wait for eventual consistency.
        Thread.sleep(6000);

        // get ids based on advanced filter.

        // disabled until TS header can support list filters
        /*
        RequestParameters filterTsHeader = RequestParameters.builder().build()
                .withFilterMetadataParameter("type", TestUtilsV1.sourceValue);

        Iterator<String> resultsIterator = connectorService.readTsHeaders(filterTsHeader);
        List<String> resultsList = new ArrayList<>();
        while (resultsIterator.hasNext()) {
            // System.out.println("In first while loop. Calling next()");
            resultsList.add(resultsIterator.next());
        }
        System.out.println("Number of items from filter: " + resultsList.size());

         */

        //Build update payload
        List<Map<String,Object>> updateItems = new ArrayList<>();
        //for (String item : resultsList) {
        for (String item : externalIds) {
            //parsedExternalId = TestUtilsV1.extractExternalIdFromItem(item);
            //assertTrue(parsedExternalId.isPresent(), "Could not parse id for item: " + item);

            Map<String, Object> root = new HashMap<>();
            //root.put("id", parsedExternalId.get());
            root.put("externalId", item);
            root.put("update", ImmutableMap.<String, Object>of(
                    "metadata", ImmutableMap.<String, Object>of(
                            "set", ImmutableMap.<String, String>of(
                                    "type", TestUtilsV1.updatedSourceValue,
                                    "source", TestUtilsV1.sourceValue
                            )
                    )
            ));

            updateItems.add(root);
        }

        postTsHeaderParameters = RequestParameters.create()
                .withItems(updateItems)
                .withProjectConfig(projectConfig);
        assertTrue(connectorService.updateTsHeaders().writeItems(postTsHeaderParameters).isSuccessful());


        // Build delete payload based on the ids
        for (String externalId : externalIds) {
            deleteItem = new HashMap<>();
            deleteItem.put("externalId", externalId);

            deleteItems.add(deleteItem);
        }

        // Build request parameters with events payload
        postTsHeaderParameters = RequestParameters.create()
                .withItems(deleteItems)
                .withProjectConfig(projectConfig);

        //assertTrue(connectorService.deleteTsHeaders().writeItems(postTsHeaderParameters));
    }

    @Test
    @Disabled
    @Tag("remoteCDP")
    void deleteTsHeaders() throws Exception {
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        List<Map<String, Object>> deleteItems = new ArrayList<>();
        Map<String, Object> deleteItem;
        List<String> externalIds = ImmutableList.of("suOjzB8fs2", "AIGYLxUoBF");
        // Build delete payload based on the ids
        for (String externalId : externalIds) {
            deleteItem = new HashMap<>();
            deleteItem.put("externalId", externalId);

            deleteItems.add(deleteItem);
        }

        // Build request parameters with events payload
        RequestParameters deleteTsHeaderParameters = RequestParameters.create()
                .withItems(deleteItems)
                .withProjectConfig(projectConfig);
        ConnectorServiceV1.ItemWriter writer = connectorService.deleteTsHeaders();
        /*
        if (!writer.writeItems(deleteTsHeaderParameters)) {
            System.out.println(writer.getResponseBody());
        }
         */

        assertTrue(writer.writeItems(deleteTsHeaderParameters).isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void readRawDbNames() {
        // Read raw database names with the connector service.
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();

        try {
            //System.out.println("Entering the read asset section");

            Iterator<CompletableFuture<ResponseItems<String>>> resultsIterator =
                    connectorService.readRawDbNames(projectConfig);

            List<String> resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In first while loop. Calling next()");
                ResponseItems<String> responseItems = resultsIterator.next().join();
                assertTrue(responseItems.isSuccessful());
                resultsList.addAll(responseItems.getResultsItems());
            }
            //System.out.println("Exiting first while loop");

            resultsList.stream().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void readRawTableNames() {
        // Read raw database names with the connector service.
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();

        try {
            //System.out.println("Entering the read asset section");

            Iterator<CompletableFuture<ResponseItems<String>>> resultsIterator =
                    connectorService.readRawTableNames("test_db", projectConfig);

            List<String> resultsList = new ArrayList<>();
            while (resultsIterator.hasNext()) {
                //System.out.println("In first while loop. Calling next()");
                resultsList.addAll(resultsIterator.next().join().getResultsItems());
            }
            //System.out.println("Exiting first while loop");
            //assertEquals(2, resultsList.size());
            resultsList.stream().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Tag("remoteCDP")
    void writeAndDeleteTsMetaAndDatapoints() throws Exception {
        int numberOfTs = 2;
        int numberOfDatapointsPerTs = 1000;
        long datapointsFrequency = 1l;

        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ConnectorServiceV1.ItemWriter itemWriter = connectorService.writeTsHeaders();

        List<String> ids = new ArrayList<>();
        Optional<String> parsedId;
        List<Map<String, Object>> deleteItems = new ArrayList<>();
        Map<String, Object> deleteItem;

        // Build request parameters with events payload
        RequestParameters postTsHeaderParameters = RequestParameters.create()
                .withItems(TestUtilsV1.generateTsHeaderItems(numberOfTs, false))
                .withProjectConfig(projectConfig);

        ResponseItems<String> responseItems = itemWriter.writeItems(postTsHeaderParameters);
        //System.out.println(itemWriter.getResponseBody());

        assertTrue(responseItems.isSuccessful());
        assertEquals(numberOfTs, responseItems.getResultsItems().size());
        System.out.println("Number of results items: " + responseItems.getResultsItems().size());

        //Get ids
        for (String item : responseItems.getResultsItems()) {
            parsedId = TestUtilsV1.extractExternalIdFromItem(item);
            assertTrue(parsedId.isPresent(), "Could not parse id for item: " + item);
            ids.add(parsedId.get());
        }

        // Build datapoints payload
        itemWriter = connectorService.writeTsDatapoints();
        RequestParameters postTsDatapointsParameters = RequestParameters.create()
                .withItems(TestUtilsV1.generateTsDatapointsItems(numberOfDatapointsPerTs, datapointsFrequency, ids))
                .withProjectConfig(projectConfig);
        assertTrue(itemWriter.writeItems(postTsDatapointsParameters).isSuccessful(), "Post datapoints failed");

        // Build delete payload based on the ids
        for (String id : ids) {
            deleteItem = new HashMap<>();
            deleteItem.put("externalId", id);

            deleteItems.add(deleteItem);
        }

        // Build request parameters with events payload
        RequestParameters deleteReqParameters = RequestParameters.create()
                .withItems(deleteItems)
                .withProjectConfig(projectConfig);

        assertTrue(connectorService.deleteTsHeaders().writeItems(deleteReqParameters).isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void createPnID() throws Exception {

        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ItemReader<String> detectAnnotations = connectorService.detectAnnotationsPnid();

        RequestParameters postPnID = RequestParameters.create()
                .withRootParameter("fileId", 7339813294098717L)
                .withRootParameter("entities",  ImmutableList.of("01-100-PE-N", "02-100-PE-N", "MP 02 S"))
                .withProjectConfig(projectConfig);

        CompletableFuture<ResponseItems<String>> responseItems = detectAnnotations.getItemsAsync(postPnID);

        //LOG.info("ResponseItems: " + responseItems.get().toString());
        //LOG.info("ResultsItems: " + responseItems.get().getResultsItems().toString());

        PnIDResponse annotationsResponse =
                PnIDResponseParser.ParsePnIDAnnotationResponse(responseItems.get().getResultsItems().get(0));
        LOG.info("PnIDResponse object, annotations: " + annotationsResponse);

        ItemReader<String> interactiveFiles = connectorService.convertPnid();
        RequestParameters getInteractiveFilesRequest = RequestParameters.create()
                .withProjectConfig(projectConfig)
                .withRootParameter("fileId", 7339813294098717L)
                .withRootParameter("items", annotationsResponse.getEntitiesList());
        //LOG.info("Request object for get interactive pnid: " + getInteractiveFilesRequest.getRequestParametersAsJson());
        CompletableFuture<ResponseItems<String>> intPnidResponse = interactiveFiles.getItemsAsync(getInteractiveFilesRequest);
        //LOG.info("Int pnid ResponseItems: " + intPnidResponse.get().toString());
        //LOG.info("Int pnid ResultsItems: " + intPnidResponse.get().getResultsItems().toString());
        PnIDResponse interactiveResponse =
                PnIDResponseParser.ParsePnIDConvertResponse(intPnidResponse.get().getResultsItems().get(0));
        LOG.info("PnIDResponse object, intPnId: " + interactiveResponse);

        // download the binaries
        CompletableFuture<FileBinary> svgResponse = null;
        CompletableFuture<FileBinary> pngResponse = null;
        if (interactiveResponse.hasSvgUrl()) {
            svgResponse = ConnectorServiceV1.DownloadFileBinary
                    .downloadFileBinaryFromURL(interactiveResponse.getSvgUrl().getValue());
        }
        if (interactiveResponse.hasPngUrl()) {
            pngResponse = ConnectorServiceV1.DownloadFileBinary
                    .downloadFileBinaryFromURL(interactiveResponse.getPngUrl().getValue());
        }

        PnIDResponse.Builder finalResponseBuilder = annotationsResponse.toBuilder()
                .mergeFrom(interactiveResponse);

        LOG.info("Merged response w/ annotations and file paths: " + finalResponseBuilder.build());

        if (null != svgResponse) {
            LOG.info("Found SVG. Adding to response object");
            finalResponseBuilder.setSvgBinary(BytesValue.of(svgResponse.join().getBinary()));
            try {
                Files.write(Paths.get("./test_int_pnid.svg"), finalResponseBuilder.getSvgBinary().getValue().toByteArray());
                Files.write(Paths.get("./test_int_pnid_from_reponse.svg"), svgResponse.get().getBinary().toByteArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (null != pngResponse) {
            LOG.info("Found PNG. Adding to response object");
            finalResponseBuilder.setPngBinary(BytesValue.of(pngResponse.get().getBinary()));
            try {
                Files.write(Paths.get("./test_int_pnid.png"), finalResponseBuilder.getPngBinary().getValue().toByteArray());
                Files.write(Paths.get("./test_int_pnid_from_reponse.png"), pngResponse.get().getBinary().toByteArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        PnIDResponse finalResponse = finalResponseBuilder.build();
        //LOG.info("PnIDResponse object, final: " + finalResponse);

        assertTrue(responseItems.get().isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void readEntityMatcherModels() throws Exception {
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ItemReader<String> entityMatchReader = connectorService.readEntityMatcherModels();

        CompletableFuture<ResponseItems<String>> responseItems = entityMatchReader
                .getItemsAsync(RequestParameters.create().withProjectConfig(projectConfig));

        //System.out.println("Response body: \r\n" + responseItems.join().getResponseBodyAsString());
        System.out.println("Response items:");
        for (String item : responseItems.join().getResultsItems()) {
            System.out.println("--------------- Item ----------------");
            System.out.println(item);
            System.out.println("-------------------------------------");
        }
        assertTrue(responseItems.join().isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void deleteEntityMatcherModel() throws Exception {
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ConnectorServiceV1.ItemWriter deleteModelWriter = connectorService.deleteEntityMatcherModels();

        CompletableFuture<ResponseItems<String>> responseItems = deleteModelWriter
                .writeItemsAsync(RequestParameters.create()
                        .withProjectConfig(projectConfig)
                        .withItems(ImmutableList.of(ImmutableMap.of("modelId", 3231184397505369l))));

        System.out.println("Response body: \r\n" + responseItems.join().getResponseBodyAsString());
        System.out.println("Response items:");
        for (String item : responseItems.join().getResultsItems()) {
            System.out.println("--------------- Item ----------------");
            System.out.println(item);
            System.out.println("-------------------------------------");
        }
        assertTrue(responseItems.join().isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void entityMatcherFit() throws Exception {
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        Connector<String> entityMatchConnector = connectorService.entityMatcherFit();

        RequestParameters entityMatchRequest = RequestParameters.create()
                .withRootParameter("matchFrom",  ImmutableList.of(
                        ImmutableMap.of("id", 1L, "name", "23-DB-9101", "fooField", "bar"),
                        ImmutableMap.of("id", 2L,"name", "23-PC-9101", "barField", "foo"),
                        ImmutableMap.of("id", 3L,"name", "343-Å")
                ))
                .withRootParameter("matchTo", ImmutableList.of(
                        ImmutableMap.of("id", 1L, "externalId", "IA-23_DB_9101"),
                        ImmutableMap.of("id", 2L, "externalId", "VAL_23_PC_9101")
                ))
                .withRootParameter("keysFromTo", ImmutableList.of(
                        ImmutableMap.of("keyFrom", "name", "keyTo", "externalId")
                ))
                .withRootParameter("featureType", "bigram")
                .withProjectConfig(projectConfig);


        CompletableFuture<ResponseItems<String>> responseItems = entityMatchConnector.executeAsync(entityMatchRequest);
        System.out.printf("Response status.\r\n isSuccessful: %1s \r\n status: %2s \r\n",
                responseItems.join().isSuccessful(),
                responseItems.join().getStatus().get(0));
        System.out.println("Response body: \r\n" + responseItems.join().getResponseBodyAsString());
        System.out.println("Response items:");
        for (String item : responseItems.join().getResultsItems()) {
            System.out.println("--------------- Item ----------------");
            System.out.println(item);
            System.out.println("-------------------------------------");
        }
        assertTrue(responseItems.join().isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void entityMatcherPredict() throws Exception {
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ItemReader<String> entityMatchReader = connectorService.entityMatcherPredict();

        RequestParameters entityMatchRequest = RequestParameters.create()
                .withRootParameter("modelId", 4971408434487220L)
                .withRootParameter("matchFrom",  ImmutableList.of(
                        ImmutableMap.of("name", "23-DB-9101", "fooField", "bar"),
                        ImmutableMap.of("name", "23-PC-9101", "barField", "foo"),
                        ImmutableMap.of("name", "343-Å")
                ))
                .withRootParameter("numMatches", 4)
                .withProjectConfig(projectConfig);


        CompletableFuture<ResponseItems<String>> responseItems = entityMatchReader.getItemsAsync(entityMatchRequest);
        System.out.printf("Response status.\r\n isSuccessful: %1s \r\n status: %2s \r\n",
                responseItems.join().isSuccessful(),
                responseItems.join().getStatus().get(0));
        System.out.println("Response body: \r\n" + responseItems.join().getResponseBodyAsString());
        System.out.println("Response items:");
        for (String item : responseItems.join().getResultsItems()) {
            System.out.println("--------------- Item ----------------");
            System.out.println(item);
            System.out.println("-------------------------------------");
        }
        assertTrue(responseItems.join().isSuccessful());
    }

    @Test
    @Tag("remoteCDP")
    void entityMatcherPredictWithMatchTo() throws Exception {
        ConnectorServiceV1 connectorService = ConnectorServiceV1.builder().build();
        ItemReader<String> entityMatchReader = connectorService.entityMatcherPredict();

        RequestParameters entityMatchRequest = RequestParameters.create()
                .withRootParameter("modelId", 4971408434487220L)
                .withRootParameter("matchFrom",  ImmutableList.of(
                        ImmutableMap.of("name", "23-DB-9101", "fooField", "bar"),
                        ImmutableMap.of("name", "23-PC-9101", "barField", "foo"),
                        ImmutableMap.of("name", "343-Å")
                ))
                .withRootParameter("matchTo", ImmutableList.of(
                        ImmutableMap.of("id", 1l, "external_id", "IA-23_DB_9101"),
                        ImmutableMap.of("id", 2l, "external_id", "VAL_23_PC_9101")
                ))
                .withProjectConfig(projectConfig);


        CompletableFuture<ResponseItems<String>> responseItems = entityMatchReader.getItemsAsync(entityMatchRequest);
        System.out.printf("Response status.\r\n isSuccessful: %1s \r\n status: %2s \r\n",
                responseItems.join().isSuccessful(),
                responseItems.join().getStatus().get(0));
        System.out.println("Response body: \r\n" + responseItems.join().getResponseBodyAsString());
        System.out.println("Response items:");
        for (String item : responseItems.join().getResultsItems()) {
            System.out.println("--------------- Item ----------------");
            System.out.println(item);
            System.out.println("-------------------------------------");
        }
        assertTrue(responseItems.join().isSuccessful());
    }
}
