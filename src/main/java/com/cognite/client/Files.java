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

package com.cognite.client;

import com.cognite.beam.io.RequestParameters;
import com.cognite.client.config.ResourceType;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.FileParser;
import com.google.auto.value.AutoValue;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite events api endpoint.
 *
 * It provides methods for reading and writing {@link Event}.
 */
@AutoValue
public abstract class Files extends ApiBase {

    private static Builder builder() {
        return new AutoValue_Files.Builder();
    }

    /**
     * Constructs a new {@link Files} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static Files of(CogniteClient client) {
        return Files.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link FileMetadata} objects that matches the filters set in the {@link RequestParameters}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The assets are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<FileMetadata>> list(RequestParameters requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Event} objects that matches the filters set in the {@link RequestParameters} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<FileMetadata>> list(RequestParameters requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.FILE_HEADER, requestParameters, partitions), this::parseFileMetadata);
    }

    /**
     * Retrieve files by id.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved file headers.
     * @throws Exception
     */
    public List<FileMetadata> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.FILE_HEADER, items).stream()
                .map(this::parseFileMetadata)
                .collect(Collectors.toList());
    }

    /**
     * Performs an item aggregation request to Cognite Data Fusion.
     *
     * The default aggregation is a total item count based on the (optional) filters in the request.
     * Multiple aggregation types are supported. Please refer to the Cognite API specification for more information
     * on the possible settings.
     *
     * @param requestParameters The filtering and aggregates specification
     * @return The aggregation results.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    public Aggregate aggregate(RequestParameters requestParameters) throws Exception {
        return aggregate(ResourceType.FILE_HEADER, requestParameters);
    }

    /**
     * Creates or updates a set of {@link FileMetadata} objects.
     *
     * If it is a new {@link FileMetadata} object (based on {@code id / externalId}, then it will be created.
     *
     * If an {@link FileMetadata} object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param fileMetadata The file headers / metadata to upsert.
     * @return The upserted file headers.
     * @throws Exception
     */
    public List<FileMetadata> upsert(List<FileMetadata> fileMetadata) throws Exception {
        String loggingPrefix = "upsert() -";
        Instant startInstant = Instant.now();
        if (fileMetadata.isEmpty()) {
            LOG.warn(loggingPrefix + "No items specified in the request. Will skip the read request.");
            return Collections.emptyList();
        }

        ConnectorServiceV1.ItemWriter updateWriter = getClient().getConnectorService().updateFileHeaders();
        ConnectorServiceV1.ItemWriter createWriter = getClient().getConnectorService().writeFileHeaders();

        // todo: implement a file specific version of this one.
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeEvents()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateEvents()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        UpsertItems<FileMetadata> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildProjectConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getFileId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        return upsertItems.upsertViaUpdateAndCreate(fileMetadata).stream()
                .map(this::parseFileMetadata)
                .collect(Collectors.toList());
    }

    /**
     * Uploads a set of file headers and binaries to Cognite Data Fusion.
     *
     * The file binary can either be placed in-memory in the file container (as a byte string)
     * or referenced (by URI) to a blob store.
     *
     * @param files The files to upload.
     * @return The file metadata / headers for the uploaded files.
     * @throws Exception
     */
    public List<FileMetadata> upload(List<FileContainer> files) throws Exception {
        return this.upload(files, false);
    }

    /**
     * Uploads a set of file headers and binaries to Cognite Data Fusion.
     *
     * The file binary can either be placed in-memory in the file container (as a byte string)
     * or referenced (by URI) to a blob store.
     *
     * In case you reference the file by URI, you can choose to automatically remove the file binary
     * from the (URI referenced) blob store after a successful upload to Cognite Data Fusion. This can
     * Be useful in situations where you perform large scala data transfers utilizing a temp backing
     * store.
     *
     * @param files The files to upload.
     * @param deleteTempFile Set to true to remove the URI binary after upload. Set to false to keep the URI binary.
     * @return The file metadata / headers for the uploaded files.
     * @throws Exception
     */
    public List<FileMetadata> upload(List<FileContainer> files, boolean deleteTempFile) throws Exception {
        String loggingPrefix = "upload() -";
        Instant startInstant = Instant.now();
        if (files.isEmpty()) {
            LOG.warn(loggingPrefix + "No items specified in the request. Will skip the read request.");
            return Collections.emptyList();
        }

        ConnectorServiceV1.FileWriter fileWriter = getClient().getConnectorService().writeFileProto()
                .enableDeleteTempFile(deleteTempFile);

        // naive de-duplication based on ids
        Map<Long, FileContainer> internalIdMap = new HashMap<>();
        Map<String, FileContainer> externalIdMap = new HashMap<>();
        for (FileContainer item : files) {
            if (item.getFileMetadata().hasExternalId()) {
                externalIdMap.put(item.getFileMetadata().getExternalId().getValue(), item);
            } else if (item.getFileMetadata().hasId()) {
                internalIdMap.put(item.getFileMetadata().getId().getValue(), item);
            } else {
                String message = loggingPrefix + "File item does not contain id nor externalId: " + item.toString();
                LOG.error(message);
                throw new Exception(message);
            }
        }
        LOG.info(loggingPrefix + "Received {} files to upload.", internalIdMap.size() + externalIdMap.size());

        // Combine into list
        List<FileContainer> fileContainerList = new ArrayList<>();
        fileContainerList.addAll(externalIdMap.values());
        fileContainerList.addAll(internalIdMap.values());

        // Results set container
        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>(10);

        // Write files async
        for (FileContainer file : fileContainerList) {
            CompletableFuture<ResponseItems<String>> future = fileWriter.writeFileAsync(
                    addAuthInfo(RequestParameters.create()
                            .withProtoRequestBody(file))
            );
            resultFutures.add(future);
        }
        LOG.info(loggingPrefix + "Dispatched {} files for upload. Duration: {}",
                fileContainerList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(resultFutures.toArray(
                new CompletableFuture[resultFutures.size()]));
        // Wait until the uber future completes.
        allFutures.join();

        // Collect the response items
        List<String> responseItems = new ArrayList<>();
        for (CompletableFuture<ResponseItems<String>> responseItemsFuture : resultFutures) {
            if (!responseItemsFuture.join().isSuccessful()) {
                // something went wrong with the request
                String message = loggingPrefix + "Failed to upload file to Cognite Data Fusion: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItemsFuture.join().getResultsItems().forEach(result -> responseItems.add(result));
        }

        LOG.info(loggingPrefix + "Completed upload of {} files within a duration of {}.",
                fileContainerList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return responseItems.stream()
                .map(this::parseFileMetadata)
                .collect(Collectors.toList());
    }

    /**
     * Deletes a set of files.
     *
     * The files to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * @param files a list of {@link Item} representing the events (externalId / id) to be deleted
     * @return The deleted events via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> files) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteFiles()
                .withHttpClient(getClient().getHttpClient())
                .withExecutorService(getClient().getExecutorService());

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildProjectConfig());

        return deleteItems.deleteItems(files);
    }



    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private FileMetadata parseFileMetadata(String json) {
        try {
            return FileParser.parseFileMetadata(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(FileMetadata item) {
        try {
            return FileParser.toRequestInsertItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(FileMetadata item) {
        try {
            return FileParser.toRequestUpdateItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(FileMetadata item) {
        try {
            return FileParser.toRequestReplaceItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of file metadata. It will first check for an externalId, second it will check for id.
    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getFileId(FileMetadata item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId().getValue());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId().getValue()));
        } else {
            return Optional.<String>empty();
        }
    }

    /*
    Returns the id of file metadata. It will first check for an externalId, second it will check for id.
    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getFileId(FileBinary item) {
        if (item.getIdTypeCase() == FileBinary.IdTypeCase.EXTERNAL_ID) {
            return Optional.of(item.getExternalId());
        } else if (item.getIdTypeCase() == FileBinary.IdTypeCase.ID) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Files build();
    }
}
