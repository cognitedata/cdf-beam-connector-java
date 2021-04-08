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

package com.cognite.beam.io.fn.write;

import com.cognite.client.dto.FileBinary;
import com.cognite.client.dto.FileContainer;
import com.cognite.client.dto.FileMetadata;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Deletes the temporary binary from blob / file store.
 *
 */
public class RemoveTempFile extends DoFn<KV<Iterable<FileContainer>, Iterable<FileMetadata>>, FileMetadata> {
    private final static Logger LOG = LoggerFactory.getLogger(RemoveTempFile.class);

    private Storage cloudStorage = null;
    final boolean deleteTempFile;

    public RemoveTempFile(boolean deleteTempFile) {
        this.deleteTempFile = deleteTempFile;
    }

    /**
     * Deletes the temporary binary from blob / file store.
     *
     * @param items
     * @param outputReceiver
     * @throws Exception
     */
    @ProcessElement
    public void processElement(@Element KV<Iterable<FileContainer>, Iterable<FileMetadata>> items,
                               OutputReceiver<FileMetadata> outputReceiver) throws Exception {
        final String batchLogPrefix = "Batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";

        if (deleteTempFile) {
            for (FileContainer container : items.getKey()) {
                if (container.hasFileBinary()
                        && container.getFileBinary().getBinaryTypeCase() == FileBinary.BinaryTypeCase.BINARY_URI) {
                    // We have a temp file binary, so let's remove it
                    try {
                        LOG.debug(batchLogPrefix + "Start delete the temp file binary {}",
                                container.getFileBinary().getBinaryUri());
                        deleteTempFile(new URI(container.getFileBinary().getBinaryUri()));
                        LOG.debug(batchLogPrefix + "Successfully deleted the temp file binary {}",
                                container.getFileBinary().getBinaryUri());
                    } catch (Exception e) {
                        LOG.error(batchLogPrefix + "Error when deleting temp file binary: {}",
                                e.toString());
                        throw new Exception(batchLogPrefix + "Error when deleting temp file binary.", e);
                    }
                }
            }
        }

        items.getValue().forEach(item -> outputReceiver.output(item));
    }

    private void deleteTempFile(@NotNull URI fileURI) throws Exception {
        if (null != fileURI.getScheme() && fileURI.getScheme().equalsIgnoreCase("gs")) {
            Blob blob = getBlob(fileURI);
            if (null == blob) {
                LOG.error("Looks like the GCS blob is null/does not exist. File URI: {}",
                        fileURI.toString());
                throw new IOException(String.format("Looks like the GCS blob is null/does not exist. File URI: %s",
                        fileURI.toString()));
            }
            blob.delete();
        } else if (null != fileURI.getScheme() && fileURI.getScheme().equalsIgnoreCase("file")) {
            // Handler for local (or network) based file system blobs
            if (!Files.isReadable(Paths.get(fileURI)) || !Files.isRegularFile(Paths.get(fileURI))) {
                throw new IOException("Temp file is not a readable file: " + fileURI.toString());
            }
            Files.delete(Paths.get(fileURI));
        } else {
            throw new IOException("URI is unsupported: " + fileURI.toString());
        }
    }

    /*
        Gets the GCS blob from a GCS URI. Can be used to access blob metadata and start upload/download.
     */
    private Blob getBlob(URI fileURI) throws IOException {
        if (!fileURI.getScheme().equalsIgnoreCase("gs")) {
            throw new IOException("URI is not a valid GCS URI: " + fileURI.toString());
        }

        String bucketName = fileURI.getHost();
        String path = fileURI.getPath();
        if (path.startsWith("/")) path = path.substring(1); // make sure the path does not start with a forward slash

        BlobId blobId = BlobId.of(bucketName, path);
        return getCloudStorage().get(blobId);
    }

    /*
    Ensure a single instantiation of the GCS service.
     */
    private Storage getCloudStorage() {
        if (null == cloudStorage) {
            cloudStorage = StorageOptions.getDefaultInstance().getService();
        }
        return cloudStorage;
    }
}
