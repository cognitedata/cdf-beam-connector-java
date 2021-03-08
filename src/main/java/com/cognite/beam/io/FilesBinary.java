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

package com.cognite.beam.io;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.read.RetrieveFileBinariesFn;
import com.cognite.client.dto.FileBinary;
import com.cognite.client.dto.Item;
import com.cognite.beam.io.transform.internal.*;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;
import java.util.List;

import static com.cognite.beam.io.CogniteIO.invalidProjectConfigFile;

public abstract class FilesBinary {
    @AutoValue
    public abstract static class ReadAllById
            extends ConnectorBase<PCollection<Item>, PCollection<FileBinary>> {

        public static FilesBinary.ReadAllById.Builder builder() {
            return new AutoValue_FilesBinary_ReadAllById.Builder()
                    .setProjectConfig(ProjectConfig.create())
                    .setReaderConfig(ReaderConfig.create())
                    .setHints(CogniteIO.defaultHints)
                    .setProjectConfigFile(invalidProjectConfigFile)
                    .setForceTempStorage(false);
        }

        public static ReadAllById create() {
            return ReadAllById.builder().build();
        }

        public abstract ReaderConfig getReaderConfig();
        @Nullable
        public abstract ValueProvider<String> getTempStorageURI();
        public abstract boolean isForceTempStorage();

        public abstract FilesBinary.ReadAllById.Builder toBuilder();

        public FilesBinary.ReadAllById withProjectConfig(ProjectConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setProjectConfig(config).build();
        }

        public ReadAllById withReaderConfig(ReaderConfig config) {
            Preconditions.checkNotNull(config, "Config cannot be null");
            return toBuilder().setReaderConfig(config).build();
        }

        public FilesBinary.ReadAllById withHints(Hints hints) {
            Preconditions.checkNotNull(hints, "Hints cannot be null");
            return toBuilder().setHints(hints).build();
        }

        public FilesBinary.ReadAllById withProjectConfigFile(String file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            Preconditions.checkArgument(!file.isEmpty(), "File cannot be an empty string.");
            return this.withProjectConfigFile(ValueProvider.StaticValueProvider.of(file));
        }

        public FilesBinary.ReadAllById withProjectConfigFile(ValueProvider<String> file) {
            Preconditions.checkNotNull(file, "File cannot be null");
            return toBuilder().setProjectConfigFile(file).build();
        }

        public FilesBinary.ReadAllById withTempStorageURI(ValueProvider<String> tempStorageURI) {
            return toBuilder().setTempStorageURI(tempStorageURI).build();
        }

        public FilesBinary.ReadAllById enableForceTempStorage(boolean forceTempStorage) {
            return toBuilder().setForceTempStorage(forceTempStorage).build();
        }

        @Override
        public PCollection<FileBinary> expand(PCollection<Item> input) {
            // project config side input
            PCollectionView<List<ProjectConfig>> projectConfigView = input.getPipeline()
                    .apply("Build project config", BuildProjectConfig.create()
                            .withProjectConfigFile(getProjectConfigFile())
                            .withProjectConfigParameters(getProjectConfig()))
                    .apply("To list view", View.<ProjectConfig>asList());

            PCollection<FileBinary> outputCollection = input
                    .apply("Shard and batch items", ItemsShardAndBatch.builder()
                            .setMaxBatchSize(4)
                            .setMaxLatency(getHints().getWriteMaxBatchLatency())
                            .setWriteShards(getHints().getWriteShards())
                            .build())
                    .apply("Read file binaries", ParDo.of(new RetrieveFileBinariesFn(getHints(),
                            getReaderConfig(), getTempStorageURI(), isForceTempStorage(), projectConfigView))
                            .withSideInputs(projectConfigView));

            return outputCollection;
        }

        @AutoValue.Builder
        public abstract static class Builder extends ConnectorBase.Builder<FilesBinary.ReadAllById.Builder> {
            abstract ReadAllById.Builder setReaderConfig(ReaderConfig value);
            abstract ReadAllById.Builder setTempStorageURI(ValueProvider<String> tempStorageURI);
            abstract ReadAllById.Builder setForceTempStorage(boolean value);

            public abstract ReadAllById build();
        }
    }
}
