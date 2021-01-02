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

package com.cognite.beam.io.transform.csv;

import com.cognite.beam.io.fn.parse.ParseCsvFileBodyFn;
import com.cognite.beam.io.fn.parse.ParseCsvFileHeaderFn;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Utility transform for reading delimited text files from the specified location and parsing it/them into
 * a collection of <code>Struct</code>. Each row from the file is parsed into a <code>Struct</code>.
 * The <code>Struct</code> represents a set of columns, one column per field/column in the text file. All columns
 * contain string representations of its value--this transform does not try to parse the input into types
 * like date, numerics, booleans, etc.
 *
 * This transform supports reading from a collection of files specified by a file pattern. It will return the
 * results as <code>Struct</code>.
 *
 * All input files must be text files with UTF8 encoding. The default delimiter is semicolon ";".
 */
@AutoValue
public abstract class ReadCsvFile extends PTransform<PBegin, PCollection<Struct>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static ReadCsvFile.Builder builder() {
        return new AutoValue_ReadCsvFile.Builder()
                .setDelimiter(ValueProvider.StaticValueProvider.of(";"))
                .setHeader(ValueProvider.StaticValueProvider.of(""));
    }

    /**
     * Reads from the given file name / pattern.
     * @param fileName
     * @return
     */
    public static ReadCsvFile from(ValueProvider<String> fileName) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadCsvFile.builder()
                .setFilePath(fileName)
                .build();
    }

    /**
     * Reads from the given file name / pattern.
     * @param fileName
     * @return
     */
    public static ReadCsvFile from(String fileName) {
        Preconditions.checkNotNull(fileName, "File name cannot be null");
        return ReadCsvFile.builder()
                .setFilePath(ValueProvider.StaticValueProvider.of(fileName))
                .build();
    }

    abstract ReadCsvFile.Builder toBuilder();
    abstract ValueProvider<String> getFilePath();
    abstract ValueProvider<String> getDelimiter();
    abstract ValueProvider<String> getHeader();

    /**
     * Specifies the delimiter to use to split a row into separate fields.
     *
     * @param delimiter
     * @return
     */
    public ReadCsvFile withDelimiter(ValueProvider<String> delimiter) {
        Preconditions.checkNotNull(delimiter, "Delimiter cannot be null.");
        return toBuilder().setDelimiter(delimiter).build();
    }

    /**
     * Specifies the delimiter to use to split a row into separate fields.
     *
     * @param delimiter
     * @return
     */
    public ReadCsvFile withDelimiter(String delimiter) {
        Preconditions.checkNotNull(delimiter, "Delimiter cannot be null.");
        return withDelimiter(ValueProvider.StaticValueProvider.of(delimiter));
    }

    /**
     * Specifies a custom header. If you set this value to a non-empty string, the reader will read all lines
     * in the file as data.
     *
     * @param header
     * @return
     */
    public ReadCsvFile withHeader(ValueProvider<String> header) {
        Preconditions.checkNotNull(header, "header cannot be null.");
        return toBuilder().setHeader(header).build();
    }

    /**
     * Specifies a custom header. If you set this value to a non-empty string, the reader will read all lines
     * in the file as data.
     *
     * @param header
     * @return
     */
    public ReadCsvFile withHeader(String header) {
        Preconditions.checkNotNull(header, "Header cannot be null.");
        return withHeader(ValueProvider.StaticValueProvider.of(header));
    }

    @Override
    public PCollection<Struct> expand(PBegin input) {
        LOG.info("Starting ReadCsvFile transform.");

        // Read the collection of input files.
        PCollection<FileIO.ReadableFile> readableFilePCollection = input.getPipeline()
                .apply("Find file", FileIO.match()
                        .filepattern(getFilePath())
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
                .apply("Read file metadata", FileIO.readMatches()
                        .withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.SKIP));

        /*
        Parse out the headers from each file. The headers are published as a view to make them available
        to the main transform parsing the data fields.
         */
        PCollectionView<Map<String, List<String>>> headerMap = readableFilePCollection
                .apply("Extract headers", ParDo.of(new ParseCsvFileHeaderFn(getDelimiter(), getHeader())))
                .apply("To map view", View.asMap());

        /*
        Parse the file body.
         */
        PCollection<Struct> outputCollection = readableFilePCollection
                .apply("Read file body", TextIO.readFiles())
                .apply("Parse to struct", ParDo.of(new ParseCsvFileBodyFn(getDelimiter(), headerMap))
                        .withSideInputs(headerMap));

        return outputCollection;
    }

    @AutoValue.Builder
    static abstract class Builder {
        abstract Builder setFilePath(ValueProvider<String> value);
        abstract Builder setDelimiter(ValueProvider<String> value);
        abstract Builder setHeader(ValueProvider<String> value);

        public abstract ReadCsvFile build();
    }
}
