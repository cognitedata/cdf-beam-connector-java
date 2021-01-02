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

package com.cognite.beam.io.fn.parse;

import com.cognite.beam.io.servicesV1.util.CharSplitter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

/**
 * Reads and parses the header from a delimited text file.
 *
 * Returns the header as a list of strings keyed to the full file path/URL.
 */
public class ParseCsvFileHeaderFn extends DoFn<FileIO.ReadableFile, KV<String, List<String>>> {
    private final static CharSplitter splitter = new CharSplitter();
    private final static char BOM_CHAR = '\uFEFF'; // Unicode char representing the BOM
    private final static List<Character> lineDelimiters = ImmutableList.of('\r', '\n');

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ValueProvider<String> delimiter;
    private final ValueProvider<String> customHeader;

    public ParseCsvFileHeaderFn(ValueProvider<String> delimiter, ValueProvider<String> customHeader) {
        this.delimiter = delimiter;
        this.customHeader = customHeader;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up ParseCsvFileHeaderFn.");
        Preconditions.checkState(delimiter.isAccessible(),
                "Delimiter is not accessible from the ValueProvider.");
    }

    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile file,
                               OutputReceiver<KV<String, List<String>>> out) throws Exception {
        String headerLine = "";
        if (customHeader.get().isEmpty()) {
            LOG.info("No custom header set. Reading header from file.");
            headerLine = readHeaderFromFile(file);
        } else {
            LOG.info("Custom header specified. Will not read header from file.");
            headerLine = customHeader.get();
        }

        LOG.info("Start of header: {}", headerLine.substring(0, Math.min(60, headerLine.length()) -1));
        LOG.debug("Complete header: {}", headerLine);

        // Break the header up into the individual components.
        Collection<String> headerComponents = splitter.split(delimiter.get().charAt(0), headerLine);
        LOG.info("Header split into {} individual columns", headerComponents.size());
        LOG.debug("Header components: {}", ImmutableList.copyOf(headerComponents).toString());

        out.output(KV.of(file.toString(), ImmutableList.copyOf(headerComponents)));
    }

    private String readHeaderFromFile(FileIO.ReadableFile file) throws Exception {
        LOG.info("Received readable file: {}", file.toString());
        String headerLine = "";
        StringBuilder stringBuilder = new StringBuilder();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
        ReadableByteChannel byteChannel = file.open();
        boolean lineDelimiter = false;

        // Read the first 1MB from the file
        byteChannel.read(byteBuffer);
        byteBuffer.flip();
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);

        // Check for BOM (byte order mark), and remove it if present
        if (charBuffer.hasRemaining()) {
            if (BOM_CHAR == charBuffer.get()) {
                LOG.info("Byte Order Mark detected. Will remove it.");
                charBuffer.compact();
                charBuffer.flip();
            } else {
                LOG.info("No byte order mark detected.");
                charBuffer.rewind();
            }
        }

        // Iterate through the character stream until we hit the first line delimiter
        char character;
        while (charBuffer.hasRemaining() && !lineDelimiter) {
            character = charBuffer.get();
            if (lineDelimiters.contains(character)) {
                lineDelimiter = true;
            } else {
                stringBuilder.append(character);
            }
        }
        headerLine = stringBuilder.toString();
        LOG.info("Finished reading header from [{}]. The header is {} characters long", file.toString(), headerLine.length());
        return headerLine;
    }
}
