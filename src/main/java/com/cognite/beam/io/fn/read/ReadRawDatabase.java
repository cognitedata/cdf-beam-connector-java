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

package com.cognite.beam.io.fn.read;

import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.fn.IOBaseFn;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

/**
 * This function reads all database names from raw.
 *
 */
public class ReadRawDatabase extends IOBaseFn<ProjectConfig, String> {
    private final static Logger LOG = LoggerFactory.getLogger(ReadRawDatabase.class);

    private final ReaderConfig readerConfig;

    public ReadRawDatabase(Hints hints, ReaderConfig readerConfig) {
        super(hints);
        Preconditions.checkNotNull(readerConfig, "Reader config cannot be null.");
        this.readerConfig = readerConfig;
    }

    @ProcessElement
    public void processElement(@Element ProjectConfig config,
                               OutputReceiver<String> outputReceiver) throws Exception {
        final String batchLogPrefix = "Batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        final Instant batchStartInstant = Instant.now();

        try {
            Iterator<List<String>> resultsIterator = getClient(config, readerConfig).raw().databases().list();
            Instant pageStartInstant = Instant.now();
            int totalNoItems = 0;
            while (resultsIterator.hasNext()) {
                List<String> results = resultsIterator.next();
                if (readerConfig.isMetricsEnabled()) {
                    apiBatchSize.update(results.size());
                    apiLatency.update(Duration.between(pageStartInstant, Instant.now()).toMillis());
                }

                results.forEach(item -> outputReceiver.output(item));

                totalNoItems += results.size();
                pageStartInstant = Instant.now();
            }

            LOG.info(batchLogPrefix + "Retrieved {} items in {}}.",
                    totalNoItems,
                    Duration.between(batchStartInstant, Instant.now()).toString());
        } catch (Exception e) {
            LOG.error(batchLogPrefix + "Error reading results from the Cognite connector.", e);
            throw e;
        }
    }
}
