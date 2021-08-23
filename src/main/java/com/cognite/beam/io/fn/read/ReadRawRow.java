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

import java.util.Iterator;
import java.util.List;

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.dto.RawRow;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;

import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.Hints;

/**
 * This function reads a set of rows from raw based on the input RequestParameters.
 *
 */
public class ReadRawRow extends ListItemsBaseFn<RawRow> {
    public ReadRawRow(Hints hints,
                      ReaderConfig readerConfig,
                      PCollectionView<List<ProjectConfig>> projectConfigView) {
        super(hints, readerConfig, projectConfigView);
    }

    @Override
    protected Iterator<List<RawRow>> listItems(CogniteClient client,
                                               RequestParameters requestParameters,
                                               String... partitions) throws Exception {
        Preconditions.checkArgument(requestParameters.getRequestParameters().containsKey("dbName")
                        && requestParameters.getRequestParameters().get("dbName") instanceof String,
                "Request parameters must include dnName with a string value");
        Preconditions.checkArgument(requestParameters.getRequestParameters().containsKey("tableName")
                        && requestParameters.getRequestParameters().get("tableName") instanceof String,
                "Request parameters must include tableName");

        String dbName = (String) requestParameters.getRequestParameters().get("dbName");
        String tableName = (String) requestParameters.getRequestParameters().get("tableName");

        return client.raw().rows().list(dbName, tableName, requestParameters.getRequest(), partitions);
    }

    @Override
    protected long getTimestamp(RawRow item) {
        return item.getLastUpdatedTime();
    }
}
