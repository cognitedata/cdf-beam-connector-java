package com.cognite.beam.io.config;

import com.squareup.moshi.FromJson;
import com.squareup.moshi.ToJson;
import org.apache.beam.sdk.options.ValueProvider;

public class ValueProviderAdapter {
    @FromJson
    ValueProvider<String> fromJson(String value) {
        return ValueProvider.StaticValueProvider.of(value);
    }

    @ToJson
    String eventToJson(ValueProvider<String> value) {
        return value.get();
    }
}
