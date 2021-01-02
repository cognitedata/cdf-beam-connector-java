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

package com.cognite.beam.io.config;

public enum UpdateFrequency {
    DAY (0.0000116d),
    HOUR (0.000278d),
    MINUTE (0.0167d),
    SECOND (1d),
    HZ_10 (10d),
    HZ_100 (100d),
    HZ_1000 (1000d);

    private double frequency; // in Hz

    UpdateFrequency(double frequency) {
        this.frequency = frequency;
    }

    public double getFrequency()  {
        return frequency;
    }
}
