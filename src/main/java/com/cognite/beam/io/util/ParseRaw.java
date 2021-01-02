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

package com.cognite.beam.io.util;

import com.google.common.base.Preconditions;
import com.google.protobuf.Value;

/**
 * This class contains various methods for parsing data from Cdf.Raw.
 */
public class ParseRaw {

    /**
     * Tries to parse a Value to a {@code Double}. If the Value has a numeric or string representation the parsing will succeed
     * as long as the Value is within the Double range.
     *
     * Will throw a {@code NumberFormatException} if parsing is unsuccessful.
     * @param rawValue
     * @return
     * @throws NumberFormatException
     */
    public static double parseDouble(Value rawValue) throws NumberFormatException {
        Preconditions.checkNotNull(rawValue, "rawValue cannot be null");
        double returnDouble;
        if (rawValue.getKindCase() == Value.KindCase.NUMBER_VALUE) {
            returnDouble = rawValue.getNumberValue();
        } else if (rawValue.getKindCase() == Value.KindCase.STRING_VALUE) {
            returnDouble = Double.parseDouble(rawValue.getStringValue());
        } else {
            throw new NumberFormatException("Unable to parse to double. "
                    + "Identified value type: " + rawValue.getKindCase()
                    + " Property value: " + rawValue.toString());
        }
        return returnDouble;
    }

    /**
     * Tries to parse a Value to a {@code Boolean}. If the Value has a boolean, numeric or string representation
     * the parsing will succeed.
     *
     * A bool Value representation is parsed directly.
     * A String Value representation returns true if the string argument is not null and equal to, ignoring case, the
     * string "true".
     * A numeric Value representation returns true if the number equals "1".
     *
     *  Will throw an {@code Exception} if parsing is unsuccessful.
     * @param rawValue
     * @return
     * @throws Exception
     */
    public static boolean parseBoolean(Value rawValue) throws Exception {
        Preconditions.checkNotNull(rawValue, "rawValue cannot be null");
        boolean returnBoolean;
        if (rawValue.getKindCase() == Value.KindCase.BOOL_VALUE) {
            returnBoolean = rawValue.getBoolValue();
        } else if (rawValue.getKindCase() == Value.KindCase.NUMBER_VALUE) {
            returnBoolean = Double.compare(1d, rawValue.getNumberValue()) == 0;
        } else if (rawValue.getKindCase() == Value.KindCase.STRING_VALUE) {
            returnBoolean = rawValue.getStringValue().equalsIgnoreCase("true");
        } else {
            throw new Exception("Unable to parse to boolean. "
                    + "Identified value type: " + rawValue.getKindCase()
                    + " Property value: " + rawValue.toString());
        }
        return returnBoolean;
    }
}
