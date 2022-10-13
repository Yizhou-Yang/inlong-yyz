/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.base.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Abstact dynamic format class
 * This class main handle:
 * 1. deserialize data from byte array to get raw data
 * 2. parse pattern and get the real value from the raw data
 * Such as:
 * 1). give a pattern "${a}{b}{c}" and the raw data contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be '123'
 * 2). give a pattern "${a}_{b}_{c}" and the raw data contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be '1_2_3'
 * 3). give a pattern "prefix_${a}_{b}_{c}_suffix" and the raw Node contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be 'prefix_1_2_3_suffix'
 */
public abstract class AbstractDynamicSchemaFormat<T> {

    public static final Pattern PATTERN = Pattern.compile("\\$\\{\\s*([\\w.-]+)\\s*}", Pattern.CASE_INSENSITIVE);

    /**
     * Extract value by key from the raw data
     *
     * @param message The byte array of raw data
     * @param keys The key list that will be used to extract
     * @return The value list maps the keys
     * @throws IOException The exceptions may throws when extract
     */
    public List<String> extract(byte[] message, String... keys) throws IOException {
        if (keys == null || keys.length == 0) {
            return new ArrayList<>();
        }
        final T data = deserialize(message);
        List<String> values = new ArrayList<>(keys.length);
        for (String key : keys) {
            values.add(extract(data, key));
        }
        return values;
    }

    /**
     * Extract value by key from the raw data
     *
     * @param data The raw data
     * @param key The key that will be used to extract
     * @return The value maps the key in the raw data
     */
    public abstract String extract(T data, String key);

    /**
     * Deserialize from byte array
     *
     * @param message The byte array of raw data
     * @return The raw data T
     * @throws IOException The exceptions may throws when deserialize
     */
    public abstract T deserialize(byte[] message) throws IOException;

    /**
     * Parse msg and replace the value by key from meta data and physical data.
     * See details {@link AbstractDynamicSchemaFormat#parse(T, String)}
     *
     * @param message The source of data rows format by bytes
     * @param pattern The pattern value
     * @return The result of parsed
     * @throws IOException The exception that will throws
     */
    public String parse(byte[] message, String pattern) throws IOException {
        return parse(deserialize(message), pattern);
    }

    /**
     * Parse msg and replace the value by key from the raw data
     * Such as:
     * 1. give a pattern "${a}{b}{c}" and the data contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be '123'
     * 2. give a pattern "${a}_{b}_{c}" and the data contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be '1_2_3'
     * 3. give a pattern "prefix_${a}_{b}_{c}_suffix" and the data contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be 'prefix_1_2_3_suffix'
     *
     * @param data The raw data
     * @param pattern The pattern value
     * @return The result of parsed
     * @throws IOException The exception will throws
     */
    public abstract String parse(T data, String pattern) throws IOException;

    /**
     * Get the identifier of this dynamic schema format
     *
     * @return The identifier of this dynamic schema format
     */
    public abstract String identifier();
}
