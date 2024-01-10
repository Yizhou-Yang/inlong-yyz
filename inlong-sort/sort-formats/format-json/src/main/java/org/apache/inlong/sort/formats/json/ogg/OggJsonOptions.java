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

package org.apache.inlong.sort.formats.json.ogg;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.formats.json.JsonOptions;

/** Option utils for ogg-json format. */
@PublicEvolving
public class OggJsonOptions {

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            JsonOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_MODE =
            JsonOptions.MAP_NULL_KEY_MODE;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL =
            JsonOptions.MAP_NULL_KEY_LITERAL;

    private OggJsonOptions() {
    }
}
