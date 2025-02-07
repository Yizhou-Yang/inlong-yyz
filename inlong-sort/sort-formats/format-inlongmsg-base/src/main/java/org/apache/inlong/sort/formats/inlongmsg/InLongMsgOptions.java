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

package org.apache.inlong.sort.formats.inlongmsg;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

public class InLongMsgOptions {

    private InLongMsgOptions() {
    }

    public static final ConfigOption<String> INNER_FORMAT =
            ConfigOptions.key("inner.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Defines the format identifier for encoding attr data. \n"
                            + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\n"
                            + "fields are set to null in case of errors");

    public static final ConfigOption<Boolean> CSV_IGNORE_PARSE_ERRORS =
            ConfigOptions.key("csv.ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Allows the case that real size exceeds the expected size.\n "
                            + "The extra column will be skipped");

    public static final ConfigOption<Boolean> CSV_IGNORE_TRAILING_UNMAPPABLE =
            ConfigOptions.key("csv.ignore-trailing-unmappable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Allows the case that real size exceeds the expected size.\n "
                            + "The extra column will be skipped");

    public static final ConfigOption<Boolean> CSV_INSERT_NULLS_FOR_MISSING_COLUMNS =
            ConfigOptions.key("csv.insert-nulls-for-missing-columns")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("For missing columns, insert null.");

    public static final ConfigOption<Boolean> CSV_EMPTY_STRING_AS_NULL =
            ConfigOptions.key("csv.empty-string-as-null")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if the string value is empty, make it as null");

    public static void validateDecodingFormatOptions(ReadableConfig config) {
        String innerFormat = config.get(INNER_FORMAT);
        if (innerFormat == null) {
            throw new ValidationException(
                    INNER_FORMAT.key() + " shouldn't be null.");
        }
    }
}
