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

package org.apache.inlong.sort.tchousex.flink.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaFormatUtil {

    private static final Pattern DIALECT_SQL_TYPE_PATTERN = Pattern.compile("([\\w, \\s]+)\\(([\\d,\\s,'\\-']*)\\)");

    public static final int DEFAULT_DECIMAL_PRECISION = 38;
    public static final int DEFAULT_DECIMAL_SCALE = 5;
    public static final int DEFAULT_CHAR_LENGTH = 255;

    public static JsonDynamicSchemaFormat getSchemaFormat(String dynamicSchemaFormat) {
        JsonDynamicSchemaFormat jsonDynamicSchemaFormat =
                (JsonDynamicSchemaFormat) DynamicSchemaFormatFactory.getFormat(dynamicSchemaFormat);
        jsonDynamicSchemaFormat.setHandleDialectSqlTypeFunction((type, dialectType) -> {
            if (StringUtils.isBlank(dialectType)) {
                return type;
            }
            Matcher matcher = DIALECT_SQL_TYPE_PATTERN.matcher(dialectType);
            if (!matcher.matches()) {
                return type;
            }
            String[] items = matcher.group(2).split(",");
            if (type instanceof DecimalType) {
                int precision;
                int scale = DEFAULT_DECIMAL_SCALE;
                precision = Integer.parseInt(items[0].trim());
                if (precision < DecimalType.MIN_PRECISION || precision > DecimalType.MAX_PRECISION) {
                    precision = DEFAULT_DECIMAL_PRECISION;
                }
                if (items.length == 2) {
                    scale = Integer.parseInt(items[1].trim());
                    if (scale < DecimalType.MIN_SCALE || scale > precision) {
                        scale = DEFAULT_DECIMAL_SCALE;
                    }
                }
                return new DecimalType(precision, scale);
            } else if (matcher.group(1).equalsIgnoreCase("SET")) {
                return new VarCharType(Integer.MAX_VALUE);
            } else if (matcher.group(1).equalsIgnoreCase("ENUM")) {
                return new VarCharType(Integer.MAX_VALUE);
            } else if (type instanceof CharType) {
                int length = Integer.parseInt(items[0].trim());
                if (length <= 0) {
                    length = DEFAULT_CHAR_LENGTH;
                }
                length = length * 4;
                if (length > 255) {
                    return new VarCharType(Integer.MAX_VALUE);
                } else {
                    return new CharType(length);
                }
            } else if (type instanceof VarCharType) {
                int length = Integer.parseInt(items[0].trim());
                if (length <= 0) {
                    return new VarCharType(Integer.MAX_VALUE);
                }
                length = length * 4;
                if (length > 65535) {
                    return new VarCharType(Integer.MAX_VALUE);
                } else {
                    return new VarCharType(length);
                }
            } else if (type instanceof VarBinaryType) {
                return new VarCharType(Integer.MAX_VALUE);
            } else if (type instanceof BinaryType) {
                return new VarCharType(Integer.MAX_VALUE);
            } else if (matcher.group(0).equalsIgnoreCase("TINYINT(1)")) {
                return new TinyIntType();
            } else if (matcher.group(0).equalsIgnoreCase("TINYINT(-1)")) {
                return new TinyIntType();
            } else if (matcher.group(1).equalsIgnoreCase("BIGINT UNSIGNED")) {
                return new DecimalType(20, 0);
            } else if (matcher.group(1).equalsIgnoreCase("BIGINT UNSIGNED ZEROFILL")) {
                return new DecimalType(20, 0);
            } else if (matcher.group(1).equalsIgnoreCase("YEAR")) {
                return new SmallIntType();
            } else {
                return type;
            }
        });
        return jsonDynamicSchemaFormat;
    }

}
