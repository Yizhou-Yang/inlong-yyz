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

package org.apache.inlong.sort.tchousex.flink.type;

public enum TCHouseXDataType {

    BOOLEAN("boolean"),
    TINYINT("tinyint"),
    SMALLINT("smallint"),
    INT("int"),
    BIGINT("bigint"),
    FLOAT("float"),
    DOUBLE("double"),
    DECIMAL("decimal(%s,%s)"),
    DATE("date"),
    CHAR("char(%s)"),
    VARCHAR("varchar(%s)"),
    STRING("string"),
    TIMESTAMP("timestamp");

    private String format;

    TCHouseXDataType(String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public static TCHouseXDataType fromType(String type) {
        String prefixType = type.split("\\(")[0];
        for (TCHouseXDataType tcHouseXDataType : TCHouseXDataType.values()) {
            if (tcHouseXDataType.name().equalsIgnoreCase(prefixType)) {
                return tcHouseXDataType;
            }
        }
        throw new RuntimeException("unsupported type " + type);
    }

    public static String quoteByType(TCHouseXDataType tCHouseXDataType) {
        switch (tCHouseXDataType) {
            case STRING:
            case CHAR:
            case VARCHAR:
            case TIMESTAMP:
            case DATE:
                return "'";
            default:
                return "";
        }
    }
}
