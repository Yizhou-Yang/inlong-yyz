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

package org.apache.inlong.sort.tchousex.flink.dml;

import org.apache.inlong.sort.tchousex.flink.catalog.Field;
import org.apache.inlong.sort.tchousex.flink.catalog.Table;
import org.apache.inlong.sort.tchousex.flink.type.TCHouseXDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TCHouseXJdbcDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(TCHouseXJdbcDialect.class);

    public static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    public static String getUpdateStatement(List<String> data, Table table) {
        List<String> fieldNames = table.getAllFields().stream().map(Field::getName).collect(Collectors.toList());
        List<String> conditionFields =
                table.getPrimaryKeyFields().stream().map(Field::getName).collect(Collectors.toList());
        Map<String, String> fieldNameMapValue = new HashMap<>();
        String database = table.getTableIdentifier().getDatabase();
        String tableName = table.getTableIdentifier().getTable();
        for (int i = 0; i < fieldNames.size(); i++) {
            fieldNameMapValue.put(fieldNames.get(i), data.get(i));
        }
        Map<String, String> fieldNameMapType = table.getFieldNameMapType();
        Map<String, TCHouseXDataType> fieldNameMapTCHouseType = table.getFieldNameMapTCHouseXType();
        String setClause =
                fieldNames.stream().map(f -> {
                    TCHouseXDataType tcHouseXDataType = fieldNameMapTCHouseType.get(f);
                    String quote =
                            TCHouseXDataType.quoteByType(tcHouseXDataType);
                    String value = quote + fieldNameMapValue.get(f) + quote;
                    if (TCHouseXDataType.VARCHAR == tcHouseXDataType ||
                            TCHouseXDataType.CHAR == tcHouseXDataType) {
                        value = String.format("cast(%s as %s)", value, fieldNameMapType.get(f));
                    }
                    return quoteIdentifier(f) + " = " + value;
                })
                        .collect(Collectors.joining(", "));
        String conditionClause =
                conditionFields.stream().map(f -> {
                    String quote =
                            TCHouseXDataType.quoteByType(fieldNameMapTCHouseType.get(f));
                    return quoteIdentifier(f) + " = " + quote + fieldNameMapValue.get(f) + quote;
                })
                        .collect(Collectors.joining(" AND "));
        return "UPDATE "
                + quoteIdentifier(database)
                + "."
                + quoteIdentifier(tableName)
                + " SET "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    /**
     * Get delete one row statement by condition fields, default not use limit 1, because limit 1 is
     * a sql dialect.
     */
    public static String getDeleteStatement(List<String> data, Table table) {
        List<String> fieldNames = table.getAllFields().stream().map(Field::getName).collect(Collectors.toList());
        List<String> conditionFields = new ArrayList<>();
        if (table.getPrimaryKeyFields() != null && !table.getPrimaryKeyFields().isEmpty()) {
            conditionFields = table.getPrimaryKeyFields().stream().map(Field::getName).collect(Collectors.toList());
        }
        if (conditionFields.isEmpty()) {
            conditionFields = fieldNames;
        }
        Map<String, String> fieldNameMapValue = new HashMap<>();
        String database = table.getTableIdentifier().getDatabase();
        String tableName = table.getTableIdentifier().getTable();
        for (int i = 0; i < fieldNames.size(); i++) {
            fieldNameMapValue.put(fieldNames.get(i), data.get(i));
        }
        Map<String, TCHouseXDataType> fieldNameMapType = table.getFieldNameMapTCHouseXType();
        String conditionClause =
                conditionFields.stream().map(f -> {
                    String quote =
                            TCHouseXDataType.quoteByType(fieldNameMapType.get(f));
                    return quoteIdentifier(f) + " = " + quote + fieldNameMapValue.get(f) + quote;
                })
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteIdentifier(database) + "." + quoteIdentifier(tableName) + " WHERE "
                + conditionClause;
    }
}
