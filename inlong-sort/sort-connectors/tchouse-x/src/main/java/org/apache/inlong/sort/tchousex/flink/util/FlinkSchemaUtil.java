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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.inlong.sort.tchousex.flink.catalog.Field;
import org.apache.inlong.sort.tchousex.flink.catalog.Table;
import org.apache.inlong.sort.tchousex.flink.catalog.TableIdentifier;
import org.apache.inlong.sort.tchousex.flink.type.TCHouseXDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FlinkSchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSchemaUtil.class);

    /**
     * A utility function used to create fieldGetters
     *
     * @param logicalTypes the logical type
     * @return the fieldGetter[] created
     */
    public static FieldGetter[] createFieldGetters(LogicalType[] logicalTypes) {
        FieldGetter[] fieldGetters = new FieldGetter[logicalTypes.length];
        for (int i = 0; i < logicalTypes.length; i++) {
            if (logicalTypes[i].toString().equalsIgnoreCase(TCHouseXDataType.DATE.name())) {
                int finalI = i;
                fieldGetters[i] = row -> {
                    if (row.isNullAt(finalI)) {
                        return null;
                    }
                    return epochToDate(row.getInt(finalI));
                };
            } else if (logicalTypes[i] instanceof TimeType) {
                int finalI = i;
                fieldGetters[i] = row -> {
                    if (row.isNullAt(finalI)) {
                        return null;
                    }
                    return epochToTime(row.getInt(finalI));
                };
            } else {
                fieldGetters[i] = RowData.createFieldGetter(logicalTypes[i], i);
            }
        }
        return fieldGetters;
    }

    /**
     * A utility function used to create fieldGetters
     *
     * @param logicalTypeList the logical type
     * @return the fieldGetter[] created
     */
    public static FieldGetter[] createFieldGetters(List<LogicalType> logicalTypeList) {
        return createFieldGetters(logicalTypeList.toArray(new LogicalType[0]));
    }

    /**
     * A utility used to change epoch dates into normal dates
     * <p/>
     * Example input: 0
     * Example output: 1970-01-01
     *
     * @param obj the epoch date that is either long or int
     * @return the transformed local date
     */
    public static LocalDate epochToDate(Object obj) {
        if (obj instanceof Long) {
            return LocalDate.ofEpochDay((Long) obj);
        }
        if (obj instanceof Integer) {
            return LocalDate.ofEpochDay((Integer) obj);
        }
        throw new IllegalArgumentException(
                "Convert to LocalDate failed from unexpected value '" + obj + "' of type " + obj.getClass().getName());
    }

    /**
     * A utility used to change epoch dates into normal dates
     * <p/>
     * Example input: 0
     * Example output: 1970-01-01
     *
     * @param obj the epoch date that is either long or int
     * @return the transformed local date
     */
    public static LocalTime epochToTime(Object obj) {
        if (obj instanceof Long) {
            return LocalTime.ofSecondOfDay((Long) obj / 1000);
        }
        if (obj instanceof Integer) {
            return LocalTime.ofSecondOfDay((Integer) obj / 1000);
        }
        throw new IllegalArgumentException(
                "Convert to LocalTime failed from unexpected value '" + obj + "' of type " + obj.getClass().getName());
    }

    public static String convertTypeFromLogicalToDataType(LogicalType type) {
        if (type instanceof BooleanType) {
            return TCHouseXDataType.BOOLEAN.getFormat();
        } else if (type instanceof TinyIntType) {
            return TCHouseXDataType.TINYINT.getFormat();
        } else if (type instanceof IntType) {
            return TCHouseXDataType.INT.getFormat();
        } else if (type instanceof SmallIntType) {
            return TCHouseXDataType.SMALLINT.getFormat();
        } else if (type instanceof BigIntType) {
            return TCHouseXDataType.BIGINT.getFormat();
        } else if (type instanceof FloatType) {
            return TCHouseXDataType.FLOAT.getFormat();
        } else if (type instanceof DoubleType) {
            return TCHouseXDataType.DOUBLE.getFormat();
        } else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return String.format(TCHouseXDataType.DECIMAL.getFormat(), decimalType.getPrecision(),
                    decimalType.getScale());
        } else if (type instanceof DateType) {
            return TCHouseXDataType.DATE.getFormat();
        } else if (type instanceof TimeType) {
            return TCHouseXDataType.STRING.getFormat();
        } else if (type instanceof TimestampType) {
            return TCHouseXDataType.TIMESTAMP.getFormat();
        } else if (type instanceof CharType) {
            CharType charType = (CharType) type;
            return String.format(TCHouseXDataType.CHAR.getFormat(), charType.getLength());
        } else if (type instanceof VarCharType) {
            VarCharType varCharType = (VarCharType) type;
            if (varCharType.getLength() == Integer.MAX_VALUE) {
                return TCHouseXDataType.STRING.getFormat();
            } else {
                return String.format(TCHouseXDataType.VARCHAR.getFormat(), varCharType.getLength());
            }
        } else if (type instanceof LocalZonedTimestampType) {
            return TCHouseXDataType.TIMESTAMP.getFormat();
        } else {
            return TCHouseXDataType.STRING.getFormat();
        }
    }

    /**
     * get table info by parse row type
     * @param rowType
     * @param tableIdentifier
     * @return
     */
    public static Table getTableByParseRowType(RowType rowType, TableIdentifier tableIdentifier,
            List<String> pkListStr) {
        Table table = new Table();
        table.setTableIdentifier(tableIdentifier);
        List<Field> normalFields = new ArrayList<>();
        table.setNormalFields(normalFields);
        List<Field> primaryKeyFields = new ArrayList<>();
        table.setPrimaryKeyFields(primaryKeyFields);
        Map<String, Field> nameMapField = new HashMap<>();

        for (RowField rowField : rowType.getFields()) {
            Field field = new Field(rowField.getName(), convertTypeFromLogicalToDataType(rowField.getType()));
            normalFields.add(field);
            nameMapField.put(rowField.getName(), field);
        }
        for (String pkField : pkListStr) {
            primaryKeyFields.add(nameMapField.get(pkField));
        }
        return table;
    }

    /**
     * compare table info is same
     * @param tableFromSource
     * @param existTable
     * @return
     */
    public static boolean tableIsSame(Table tableFromSource, Table existTable) {
        if (existTable == null || tableFromSource == null) {
            LOG.warn("existTable is null or tableFromSource is null, please check you open auto create table");
            return false;
        }
        if (tableFromSource.getAllFields().size() != existTable.getAllFields().size()) {
            LOG.warn("table field num is different, tableFromSource:{}\n, existTable:{}",
                    tableFromSource.getAllFields(), existTable.getAllFields());
            return false;
        }
        Map<String, Field> tableFromSourceMap =
                tableFromSource.getAllFields().stream()
                        .collect(Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), Function.identity()));
        for (int i = 0; i < existTable.getAllFields().size(); i++) {
            Field fieldForExistTable = existTable.getAllFields().get(i);
            Field fieldFromSource = tableFromSourceMap.get(fieldForExistTable.getName());
            if (fieldFromSource != null && fieldFromSource.getType().contains(TCHouseXDataType.TINYINT.getFormat()) &&
                    fieldForExistTable.getType().contains(TCHouseXDataType.BOOLEAN.getFormat())) {
                LOG.info("tinyint(1) and boolean process as equal");
                continue;
            }
            if (!fieldForExistTable.equals(fieldFromSource)) {
                LOG.warn("tableFromSource field:{} is different with existTable field:{}", fieldFromSource,
                        fieldForExistTable);
                return false;
            }
        }
        existTable.setPrimaryKeyFields(tableFromSource.getPrimaryKeyFields());
        return true;
    }

    public static Map<String, Integer> getFieldNameMapSourcePosition(RowType rowType) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < rowType.getFields().size(); i++) {
            map.put(rowType.getFieldNames().get(i).toLowerCase(Locale.ROOT), i);
        }
        return map;
    }
}
