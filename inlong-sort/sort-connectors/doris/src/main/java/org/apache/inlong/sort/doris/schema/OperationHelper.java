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

package org.apache.inlong.sort.doris.schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.doris.model.TableSchema;
import org.apache.inlong.sort.protocol.ddl.Column;
import org.apache.inlong.sort.protocol.ddl.enums.PositionType;
import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.protocol.ddl.operations.CreateTableOperation;

import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class OperationHelper {

    private static final String APOSTROPHE = "'";
    private static final String DOUBLE_QUOTES = "\"";
    private final JsonDynamicSchemaFormat dynamicSchemaFormat;
    private final int VARCHAR_MAX_LENGTH = 65533;
    private final int CHAR_MAX_LENGTH = 255;
    private final boolean supportDecimalV3;

    private OperationHelper(JsonDynamicSchemaFormat dynamicSchemaFormat, boolean supportDecimalV3) {
        this.dynamicSchemaFormat = dynamicSchemaFormat;
        this.supportDecimalV3 = supportDecimalV3;
    }

    public static OperationHelper of(JsonDynamicSchemaFormat dynamicSchemaFormat, boolean supportDecimalV3) {
        return new OperationHelper(dynamicSchemaFormat, supportDecimalV3);
    }

    private String convert2DorisType(int jdbcType, boolean isNullable, List<String> precisions) {
        String type = null;
        switch (jdbcType) {
            case Types.BOOLEAN:
            case Types.DATE:
            case Types.FLOAT:
            case Types.DOUBLE:
                type = dynamicSchemaFormat.sqlType2FlinkType(jdbcType).copy(isNullable).asSummaryString();
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                if (precisions != null && !precisions.isEmpty()) {
                    type = String.format("%s(%s)%s", dynamicSchemaFormat.sqlType2FlinkType(jdbcType).asSummaryString(),
                            StringUtils.join(precisions, ","), isNullable ? "" : " NOT NULL");
                } else {
                    type = dynamicSchemaFormat.sqlType2FlinkType(jdbcType).copy(isNullable).asSummaryString();
                }
                break;
            case Types.DECIMAL:
                DecimalType decimalType = (DecimalType) dynamicSchemaFormat.sqlType2FlinkType(jdbcType);
                if (precisions != null && !precisions.isEmpty()) {
                    Preconditions.checkState(precisions.size() < 3,
                            "The length of precisions with DECIMAL must small than 3");
                    int precision = Integer.parseInt(precisions.get(0));
                    int scale = JsonDynamicSchemaFormat.DEFAULT_DECIMAL_SCALE;
                    if (precisions.size() == 2) {
                        scale = Integer.parseInt(precisions.get(1));
                    }
                    decimalType = new DecimalType(isNullable, precision, scale);
                } else {
                    decimalType = new DecimalType(isNullable, decimalType.getPrecision(), decimalType.getScale());
                }
                type = handleDecimalType(decimalType);
                break;
            case Types.CHAR:
                LogicalType charType = dynamicSchemaFormat.sqlType2FlinkType(jdbcType);
                if (precisions != null && !precisions.isEmpty()) {
                    Preconditions.checkState(precisions.size() == 1,
                            "The length of precisions with CHAR must be 1");
                    charType = new CharType(isNullable, Integer.parseInt(precisions.get(0)));
                } else {
                    charType = charType.copy(isNullable);
                }
                type = charType.asSerializableString();
                break;
            case Types.VARCHAR:
                LogicalType varcharType = dynamicSchemaFormat.sqlType2FlinkType(jdbcType);
                if (precisions != null && !precisions.isEmpty()) {
                    Preconditions.checkState(precisions.size() == 1,
                            "The length of precisions with VARCHAR must be 1");
                    // Because the precision definition of varchar by Doris is different from that of MySQL.
                    // The precision in MySQL is the number of characters, while Doris is the number of bytes,
                    // and Chinese characters occupy 4 bytes, so the precision multiplys by 3 here.
                    int precision = Math.min(Integer.parseInt(precisions.get(0)) * 4, VARCHAR_MAX_LENGTH);
                    varcharType = new VarCharType(isNullable, precision);
                } else {
                    varcharType = varcharType.copy(isNullable);
                }
                type = varcharType.asSerializableString();
                break;
            // The following types are not directly supported in doris,
            // and can only be converted to compatible types as much as possible
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.LONGNVARCHAR:
            case Types.LONGVARBINARY:
            case Types.LONGVARCHAR:
            case Types.ARRAY:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.OTHER:
                type = String.format("STRING%s", isNullable ? "" : " NOT NULL");
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.TIMESTAMP:
                type = "DATETIME";
                break;
            case Types.REAL:
            case Types.NUMERIC:
                int precision = JsonDynamicSchemaFormat.DEFAULT_DECIMAL_PRECISION;
                int scale = JsonDynamicSchemaFormat.DEFAULT_DECIMAL_SCALE;
                if (precisions != null && !precisions.isEmpty()) {
                    Preconditions.checkState(precisions.size() < 3,
                            "The length of precisions with NUMERIC must small than 3");
                    precision = Integer.parseInt(precisions.get(0));
                    if (precisions.size() == 2) {
                        scale = Integer.parseInt(precisions.get(1));
                    }
                }
                decimalType = new DecimalType(isNullable, precision, scale);
                type = handleDecimalType(decimalType);
                break;
            case Types.BIT:
                type = String.format("BOOLEAN %s", isNullable ? "" : " NOT NULL");
                break;
            default:
        }
        return type;
    }

    /**
     * Build the statement of AddColumn
     *
     * @param alterColumns The list of AlterColumn
     * @return A statement of AddColumn
     */
    public String buildAddColumnStatement(List<AlterColumn> alterColumns) {
        Preconditions.checkState(alterColumns != null
                && !alterColumns.isEmpty(), "Alter columns is empty");
        Iterator<AlterColumn> iterator = alterColumns.iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            AlterColumn expression = iterator.next();
            Preconditions.checkNotNull(expression.getNewColumn(), "New column is null");
            Column column = expression.getNewColumn();
            Preconditions.checkState(column.getName() != null && !column.getName().trim().isEmpty(),
                    "The column name is blank");
            sb.append("ADD COLUMN `").append(column.getName()).append("` ")
                    .append(convert2DorisType(expression.getNewColumn().getJdbcType(),
                            column.isNullable(), column.getDefinition()));
            if (validDefaultValue(column.getDefaultValue())) {
                sb.append(" DEFAULT ").append(quote(column.getDefaultValue()));
            }
            if (column.getComment() != null) {
                sb.append(" COMMENT ").append(quote(column.getComment()));
            }
            if (column.getPosition() != null && column.getPosition().getPositionType() != null) {
                if (column.getPosition().getPositionType() == PositionType.FIRST) {
                    sb.append(" FIRST");
                } else if (column.getPosition().getPositionType() == PositionType.AFTER) {
                    Preconditions.checkState(column.getPosition().getColumnName() != null
                            && !column.getPosition().getColumnName().trim().isEmpty(),
                            "The column name of Position is empty");
                    sb.append(" AFTER `").append(column.getPosition().getColumnName()).append("`");
                }
            }
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private String quote(String value) {
        if (value == null) {
            return "'null'";
        }
        if (!value.startsWith(APOSTROPHE) && !value.startsWith(DOUBLE_QUOTES)) {
            return String.format("'%s'", value);
        }
        return value;
    }

    /**
     * Build the statement of DropColumn
     *
     * @param alterColumns The list of AlterColumn
     * @return A statement of DropColumn
     */
    public String buildDropColumnStatement(List<AlterColumn> alterColumns) {
        Preconditions.checkState(alterColumns != null
                && !alterColumns.isEmpty(), "Alter columns is empty");
        Iterator<AlterColumn> iterator = alterColumns.iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            AlterColumn expression = iterator.next();
            Preconditions.checkNotNull(expression.getOldColumn(), "Old column is null");
            Column column = expression.getOldColumn();
            Preconditions.checkState(column.getName() != null && !column.getName().trim().isEmpty(),
                    "The column name is blank");
            sb.append("DROP COLUMN `").append(column.getName()).append("`");
            if (iterator.hasNext()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /**
     * Build common statement of alter
     *
     * @param database The database of Doris
     * @param table The table of Doris
     * @return A statement of Alter table
     */
    public String buildAlterStatementCommon(String database, String table) {
        return "ALTER TABLE `" + database + "`.`" + table + "` ";
    }

    private boolean validDefaultValue(String defaultValue) {
        return defaultValue != null && !defaultValue.trim().isEmpty() && !"NULL"
                .equalsIgnoreCase(defaultValue);
    }

    /**
     * Build the statement of CreateTable
     *
     * @param database The database of Doris
     * @param table The table of Doris
     * @param primaryKeys The primary key of Doris
     * @param operation The Operation
     * @return A statement of CreateTable
     */
    public String buildCreateTableStatement(String database, String table, List<String> primaryKeys,
            CreateTableOperation operation) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(database).append("`.`").append(table).append("`(\n");
        Preconditions.checkState(operation.getColumns() != null && !operation.getColumns().isEmpty(),
                String.format("The columns of table: %s.%s is empty", database, table));
        Iterator<Column> iterator = operation.getColumns().iterator();
        StringJoiner joiner = new StringJoiner(",");
        while (iterator.hasNext()) {
            Column column = iterator.next();
            Preconditions.checkNotNull(column, "The column is null");
            Preconditions.checkState(column.getName() != null && !column.getName().trim().isEmpty(),
                    "The column name is blank");
            sb.append("\t`").append(column.getName()).append("` ").append(convert2DorisType(column.getJdbcType(),
                    column.isNullable(), column.getDefinition()));
            if (validDefaultValue(column.getDefaultValue())) {
                sb.append(" DEFAULT ").append(quote(column.getDefaultValue()));
            }
            if (column.getComment() != null) {
                sb.append(" COMMENT ").append(quote(column.getComment()));
            }
            joiner.add(String.format("`%s`", column.getName()));
            if (iterator.hasNext()) {
                sb.append(",\n");
            }
        }
        sb.append("\n)\n");
        String model = "DUPLICATE";
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            model = "UNIQUE";
            joiner = new StringJoiner(",");
            for (String primaryKey : primaryKeys) {
                joiner.add(String.format("`%s`", primaryKey));
            }
        }
        String keys = joiner.toString();
        sb.append(model).append(" KEY(").append(keys).append(")");
        if (StringUtils.isNotBlank(operation.getComment())) {
            sb.append("\nCOMMENT ").append(quote(operation.getComment()));
        }
        sb.append("\nDISTRIBUTED BY HASH(").append(keys).append(")");
        // Add light schema change support for it if the version of doris is greater than 1.2.0 or equals 1.2.0
        sb.append("\nPROPERTIES (\n\t\"light_schema_change\" = \"true\"\n)");
        return sb.toString();
    }

    public String buildCreateTableStatement(String database, String table, List<String> primaryKeys,
            RowType rowType) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(database).append("`.`").append(table).append("`(\n");
        Iterator<RowField> iterator = rowType.getFields().iterator();
        StringJoiner joiner = new StringJoiner(",");
        while (iterator.hasNext()) {
            RowField column = iterator.next();
            sb.append("\t`").append(column.getName()).append("` ").append(convert2DorisType(column.getType()));
            sb.append(" COMMENT ").append(quote("Add Column Auto"));
            joiner.add(String.format("`%s`", column.getName()));
            if (iterator.hasNext()) {
                sb.append(",\n");
            }
        }
        sb.append("\n)\n");
        String model = "DUPLICATE";
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            model = "UNIQUE";
            joiner = new StringJoiner(",");
            for (String primaryKey : primaryKeys) {
                joiner.add(String.format("`%s`", primaryKey));
            }
        }
        String keys = joiner.toString();
        sb.append(model).append(" KEY(").append(keys).append(")");
        sb.append("\nCOMMENT ").append(quote("Create Table Auto"));
        sb.append("\nDISTRIBUTED BY HASH(").append(keys).append(")");
        // Add light schema change support for it if the version of doris is greater than 1.2.0 or equals 1.2.0
        sb.append("\nPROPERTIES (\n\t\"light_schema_change\" = \"true\"\n)");
        // sb.append("\nPROPERTIES (\n\t\"replication_num\" = \"1\"\n)");
        return sb.toString();
    }

    private String convert2DorisType(LogicalType type) {
        String dorisType;
        if (type instanceof VarCharType) {
            VarCharType varcharType = (VarCharType) type;
            // Because the precision definition of varchar by Doris is different from that of MySQL.
            // The precision in MySQL is the number of characters, while Doris is the number of bytes,
            // and Chinese characters occupy 4 bytes, so the precision multiplys by 4 here.
            int length = Math.min(varcharType.getLength() * 4, VARCHAR_MAX_LENGTH);
            varcharType = new VarCharType(type.isNullable(), length);
            dorisType = varcharType.asSummaryString();
        } else if (type instanceof CharType) {
            CharType charType = (CharType) type;
            int length = Math.min(charType.getLength(), CHAR_MAX_LENGTH);
            if (length != charType.getLength()) {
                charType = new CharType(length);
            }
            dorisType = charType.asSummaryString();
        } else if (type instanceof TimestampType || type instanceof LocalZonedTimestampType
                || type instanceof ZonedTimestampType) {
            dorisType = "DATETIME";
        } else if (type instanceof TimeType || type instanceof BinaryType || type instanceof VarBinaryType) {
            dorisType = String.format("STRING%s", type.isNullable() ? "" : " NOT NULL");
        } else if (type instanceof DecimalType) {
            dorisType = handleDecimalType((DecimalType) type);
        } else {
            dorisType = type.asSummaryString();
        }
        return dorisType;
    }

    private String handleDecimalType(DecimalType decimalType) {
        int precision = Math.min(decimalType.getPrecision(), 38);
        int scale = Math.min(decimalType.getScale(), 38);
        String type = "DECIMALV3";
        if (!supportDecimalV3) {
            precision = Math.min(27, precision);
            scale = Math.min(27, scale);
            type = "DECIMAL";
        }
        return String.format("%s(%s,%s)", type, precision, scale);
    }

    public String buildAddColumnStatement(TableSchema tableSchema, RowType rowType) {
        if (tableSchema.getProperties() != null && tableSchema.getProperties().size() < rowType.getFieldCount()) {
            // Only support add column auto
            Set<String> columnsOfSchema = tableSchema.getProperties().stream().map(TableSchema.Column::getName)
                    .collect(Collectors.toSet());
            RowField preField = null;
            StringBuilder sb = new StringBuilder();
            for (RowField field : rowType.getFields()) {
                if (!columnsOfSchema.contains(field.getName())) {
                    sb.append("ADD COLUMN `").append(field.getName()).append("` ")
                            .append(convert2DorisType(field.getType()));
                    sb.append(" COMMENT ").append(quote("Add Column Auto"));
                    if (preField != null && columnsOfSchema.contains(preField.getName())) {
                        sb.append(" AFTER `").append(preField.getName()).append("`");
                    }
                    sb.append(",");
                }
                preField = field;
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.lastIndexOf(","));
                return sb.toString();
            } else {
                return null;
            }
        }
        return null;
    }

    public String buildCreateDatabaseStatement(String database) {
        return String.format("CREATE DATABASE `%s`", database);
    }
}
