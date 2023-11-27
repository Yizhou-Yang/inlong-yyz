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

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.doris.model.TableSchema;
import org.apache.inlong.sort.protocol.ddl.Column;
import org.apache.inlong.sort.protocol.ddl.enums.PositionType;
import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class OperationHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperationHelper.class);
    private static final String APOSTROPHE = "'";
    private static final String DOUBLE_QUOTES = "\"";
    private final int VARCHAR_MAX_LENGTH = 16383; // The max value is 65533/4;
    private final int CHAR_MAX_LENGTH = 63;
    private final boolean supportDecimalV3;

    private OperationHelper(boolean supportDecimalV3) {
        this.supportDecimalV3 = supportDecimalV3;
    }

    public static OperationHelper of(boolean supportDecimalV3) {
        return new OperationHelper(supportDecimalV3);
    }

    private String handleVarcharType(VarCharType varcharType) {
        // Because the precision definition of varchar by Doris is different from that of MySQL.
        // The precision in MySQL is the number of characters, while Doris is the number of bytes,
        // and Chinese characters occupy 4 bytes, so the precision multiplys by 4 here.
        int length = varcharType.getLength();
        if (length > VARCHAR_MAX_LENGTH) {
            return "STRING";
        }
        length = length * 4;
        return String.format("VARCHAR(%s)", length);
    }

    private String handleCharType(CharType charType) {
        // Because the precision definition of char by Doris is different from that of MySQL.
        // The precision in MySQL is the number of characters, while Doris is the number of bytes,
        // and Chinese characters occupy 4 bytes, so the precision multiplys by 4 here.
        int length = charType.getLength();
        String type = "CHAR";
        if (length > CHAR_MAX_LENGTH) {
            type = "VARCHAR";
        }
        length = length * 4;
        return String.format("%s(%s)", type, length);
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
     * Build the statement of AddColumn
     * @param alterColumns The list of AlterColumn
     * @return A statement of AddColumn
     */
    public String buildModifyColumnStatement(List<AlterColumn> alterColumns) {
        LOGGER.info("starting to handle change column statements");
        Preconditions.checkState(alterColumns != null
                && !alterColumns.isEmpty(), "Alter columns is empty");
        Iterator<AlterColumn> iterator = alterColumns.iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            AlterColumn expression = iterator.next();
            Preconditions.checkNotNull(expression.getNewColumn(), "New column is null");
            Column newColumn = expression.getNewColumn();
            Preconditions.checkState(newColumn.getName() != null && !newColumn.getName().trim().isEmpty(),
                    "The column name is blank");
            LOGGER.info("handling change column statements" + expression);
            // note: can't support change column syntax by combining modify column with a rename, since
            // Alter operation RENAME conflicts with operation SCHEMA_CHANGE

            // MODIFY COLUMN new_name type
            sb.append("MODIFY COLUMN `").append(newColumn.getName()).append("` ")
                    .append(convert2DorisType(newColumn.getJdbcType(),
                            newColumn.isNullable(), newColumn.getDefinition()));

            // TODO: support [KEY | agg_type]

            // [DEFAULT "default_value"]
            if (validDefaultValue(newColumn.getDefaultValue())) {
                sb.append(" DEFAULT ").append(quote(newColumn.getDefaultValue()));
            }

            // [AFTER column_name|FIRST]
            if (newColumn.getPosition() != null && newColumn.getPosition().getPositionType() != null) {
                if (newColumn.getPosition().getPositionType() == PositionType.FIRST) {
                    sb.append(" FIRST");
                } else if (newColumn.getPosition().getPositionType() == PositionType.AFTER) {
                    Preconditions.checkState(newColumn.getPosition().getColumnName() != null
                                    && !newColumn.getPosition().getColumnName().trim().isEmpty(),
                            "The column name of Position is empty");
                    sb.append(" AFTER `").append(newColumn.getPosition().getColumnName()).append("`");
                }
            }

            // TODO: support [FROM rollup_index_name]

            if (iterator.hasNext()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /**
     * Build the statement of RenameColumn
     *
     * @param alterColumns The list of AlterColumn
     * @return A statement of DropColumn
     */
    public String buildRenameColumnStatement(List<AlterColumn> alterColumns) {
        Preconditions.checkState(alterColumns != null
                && !alterColumns.isEmpty(), "Alter columns is empty");
        Iterator<AlterColumn> iterator = alterColumns.iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            AlterColumn expression = iterator.next();
            Preconditions.checkNotNull(expression.getOldColumn(), "Old column is null");
            Column oldColumn = expression.getOldColumn();
            Column newColumn = expression.getNewColumn();
            Preconditions.checkState(newColumn.getName() != null && !newColumn.getName().trim().isEmpty(),
                    "The column name is blank");
            sb.append("RENAME COLUMN `").append(oldColumn.getName()).append("` `").append(newColumn.getName())
                    .append("`");
            if (iterator.hasNext()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /**



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

    private String builComment(String comment) {
        return "'" + comment.replaceAll("'", "\"") + "'";
    }

    public String buildCreateTableStatement(String database, String table, List<String> primaryKeys,
            RowType rowType, String comment, RowType rowTypeFromData) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(database).append("`.`").append(table).append("`(\n");
        RowField[] fields = getRowFields(rowType.getFields(), primaryKeys);
        Map<String, LogicalType> dataTypeMap = new HashMap<>();
        if (rowTypeFromData != null) {
            for (RowField field : rowTypeFromData.getFields()) {
                dataTypeMap.put(field.getName(), field.getType());
            }
        }
        StringJoiner joiner = new StringJoiner(",");
        for (int i = 0; i < fields.length; i++) {
            RowField column = fields[i];
            String columnComment = column.getDescription().orElse(Constants.ADD_COLUMN_COMMENT);
            sb.append("\t`").append(column.getName()).append("` ").append(convert2DorisType(column.getType(),
                    dataTypeMap.get(column.getName())));
            sb.append(" COMMENT ").append(builComment(columnComment));
            if (i != fields.length - 1) {
                sb.append(",\n");
            }
            joiner.add(String.format("`%s`", column.getName()));
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
        sb.append("\nCOMMENT ").append(builComment(comment));
        sb.append("\nDISTRIBUTED BY HASH(").append(keys).append(")");
        // Add light schema change support for it if the version of doris is greater than 1.2.0 or equals 1.2.0
        sb.append("\nPROPERTIES (\n\t\"light_schema_change\" = \"true\"\n)");
        // sb.append("\nPROPERTIES (\n\t\"replication_num\" = \"1\"\n)");
        return sb.toString();
    }

    private RowField[] getRowFields(List<RowField> fields, List<String> primaryKeys) {
        int index = 0;
        RowField[] fieldArray = new RowField[fields.size()];
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            index = primaryKeys.size();
            Map<String, RowField> primaryFields = new HashMap<>(primaryKeys.size());
            for (RowField field : fields) {
                if (primaryKeys.contains(field.getName())) {
                    primaryFields.put(field.getName(), field);
                } else {
                    fieldArray[index++] = field;
                }
            }
            int primaryInex = 0;
            for (String primaryKey : primaryKeys) {
                fieldArray[primaryInex++] = primaryFields.get(primaryKey);
            }
        } else {
            for (RowField field : fields) {
                fieldArray[index++] = field;
            }
        }
        return fieldArray;
    }

    private String convert2DorisType(LogicalType type, LogicalType typeFromData) {
        String dorisType;
        if (type instanceof VarCharType) {
            dorisType = handleVarcharType((VarCharType) type);
        } else if (type instanceof CharType) {
            dorisType = handleCharType((CharType) type);
        } else if (type instanceof TimestampType || type instanceof LocalZonedTimestampType
                || type instanceof ZonedTimestampType) {
            dorisType = "DATETIME";
        } else if (type instanceof TimeType || type instanceof BinaryType || type instanceof VarBinaryType) {
            dorisType = "STRING";
        } else if (type instanceof DecimalType) {
            dorisType = handleDecimalType((DecimalType) type);
        } else {
            dorisType = numberTypeScopeExpand(type, typeFromData);
        }
        return dorisType;
    }

    private String numberTypeScopeExpand(LogicalType originType, LogicalType typeFromData) {
        if (typeFromData == null) {
            return originType.asSummaryString();
        }
        if (originType instanceof IntType && typeFromData instanceof BigIntType) {
            return typeFromData.asSummaryString();
        }
        if (originType instanceof BigIntType && typeFromData instanceof DecimalType) {
            return typeFromData.asSummaryString();
        }
        if (originType instanceof TinyIntType && typeFromData instanceof SmallIntType) {
            return typeFromData.asSummaryString();
        }
        if (originType instanceof SmallIntType && typeFromData instanceof IntType) {
            return typeFromData.asSummaryString();
        }
        return originType.asSummaryString();
    }

    private String handleDecimalType(DecimalType decimalType) {
        if (decimalType.getPrecision() > 38 || decimalType.getScale() > decimalType.getPrecision()) {
            return "STRING";
        }
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();
        String type = "DECIMALV3";
        if (!supportDecimalV3) {
            if (precision > 27 || scale > precision) {
                return "STRING";
            }
            type = "DECIMAL";
        }
        return String.format("%s(%s,%s)", type, precision, scale);
    }

    public String buildAddColumnStatement(List<RowField> addFields, Map<String, String> positionMap, RowType rowType) {
        StringBuilder sb = new StringBuilder();
        Map<String, LogicalType> dataFields = new HashMap<>();
        if (rowType != null) {
            for (RowField field : rowType.getFields()) {
                dataFields.put(field.getName(), field.getType());
            }
        }
        for (RowField field : addFields) {
            String comment = field.getDescription().orElse(Constants.ADD_COLUMN_COMMENT);
            sb.append("ADD COLUMN ")
                    .append("`").append(field.getName()).append("` ")
                    .append(convert2DorisType(field.getType(), dataFields.get(field.getName())))
                    .append(" COMMENT ").append(builComment(comment));
            if (positionMap.containsKey(field.getName())) {
                String afterField = positionMap.get(field.getName());
                if (afterField != null) {
                    sb.append(" AFTER ").append("`").append(afterField).append("`");
                }
            }
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public String buildCreateDatabaseStatement(String database) {
        return String.format("CREATE DATABASE `%s`", database);
    }

    public List<RowField> extractAddFields(TableSchema tableSchema, RowType rowType) {
        List<RowField> addFields = new ArrayList<>();
        if (tableSchema.getProperties() != null && tableSchema.getProperties().size() < rowType.getFieldCount()) {
            // Only support add column auto
            Set<String> columnsOfSchema = tableSchema.getProperties().stream().map(TableSchema.Column::getName)
                    .collect(Collectors.toSet());
            for (RowField field : rowType.getFields()) {
                if (!columnsOfSchema.contains(field.getName())) {
                    addFields.add(field);
                }
            }
        }
        return addFields;
    }
}
