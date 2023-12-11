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

package org.apache.inlong.sort.base.schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.metric.sub.SinkTableMetricData;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.protocol.ddl.Column;
import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.protocol.ddl.operations.AlterOperation;
import org.apache.inlong.sort.protocol.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.protocol.ddl.operations.Operation;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.apache.inlong.sort.util.SchemaChangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SchemaChangeHelper implements SchemaChangeHandle {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChangeHelper.class);
    private final boolean schemaChange;
    protected final Map<SchemaChangeType, SchemaChangePolicy> policyMap;
    protected final JsonDynamicSchemaFormat dynamicSchemaFormat;
    private final String databasePattern;
    private final String tablePattern;
    private final String schemaPattern;
    protected final SchemaUpdateExceptionPolicy exceptionPolicy;
    private final SinkTableMetricData metricData;
    private final DirtySinkHelper<Object> dirtySinkHelper;

    public SchemaChangeHelper(JsonDynamicSchemaFormat dynamicSchemaFormat, boolean schemaChange,
            Map<SchemaChangeType, SchemaChangePolicy> policyMap, String databasePattern, String schemaPattern,
            String tablePattern, SchemaUpdateExceptionPolicy exceptionPolicy, SinkTableMetricData metricData,
            DirtySinkHelper<Object> dirtySinkHelper) {
        this.dynamicSchemaFormat = Preconditions.checkNotNull(dynamicSchemaFormat, "dynamicSchemaFormat is null");
        this.schemaChange = schemaChange;
        this.policyMap = policyMap;
        this.databasePattern = databasePattern;
        this.schemaPattern = schemaPattern;
        this.tablePattern = tablePattern;
        this.exceptionPolicy = exceptionPolicy;
        this.metricData = metricData;
        this.dirtySinkHelper = dirtySinkHelper;
    }

    @Override
    public boolean checkSchemaChangeTypeEnable(SchemaChangeType type) {
        return policyMap.get(type) == SchemaChangePolicy.ENABLE;
    }

    @Override
    public void process(byte[] originData, JsonNode data) {
        // 1. Extract the schema change type from the data;
        if (!schemaChange) {
            LOGGER.warn("schemaChange is false, so skip ddl process");
            return;
        }
        String database;
        String table;
        String schema = null;
        try {
            database = dynamicSchemaFormat.parse(data, databasePattern);
            table = dynamicSchemaFormat.parse(data, tablePattern);
            if (StringUtils.isNotBlank(schemaPattern)) {
                schema = dynamicSchemaFormat.parse(data, schemaPattern);
            }
        } catch (Exception e) {
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                // ddl shouldn't force restart，since in doris，nothing is changed is an exception, and
                // nothing is changed causing restart is very weird. runtime exception changed to log.error
                LOGGER.error("sync ddl failed" + e);
            }
            LOGGER.warn("Parse database, table from origin data failed, origin data: {}", new String(originData), e);
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE) {
                dirtySinkHelper.invoke(new String(originData), DirtyType.JSON_PROCESS_ERROR, e);
                if (metricData != null) {
                    metricData.invokeDirty(1, originData.length);
                }
            }
            return;
        }
        Operation operation;
        try {
            JsonNode operationNode = data.get("operation");
            if (operationNode == null) {
                LOGGER.warn("operation is null. Unsupported for schema-change: {}", data);
                return;
            }
            operation = Preconditions.checkNotNull(
                    JsonDynamicSchemaFormat.OBJECT_MAPPER.convertValue(operationNode, new TypeReference<Operation>() {
                    }), "Operation is null");
        } catch (Exception e) {
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                throw new SchemaChangeHandleException(
                        String.format("Extract Operation from origin data failed,origin data: %s", data), e);
            }
            LOGGER.warn("Extract Operation from origin data failed,origin data: {}", data, e);
            handleDirtyData(data, originData, database, schema, table, DirtyType.JSON_PROCESS_ERROR, e);
            return;
        }
        String originSchema = dynamicSchemaFormat.extractDDL(data);
        SchemaChangeType type = SchemaChangeUtils.extractSchemaChangeType(operation);
        if (type == null) {
            LOGGER.warn("Unsupported for schema-change: {}", originSchema);
            return;
        }
        // 2. Apply schema change;
        if (type == SchemaChangeType.ALTER) {
            handleAlterOperation(database, schema, table, originData,
                    originSchema, data, (AlterOperation) operation);
            return;
        }
        SchemaChangePolicy policy = policyMap.get(type);
        if (policy != SchemaChangePolicy.ENABLE) {
            doSchemaChangeBase(type, policy, originSchema);
            return;
        }
        switch (type) {
            case CREATE_TABLE:
                doCreateTable(originData, database, schema, table, type, originSchema, data,
                        (CreateTableOperation) operation);
                break;
            case DROP_TABLE:
                doDropTable(database, schema, table, type, originSchema);
                break;
            case RENAME_TABLE:
                doRenameTable(database, schema, table, type, originSchema);
                break;
            case TRUNCATE_TABLE:
                doTruncateTable(database, schema, table, type, originSchema);
                break;
            default:
                LOGGER.warn("Unsupported for {}: {}", type, originSchema);
        }
    }

    @Override
    public void handleAlterOperation(String database, String schema, String table, byte[] originData,
            String originSchema, JsonNode data, AlterOperation operation) {
        if (operation.getAlterColumns() == null || operation.getAlterColumns().isEmpty()) {
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                throw new SchemaChangeHandleException(
                        String.format("Alter columns is empty, origin schema: %s", originSchema));
            }
            LOGGER.warn("Alter columns is empty, origin schema: {}", originSchema);
            return;
        }

        Map<SchemaChangeType, List<AlterColumn>> typeMap = new LinkedHashMap<>();
        for (AlterColumn alterColumn : operation.getAlterColumns()) {
            Set<SchemaChangeType> types = null;
            try {
                types = SchemaChangeUtils.extractSchemaChangeType(alterColumn);
                if (types.isEmpty()) {
                    LOGGER.warn("Ignore the unsupportted schema change, origin schema: {}", originSchema);
                    continue;
                }
                // Preconditions.checkState(!types.isEmpty(), "Schema change types is empty");
            } catch (Exception e) {
                if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Extract schema change type failed, origin schema: %s", originSchema), e);
                }
                LOGGER.warn("Extract schema change type failed, origin schema: {}", originSchema, e);
            }
            if (types == null) {
                continue;
            }
            if (types.size() == 1) {
                SchemaChangeType type = types.stream().findFirst().get();
                typeMap.computeIfAbsent(type, k -> new ArrayList<>()).add(alterColumn);
            } else {
                // Handle change column, it only exists change column type and rename column in this scenario for now.
                for (SchemaChangeType type : types) {
                    SchemaChangePolicy policy = policyMap.get(type);
                    if (policy == SchemaChangePolicy.ENABLE) {
                        LOGGER.warn("Unsupported for {}: {}", type, originSchema);
                    } else {
                        doSchemaChangeBase(type, policy, originSchema);
                    }
                }
            }
        }
        if (!typeMap.isEmpty()) {
            doAlterOperation(database, schema, table, originData, originSchema, data, typeMap);
        }
    }

    @Override
    public void handleAlterOperation(String database, String table, byte[] originData,
            String originSchema, JsonNode data, AlterOperation operation) {
        handleAlterOperation(database, null, table, originData, originSchema, data, operation);
    }

    @Override
    public String doAddColumn(String database, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema) {
        throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, originSchema));
    }

    @Override
    public String doChangeColumnType(String database, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema) {
        throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, originSchema));
    }

    @Override
    public String doRenameColumn(String database, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema) {
        throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, originSchema));
    }

    @Override
    public String doDropColumn(String database, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema) {
        throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, originSchema));
    }

    @Override
    public void doCreateTable(byte[] originData, String database, String table, SchemaChangeType type,
            String originSchema, JsonNode data, CreateTableOperation operation) {
        throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, originSchema));
    }

    @Override
    public void doDropTable(String database, String table, SchemaChangeType type, String originSchema) {
        throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, originSchema));
    }

    @Override
    public void doRenameTable(String database, String table, SchemaChangeType type, String originSchema) {
        throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, originSchema));
    }

    @Override
    public void doTruncateTable(String database, String table, SchemaChangeType type, String originSchema) {
        throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, originSchema));
    }

    protected void handleDirtyData(JsonNode data, byte[] originData, String database, String schema,
            String table, DirtyType dirtyType, Throwable e) {
        if (exceptionPolicy == SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE) {
            String label = parseValue(data, dirtySinkHelper.getDirtyOptions().getLabels());
            String logTag = parseValue(data, dirtySinkHelper.getDirtyOptions().getLogTag());
            String identifier = parseValue(data, dirtySinkHelper.getDirtyOptions().getIdentifier());
            dirtySinkHelper.invoke(new String(originData), dirtyType, label, logTag, identifier, e);
            if (metricData != null) {
                if (schema != null) {
                    metricData.outputDirtyMetricsWithEstimate(database, schema, table, 1, originData.length);
                } else {
                    metricData.outputDirtyMetricsWithEstimate(database, table, 1, originData.length);
                }
            }
        }
    }

    protected void reportWriteMetric(byte[] originData, String database, String schema, String table) {
        if (metricData != null) {
            metricData.outputMetrics(database, schema, table, 1, originData.length);
        }
    }

    protected void reportDirtyMetric(byte[] originData, String database, String schema, String table) {
        if (metricData != null) {
            metricData.outputDirtyMetrics(database, schema, table, 1, originData.length);
        }
    }

    private String parseValue(JsonNode data, String pattern) {
        try {
            return dynamicSchemaFormat.parse(data, pattern);
        } catch (Exception e) {
            LOGGER.warn("Parse value from pattern failed,the pattern: {}, data: {}", pattern, data);
        }
        return pattern;
    }

    protected void doSchemaChangeBase(SchemaChangeType type, SchemaChangePolicy policy, String schema) {
        if (policy == null) {
            LOGGER.warn("Unsupported for {}: {}", type, schema);
            return;
        }
        switch (policy) {
            case LOG:
                LOGGER.warn("Unsupported for {}: {}", type, schema);
                break;
            case ERROR:
                throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", type, schema));
            default:
        }
    }

    @Override
    public List<RowField> extractAddColumns(RowType oldSchema, RowType newSchema) {
        Set<String> oldFieldSet = new HashSet<>(oldSchema.getFieldCount());
        for (RowField field : oldSchema.getFields()) {
            oldFieldSet.add(field.getName());
        }
        List<RowField> addFields = new ArrayList<>();
        for (RowType.RowField newField : newSchema.getFields()) {
            if (!oldFieldSet.contains(newField.getName())) {
                addFields.add(newField);
            }
        }
        return addFields;
    }

    public RowField convert2RowField(Column column) {
        return convert2RowField(column, false);
    }

    public RowField convert2RowField(Column column, boolean keepOriginNullable) {
        boolean nullable = !keepOriginNullable || column.isNullable();
        LogicalType logicalType = dynamicSchemaFormat.sqlType2FlinkType(column.getJdbcType());
        List<String> definitions = column.getDefinition();
        switch (logicalType.getTypeRoot()) {
            case VARCHAR:
                if (definitions != null && !definitions.isEmpty()) {
                    if (definitions.size() != 1) {
                        try {
                            LOGGER.warn(
                                    "The length of definitions with VARCHAR should be 1, but found {}, the column: {}",
                                    definitions.size(),
                                    JsonDynamicSchemaFormat.OBJECT_MAPPER.writeValueAsString(column));
                        } catch (JsonProcessingException e) {
                            LOGGER.warn("Serialize column failed,the column: {}", column, e);
                            LOGGER.warn(
                                    "The length of definitions with VARCHAR should be 1, but found {}, the column: {}",
                                    definitions.size(), column);
                        }
                    }
                    VarCharType type = (VarCharType) logicalType;
                    int length = type.getLength();
                    try {
                        length = Integer.parseInt(definitions.get(0));
                        if (length <= 0) {
                            length = type.getLength();
                        }
                    } catch (NumberFormatException e) {
                        LOGGER.warn("Parse the length of VARCHAR failed", e);
                    }
                    logicalType = new VarCharType(nullable, length);
                } else {
                    logicalType = logicalType.copy(nullable);
                }
                break;
            case CHAR:
                if (definitions != null && !definitions.isEmpty()) {
                    if (definitions.size() != 1) {
                        try {
                            LOGGER.warn("The length of definitions with CHAR should be 1, but found {}, the column: {}",
                                    definitions.size(),
                                    JsonDynamicSchemaFormat.OBJECT_MAPPER.writeValueAsString(column));
                        } catch (JsonProcessingException e) {
                            LOGGER.warn("Serialize column failed,the column: {}", column, e);
                            LOGGER.warn("The length of definitions with CHAR should be 1, but found {}, the column: {}",
                                    definitions.size(), column);
                        }
                    }
                    CharType type = (CharType) logicalType;
                    int length = type.getLength();
                    try {
                        length = Integer.parseInt(definitions.get(0));
                        if (length <= 0) {
                            length = type.getLength();
                        }
                    } catch (NumberFormatException e) {
                        LOGGER.warn("Parse the length of VARCHAR failed", e);
                    }
                    logicalType = new CharType(nullable, length);
                } else {
                    logicalType = logicalType.copy(nullable);
                }
                break;
            case DECIMAL:
                if (definitions != null && !definitions.isEmpty()) {
                    try {
                        Preconditions.checkState(definitions.size() < 3,
                                String.format(
                                        "The length of definitions with DECIMAL must small than 3, the column: %s",
                                        JsonDynamicSchemaFormat.OBJECT_MAPPER.writeValueAsString(column)));
                    } catch (JsonProcessingException e) {
                        LOGGER.warn("Parse column failed,the column: {}", column, e);
                        Preconditions.checkState(definitions.size() < 3,
                                String.format(
                                        "The length of definitions with DECIMAL must small than 3, the column: %s",
                                        column));
                    }
                    int precision = Integer.parseInt(definitions.get(0));
                    int scale = JsonDynamicSchemaFormat.DEFAULT_DECIMAL_SCALE;
                    if (definitions.size() == 2) {
                        scale = Integer.parseInt(definitions.get(1));
                    }
                    try {
                        logicalType = new DecimalType(nullable, precision, scale);
                    } catch (ValidationException e) {
                        logicalType = new VarCharType(VarCharType.MAX_LENGTH);
                        LOGGER.warn("The precision or scale is unvalid and it will be converted to STRING, "
                                + "the column: {}", column, e);
                    }
                } else {
                    logicalType = logicalType.copy(nullable);
                }
                break;
            default:
                logicalType = logicalType.copy(nullable);
        }
        String comment = StringUtils.isBlank(column.getComment()) ? Constants.ADD_COLUMN_COMMENT
                : column.getComment() + " " + Constants.ADD_COLUMN_COMMENT;
        return new RowField(column.getName(), logicalType, comment);
    }
}
