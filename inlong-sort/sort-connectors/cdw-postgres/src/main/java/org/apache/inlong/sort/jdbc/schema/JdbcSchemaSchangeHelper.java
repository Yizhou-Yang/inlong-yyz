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

package org.apache.inlong.sort.jdbc.schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.metric.sub.SinkTableMetricData;
import org.apache.inlong.sort.base.schema.SchemaChangeHandleException;
import org.apache.inlong.sort.base.schema.SchemaChangeHelper;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.jdbc.internal.JdbcMultiBatchingOutputFormat;
import org.apache.inlong.sort.jdbc.table.AbstractJdbcDialect;
import org.apache.inlong.sort.jdbc.utils.JdbcMultipleUtils;
import org.apache.inlong.sort.protocol.ddl.Column;
import org.apache.inlong.sort.protocol.ddl.enums.PositionType;
import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.protocol.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import static org.apache.inlong.sort.base.Constants.INCREMENTAL;

/**
 * Jdbc schema change helper
 */
public class JdbcSchemaSchangeHelper extends SchemaChangeHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSchemaSchangeHelper.class);

    private final int retryTimes;
    @SuppressWarnings("rawtypes")
    private final JdbcMultiBatchingOutputFormat outputFormat;
    private final Map<String, JsonNode> firstDataMap;
    private final Map<String, RowType> rowTypeMap;
    private final AbstractJdbcDialect dialect;
    private static final String TABLE_NOT_USE = "not_use";
    private final boolean autoCreateTableWhenSnapshot;

    @SuppressWarnings("rawtypes")
    public JdbcSchemaSchangeHelper(JsonDynamicSchemaFormat dynamicSchemaFormat, boolean schemaChange,
            Map<SchemaChangeType, SchemaChangePolicy> policyMap, String databasePattern, String schemaPattern,
            String tablePattern, SchemaUpdateExceptionPolicy exceptionPolicy, SinkTableMetricData metricData,
            DirtySinkHelper<Object> dirtySinkHelper, boolean autoCreateTableWhenSnapshot,
            JdbcMultiBatchingOutputFormat outputFormat, Map<String, JsonNode> latestDataMap,
            Map<String, RowType> rowTypeMap, int retryTimes) {
        super(dynamicSchemaFormat, schemaChange, policyMap, databasePattern, schemaPattern,
                tablePattern, exceptionPolicy, metricData, dirtySinkHelper);
        this.autoCreateTableWhenSnapshot = autoCreateTableWhenSnapshot;
        this.outputFormat = outputFormat;
        this.firstDataMap = latestDataMap;
        this.rowTypeMap = rowTypeMap;
        this.retryTimes = retryTimes;
        dialect = (AbstractJdbcDialect) outputFormat.getJdbcOptions().getDialect();
    }

    @SuppressWarnings("rawtypes")
    public static JdbcSchemaSchangeHelper of(JsonDynamicSchemaFormat dynamicSchemaFormat, boolean schemaChange,
            Map<SchemaChangeType, SchemaChangePolicy> policyMap, String databasePattern, String schemaPattern,
            String tablePattern, SchemaUpdateExceptionPolicy exceptionPolicy, SinkTableMetricData metricData,
            DirtySinkHelper<Object> dirtySinkHelper, boolean autoCreateTableWhenSnapshot,
            JdbcMultiBatchingOutputFormat outputFormat, Map<String, JsonNode> latestDataMap,
            Map<String, RowType> rowTypeMap, int retryTimes) {
        return new JdbcSchemaSchangeHelper(dynamicSchemaFormat, schemaChange, policyMap, databasePattern, schemaPattern,
                tablePattern, exceptionPolicy, metricData, dirtySinkHelper, autoCreateTableWhenSnapshot, outputFormat,
                latestDataMap, rowTypeMap, retryTimes);
    }

    public void applySchemaChange(String tableIdentifier, RowType oldSchema, RowType newSchema) {
        List<RowField> addFields = extractAddColumns(oldSchema, newSchema);
        if (addFields.isEmpty()) {
            return;
        }
        doSchemaChangeBase(SchemaChangeType.ADD_COLUMN,
                policyMap.get(SchemaChangeType.ADD_COLUMN), null);
        if (!checkSchemaChangeTypeEnable(SchemaChangeType.ADD_COLUMN)) {
            return;
        }
        String database = JdbcMultipleUtils.getDatabaseFromIdentifier(tableIdentifier,
                dialect.getDefaultDatabase());
        String stmt =
                dialect.buildAlterTableStatement(tableIdentifier) + " "
                        + dialect.buildAddColumnStatement(tableIdentifier, addFields);
        try {
            executeStatement(stmt, database);
            outputFormat.outputMetrics(tableIdentifier, 1,
                    stmt.getBytes(StandardCharsets.UTF_8).length, false);
        } catch (Exception e) {
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                throw new SchemaChangeHandleException("Apply schema change failed", e);
            }
            LOGGER.warn("Apply schema change failed", e);
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE) {
                outputFormat.outputMetrics(tableIdentifier, 1, stmt.getBytes(StandardCharsets.UTF_8).length, true);
                outputFormat.handleDirtyData(stmt, tableIdentifier, DirtyType.APPLY_SCHEMA_CHANGE_ERROR, e);
            }
        }
    }

    private boolean isIncremental(String tableIdentifier) {
        JsonNode firstData = firstDataMap.get(tableIdentifier);
        if (firstData == null) {
            return false;
        }
        return Optional.ofNullable(firstData.get(INCREMENTAL))
                .map(JsonNode::asBoolean).orElse(true);
    }

    public boolean handleResourceNotExists(String tableIdentifier, SQLException e) {
        if (!dialect.isResourceNotExists(e)) {
            LOGGER.warn("Can't handle the error", e);
            return false;
        }
        LOGGER.warn("handleResourceNotExists due to", e);
        String stmt;
        String database = JdbcMultipleUtils.getDatabaseFromIdentifier(tableIdentifier,
                dialect.getDefaultDatabase());
        String schema = JdbcMultipleUtils.getSchemaFromIdentifier(tableIdentifier);
        long length = 0;
        StringBuilder sb = new StringBuilder();
        String comment = Constants.CREATE_TABLE_COMMENT;
        try {
            List<String> primaryKeys =
                    dynamicSchemaFormat.extractPrimaryKeyNames(firstDataMap.get(tableIdentifier));
            if (dialect.parseUnknownDatabase(e)) {
                doSchemaChangeBase(SchemaChangeType.CREATE_TABLE,
                        policyMap.get(SchemaChangeType.CREATE_TABLE), null);
                if (!checkSchemaChangeTypeEnable(SchemaChangeType.CREATE_TABLE)) {
                    return false;
                }
                stmt = dialect.buildCreateDatabaseStatement(database);
                if (stmt != null) {
                    sb.append(stmt);
                    length += stmt.getBytes(StandardCharsets.UTF_8).length;
                }
                executeStatement(stmt, dialect.getDefaultDatabase());
                stmt = dialect.buildCreateSchemaStatement(schema);
                if (stmt != null) {
                    sb.append(stmt);
                    length += stmt.getBytes(StandardCharsets.UTF_8).length;
                }
                executeStatement(stmt, database);
                stmt = dialect.buildCreateTableStatement(tableIdentifier, primaryKeys,
                        rowTypeMap.get(tableIdentifier), comment);
                if (stmt != null) {
                    sb.append(stmt);
                    length += stmt.getBytes(StandardCharsets.UTF_8).length;
                }
                executeStatement(stmt, database);
            } else if (dialect.parseUnkownSchema(e)) {
                doSchemaChangeBase(SchemaChangeType.CREATE_TABLE,
                        policyMap.get(SchemaChangeType.CREATE_TABLE), null);
                if (!checkSchemaChangeTypeEnable(SchemaChangeType.CREATE_TABLE)) {
                    return false;
                }
                schema = JdbcMultipleUtils.getSchemaFromIdentifier(tableIdentifier);
                stmt = dialect.buildCreateSchemaStatement(schema);
                if (stmt != null) {
                    sb.append(stmt);
                    length += stmt.getBytes(StandardCharsets.UTF_8).length;
                }
                executeStatement(stmt, database);
                stmt = dialect.buildCreateTableStatement(tableIdentifier, primaryKeys,
                        rowTypeMap.get(tableIdentifier), comment);
                if (stmt != null) {
                    sb.append(stmt);
                    length += stmt.getBytes(StandardCharsets.UTF_8).length;
                }
                executeStatement(stmt, database);
            } else if (dialect.parseUnkownTable(e)) {
                doSchemaChangeBase(SchemaChangeType.CREATE_TABLE,
                        policyMap.get(SchemaChangeType.CREATE_TABLE), null);
                if (!checkSchemaChangeTypeEnable(SchemaChangeType.CREATE_TABLE)) {
                    return false;
                }
                if (StringUtils.isNotBlank(database)) {
                    stmt = dialect.buildCreateDatabaseStatement(database);
                    if (StringUtils.isNotBlank(stmt)) {
                        sb.append(stmt);
                        length += stmt.getBytes(StandardCharsets.UTF_8).length;
                        executeStatement(stmt, dialect.getDefaultDatabase());
                    }
                }
                if (StringUtils.isNotBlank(schema)) {
                    stmt = dialect.buildCreateSchemaStatement(schema);
                    if (StringUtils.isNotBlank(stmt)) {
                        sb.append(stmt);
                        length += stmt.getBytes(StandardCharsets.UTF_8).length;
                        executeStatement(stmt, database);
                    }
                }
                stmt = dialect.buildCreateTableStatement(tableIdentifier, primaryKeys,
                        rowTypeMap.get(tableIdentifier), comment);
                if (stmt != null) {
                    sb.append(stmt);
                    length += stmt.getBytes(StandardCharsets.UTF_8).length;
                }
                executeStatement(stmt, database);
            } else if (dialect.parseUnkownColumn(e)) {
                doSchemaChangeBase(SchemaChangeType.ADD_COLUMN,
                        policyMap.get(SchemaChangeType.ADD_COLUMN), null);
                if (!checkSchemaChangeTypeEnable(SchemaChangeType.ADD_COLUMN)) {
                    return false;
                }
                String unknownColumn = dialect.extractUnkownColumn(e);
                if (unknownColumn != null) {
                    RowType rowType = rowTypeMap.get(tableIdentifier);
                    if (rowType == null) {
                        LOGGER.warn("Ignore the unknown column handle because the rowtype is null for {}",
                                tableIdentifier);
                        return false;
                    }
                    int columnIndex = rowType.getFieldIndex(unknownColumn);
                    if (columnIndex == -1) {
                        LOGGER.warn(
                                "Ignore the unknown column handle because the unknown column is not found in schema for {}",
                                tableIdentifier);
                        return false;
                    }
                    stmt = dialect.buildAlterTableStatement(tableIdentifier) + " "
                            + dialect.buildAddColumnStatement(tableIdentifier,
                                    Collections.singletonList(rowType.getFields().get(columnIndex)));
                    sb.append(stmt);
                    length += stmt.getBytes(StandardCharsets.UTF_8).length;
                    executeStatement(stmt, database);
                    return true;
                }
                LOGGER.warn("Ignore the unknown column handle because the unknown column is null for {}",
                        tableIdentifier);
                return false;
            } else {
                throw new SchemaChangeHandleException("Unsupported operation", e);
            }
            outputFormat.outputMetrics(tableIdentifier, 1, length, false);
            return true;
        } catch (SchemaChangeHandleException ex) {
            throw ex;
        } catch (Exception ex) {
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                throw new SchemaChangeHandleException("handle resource not exists failed", ex);
            }
            LOGGER.warn("handle resource not exists failed", ex);
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE) {
                outputFormat.outputMetrics(tableIdentifier, 1, length, true);
                outputFormat.handleDirtyData(sb.toString(), tableIdentifier, DirtyType.APPLY_SCHEMA_CHANGE_ERROR,
                        ex);
            }
        }
        return false;
    }

    private void executeStatement(String stmt, String database) throws Exception {
        if (StringUtils.isBlank(stmt)) {
            return;
        }
        LOGGER.info("Start execute statement in database: {}, the statement:\n{} ", database, stmt);
        JdbcOptions execOptions =
                JdbcMultipleUtils.getExecJdbcOptions(outputFormat.getJdbcOptions(), database, TABLE_NOT_USE);
        SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(execOptions);
        PreparedStatement ps = null;
        for (int i = 0; i <= retryTimes; i++) {
            try {
                Connection conn = provider.getOrEstablishConnection();
                ps = conn.prepareStatement(stmt);
                ps.execute();
                LOGGER.info("Execute statement in database: {} success, the statement:\n{}", database, stmt);
                break;
            } catch (SQLException e) {
                if (dialect.parseResourceExistsError(e)) {
                    LOGGER.warn("The error is ignore, the code: {}, the sql state: {}",
                            e.getErrorCode(), e.getSQLState(), e);
                    break;
                }
                LOGGER.warn("Execute statement failed in database: {},the statement:\n{}", database, stmt, e);
                if (i < retryTimes) {
                    Thread.sleep(1000);
                } else {
                    throw e;
                }
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException ex) {
                        LOGGER.warn("Close statement failed", ex);
                    }
                }
                provider.closeConnection();
            }
        }
    }

    @Override
    public void doAlterOperation(String database, String schema, String table, byte[] originData, String originSchema,
            JsonNode data, Map<SchemaChangeType, List<AlterColumn>> typeMap) {
        String alterStatement = null;
        for (Entry<SchemaChangeType, List<AlterColumn>> kv : typeMap.entrySet()) {
            SchemaChangePolicy policy = policyMap.get(kv.getKey());
            doSchemaChangeBase(kv.getKey(), policy, originSchema);
            if (policy == SchemaChangePolicy.ENABLE && kv.getKey() == SchemaChangeType.ADD_COLUMN) {
                try {
                    alterStatement = doAddColumn(database, schema, table, kv.getValue(), kv.getKey(), originSchema);
                } catch (Exception e) {
                    if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                        throw new SchemaChangeHandleException(
                                String.format("Build alter statement failed, origin schema: %s", originSchema), e);
                    }
                    LOGGER.warn("Build alter statement failed, origin schema: {}", originSchema, e);
                }
            }
        }
        if (alterStatement != null) {
            try {
                alterStatement = dialect.buildAlterTableStatement(
                        JdbcMultipleUtils.buildTableIdentifier(database, schema, table)) + " " + alterStatement;
                // The checkLightSchemaChange is removed because most scenarios support it
                executeStatement(alterStatement, database);
                LOGGER.info("Alter table success,statement: {}", alterStatement);
                reportWriteMetric(originData, database, schema, table);
                // Refresh schema of table when do alter table success to avoid report write metric twice.
                outputFormat.updateRowType(database, schema, table, data);
            } catch (Exception e) {
                if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Alter table failed, origin schema: %s", originSchema), e);
                }
                handleDirtyData(data, originData, database, schema, table, DirtyType.HANDLE_ALTER_TABLE_ERROR, e);
            }
        }
    }

    @Override
    public void doAlterOperation(String database, String table, byte[] originData, String originSchema, JsonNode data,
            Map<SchemaChangeType, List<AlterColumn>> typeMap) {
        doAlterOperation(database, null, table, originData, originSchema, data, typeMap);
    }

    @Override
    public String doAddColumn(String database, String schema, String table, List<AlterColumn> alterColumns,
            SchemaChangeType type, String originSchema) {
        Map<String, String> positionMap = new HashMap<>();
        List<RowField> fields = alterColumns.stream().map(s -> {
            Column column = s.getNewColumn();
            parsePosition(column, positionMap);
            return convert2RowField(column);
        }).collect(Collectors.toList());
        return dialect.buildAddColumnStatement(JdbcMultipleUtils.buildTableIdentifier(database, schema, table), fields,
                positionMap);
    }

    private void parsePosition(Column column, Map<String, String> positionMap) {
        if (column.getPosition() != null && column.getPosition().getPositionType() != null) {
            if (column.getPosition().getPositionType() == PositionType.FIRST) {
                positionMap.put(column.getName(), null);
            } else if (column.getPosition().getPositionType() == PositionType.AFTER) {
                Preconditions.checkState(column.getPosition().getColumnName() != null
                        && !column.getPosition().getColumnName().trim().isEmpty(),
                        "The column name of Position is empty");
                positionMap.put(column.getName(), column.getPosition().getColumnName());
            }
        }
    }

    @Override
    public void doCreateTable(byte[] originData, String database, String schema, String table, SchemaChangeType type,
            String originSchema, JsonNode data, CreateTableOperation operation) {
        SchemaChangePolicy policy = policyMap.get(type);
        if (policy == SchemaChangePolicy.ENABLE) {
            try {
                List<String> primaryKeys = dynamicSchemaFormat.extractPrimaryKeyNames(data);
                String comment = operation.getComment() == null ? Constants.CREATE_TABLE_COMMENT
                        : operation.getComment() + " " + Constants.CREATE_TABLE_COMMENT;
                List<RowField> fields = new ArrayList<>();
                for (Column column : operation.getColumns()) {
                    fields.add(convert2RowField(column));
                }
                if (StringUtils.isNotBlank(database)) {
                    String stmt = dialect.buildCreateDatabaseStatement(database);
                    if (StringUtils.isNotBlank(stmt)) {
                        executeStatement(stmt, dialect.getDefaultDatabase());
                    }
                }
                if (StringUtils.isNotBlank(schema)) {
                    String stmt = dialect.buildCreateSchemaStatement(schema);
                    if (StringUtils.isNotBlank(stmt)) {
                        executeStatement(stmt, database);
                    }
                }
                String stmt = dialect.buildCreateTableStatement(
                        JdbcMultipleUtils.buildTableIdentifier(database, schema, table),
                        primaryKeys, new RowType(fields), comment);
                executeStatement(stmt, database);
                reportDirtyMetric(originData, database, schema, table);
                return;
            } catch (Exception e) {
                if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Create table failed, origin schema: %s", originSchema), e);
                }
                handleDirtyData(data, originData, database, schema, table, DirtyType.CREATE_TABLE_ERROR, e);
                return;
            }
        }
        doSchemaChangeBase(type, policy, originSchema);
    }
}