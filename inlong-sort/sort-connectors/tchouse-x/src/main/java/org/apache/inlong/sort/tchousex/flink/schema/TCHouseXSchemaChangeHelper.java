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

package org.apache.inlong.sort.tchousex.flink.schema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.metric.sub.SinkTableMetricData;
import org.apache.inlong.sort.base.schema.SchemaChangeHandleException;
import org.apache.inlong.sort.base.schema.SchemaChangeHelper;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.protocol.ddl.Column;
import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.protocol.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.apache.inlong.sort.schema.TableChange;
import org.apache.inlong.sort.tchousex.flink.catalog.TCHouseXCatalog;
import org.apache.inlong.sort.tchousex.flink.catalog.Table;
import org.apache.inlong.sort.tchousex.flink.catalog.TableIdentifier;
import org.apache.inlong.sort.tchousex.flink.util.FlinkSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TCHouseXSchemaChangeHelper extends SchemaChangeHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TCHouseXSchemaChangeHelper.class);

    private TCHouseXCatalog tCHouseXCatalog;

    private AtomicBoolean isSuccessDDL = new AtomicBoolean(false);

    private Map<TableIdentifier, Table> schemaCache;

    public TCHouseXSchemaChangeHelper(JsonDynamicSchemaFormat dynamicSchemaFormat, boolean schemaChange,
            Map<SchemaChangeType, SchemaChangePolicy> policyMap, String databasePattern, String tablePattern,
            SchemaUpdateExceptionPolicy exceptionPolicy, SinkTableMetricData metricData,
            DirtySinkHelper<Object> dirtySinkHelper, TCHouseXCatalog tCHouseXCatalog,
            Map<TableIdentifier, Table> schemaCache) {
        super(dynamicSchemaFormat, schemaChange, policyMap, databasePattern, null, tablePattern,
                exceptionPolicy, metricData, dirtySinkHelper);
        this.tCHouseXCatalog = tCHouseXCatalog;
        this.schemaCache = schemaCache;
    }

    /**
     * apply the modify column operation
     *
     * @param database
     * @param table
     * @param originData
     * @param originSchema
     * @param data
     * @param typeMap
     */
    @Override
    public void doAlterOperation(String database, String table, byte[] originData, String originSchema, JsonNode data,
            Map<SchemaChangeType, List<AlterColumn>> typeMap) {
        for (Map.Entry<SchemaChangeType, List<AlterColumn>> kv : typeMap.entrySet()) {
            SchemaChangePolicy policy = policyMap.get(kv.getKey());
            try {
                if (policy != SchemaChangePolicy.ENABLE) {
                    doSchemaChangeBase(kv.getKey(), policy, originSchema);
                } else {
                    switch (kv.getKey()) {
                        case ADD_COLUMN:
                            doAddColumn(kv.getValue(), TableIdentifier.of(database, table));
                            break;
                        case DROP_COLUMN:
                            doDropColumn(null, null, null, kv.getKey(), originSchema);
                            break;
                        case RENAME_COLUMN:
                            doRenameColumn(null, null, null, kv.getKey(), originSchema);
                            break;
                        case CHANGE_COLUMN_TYPE:
                            doChangeColumnType(null, null, null, kv.getKey(), originSchema);
                            break;
                        default:
                    }
                    isSuccessDDL.set(true);
                }
            } catch (Exception e) {
                if (policy == SchemaChangePolicy.ERROR
                        || exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Apply alter column failed, origin schema: %s", originSchema), e);
                }
                LOGGER.warn("Apply alter column failed, origin schema: {}", originSchema, e);
                handleDirtyData(data, originData, database, null, table, DirtyType.HANDLE_ALTER_TABLE_ERROR, e);
            }
        }
    }

    @Override
    public void doCreateTable(byte[] originData, String database, String tableName, SchemaChangeType type,
            String originSchema, JsonNode data, CreateTableOperation operation) {
        try {
            TableIdentifier tableId = TableIdentifier.of(database, tableName);
            List<String> pkListStr = dynamicSchemaFormat.extractPrimaryKeyNames(data);
            RowType rowType = dynamicSchemaFormat.extractSchema(data, pkListStr);
            Table table = FlinkSchemaUtil.getTableByParseRowType(rowType, tableId, pkListStr);
            tCHouseXCatalog.createTable(table, true);
            schemaCache.put(tableId, table);
            isSuccessDDL.set(true);
        } catch (Exception e) {
            LOGGER.warn(String.format("tableId:%s.%s, doCreateTable exception", database, tableName), e);
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                throw new SchemaChangeHandleException(String.format("create table failed, origin schema: %s",
                        originSchema), e);
            }
            handleDirtyData(data, originData, database, null, tableName, DirtyType.CREATE_TABLE_ERROR, e);
        }
    }

    private void doAddColumn(List<AlterColumn> alterColumns, TableIdentifier tableId) {
        List<TableChange> tableChanges = new ArrayList<>();
        alterColumns.forEach(alterColumn -> {
            Column column = alterColumn.getNewColumn();
            LogicalType dataType = dynamicSchemaFormat.sqlType2FlinkType(column.getJdbcType());

            TableChange.AddColumn addColumn = new TableChange.AddColumn(new String[]{column.getName()},
                    dataType, column.isNullable(), column.getComment(), null);
            tableChanges.add(addColumn);
        });
        tCHouseXCatalog.applySchemaChanges(tableId, tableChanges);
        Table table = tCHouseXCatalog.getTable(tableId);
        schemaCache.put(tableId, table);
    }

    public AtomicBoolean ddlExecSuccess() {
        return isSuccessDDL;
    }

    public Map<TableIdentifier, Table> getSchemaCache() {
        return schemaCache;
    }

    public void setSchemaCache(Map<TableIdentifier, Table> schemaCache) {
        this.schemaCache = schemaCache;
    }
}
