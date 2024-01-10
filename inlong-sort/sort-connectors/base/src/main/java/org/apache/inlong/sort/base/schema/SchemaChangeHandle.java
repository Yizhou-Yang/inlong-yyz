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

import org.apache.flink.table.types.logical.RowType;
import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.protocol.ddl.operations.AlterOperation;
import org.apache.inlong.sort.protocol.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/**
 * Schema change helper interface
 * */
public interface SchemaChangeHandle {

    /**
     * execute ddl statement
     * */
    void process(byte[] originData, JsonNode data);
    /**
     * Handle modify column operations
     * */
    void handleAlterOperation(String database, String table, byte[] originData, String originSchema,
            JsonNode data, AlterOperation operation);

    default void handleAlterOperation(String database, String schema, String table, byte[] originData,
            String originSchema,
            JsonNode data, AlterOperation operation) {
        handleAlterOperation(database, table, originData, originSchema, data, operation);
    }

    /**
     * apply the modify column operation
     * */
    default void doAlterOperation(String database, String schema, String table, byte[] originData, String originSchema,
            JsonNode data, Map<SchemaChangeType, List<AlterColumn>> typeMap) {
        doAlterOperation(database, table, originData, originSchema, data, typeMap);
    }

    void doAlterOperation(String database, String table, byte[] originData, String originSchema,
            JsonNode data, Map<SchemaChangeType, List<AlterColumn>> typeMap);
    /**
     * apply the add column operation
     * */
    default String doAddColumn(String database, String schema, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema) {
        return doAddColumn(database, table, alterColumns, type, originSchema);
    }

    String doAddColumn(String database, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema);
    /**
     * apply the operation of changing the column type
     * */
    default String doChangeColumnType(String database, String schema, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema) {
        return doChangeColumnType(database, table, alterColumns, type, originSchema);
    }

    String doChangeColumnType(String database, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema);

    /**
     * apply the change column name operation
     * */
    default String doRenameColumn(String database, String schema, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema) {
        return doRenameColumn(database, table, alterColumns, type, originSchema);
    }

    String doRenameColumn(String database, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema);
    /**
     * apply the delete column operation
     * */
    default String doDropColumn(String database, String schema, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema) {
        return doDropColumn(database, table, alterColumns, type, originSchema);
    }

    String doDropColumn(String database, String table,
            List<AlterColumn> alterColumns, SchemaChangeType type, String originSchema);

    /**
     * handle the create table operation
     * */
    default void doCreateTable(byte[] originData, String database, String schema, String table, SchemaChangeType type,
            String originSchema, JsonNode data, CreateTableOperation operation) {
        doCreateTable(originData, database, table, type, originSchema, data, operation);
    }

    void doCreateTable(byte[] originData, String database, String schema, String table, SchemaChangeType type,
            String originSchema, JsonNode data, CreateTableOperation operation, String partitionNameRules);

    void doCreateTable(byte[] originData, String database, String table, SchemaChangeType type,
            String originSchema, JsonNode data, CreateTableOperation operation);

    /**
     * handle drop table operation
     * */
    default void doDropTable(String database, String schema, String table, SchemaChangeType type, String originSchema) {
        doDropTable(database, table, type, originSchema);
    }

    void doDropTable(String database, String table, SchemaChangeType type, String originSchema);

    /**
     * handle rename table operation
     * */
    default void doRenameTable(String database, String schema, String table, SchemaChangeType type,
            String originSchema) {
        doRenameTable(database, table, type, originSchema);
    }

    void doRenameTable(String database, String table, SchemaChangeType type, String originSchema);
    /**
     * handle truncate table operation
     * */
    default void doTruncateTable(String database, String schema, String table, SchemaChangeType type,
            String originSchema) {
        doTruncateTable(database, table, type, originSchema);
    }

    void doTruncateTable(String database, String table, SchemaChangeType type, String originSchema);

    /**
     * Check if the schema change type is supported
     * @param type The type {@link SchemaChangeType}
     * @return true if support else false
     */
    boolean checkSchemaChangeTypeEnable(SchemaChangeType type);

    /**
     * Extact add columns
     * @param oldSchema The old schema
     * @param newSchema The new schema
     * @return The list of {@link org.apache.flink.table.types.logical.RowType.RowField}
     */
    List<RowType.RowField> extractAddColumns(RowType oldSchema, RowType newSchema);
}