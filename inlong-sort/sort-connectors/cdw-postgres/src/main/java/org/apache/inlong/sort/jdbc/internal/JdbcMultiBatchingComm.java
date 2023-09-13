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

package org.apache.inlong.sort.jdbc.internal;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor;
import org.apache.inlong.sort.jdbc.dialect.PostgresDialect;
import org.apache.inlong.sort.jdbc.executor.TableBatchDeleteExecutor;
import org.apache.inlong.sort.jdbc.executor.TableBufferReducedStatementExecutor;
import org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor;
import org.apache.inlong.sort.jdbc.executor.TableSimpleStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.inlong.sort.jdbc.options.JdbcExecutionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Comm function for A JDBC multi-table outputFormat to get or create JDBC Executor
 */
public class JdbcMultiBatchingComm {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMultiBatchingComm.class);
    private static JdbcOptions jdbcOption;

    public static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
            JdbcDmlOptions opt,
            RuntimeContext ctx,
            TypeInformation<RowData> rowDataTypeInfo,
            LogicalType[] fieldTypes,
            JdbcExecutionOptions executionOptions) {
        checkArgument(opt.getKeyFields().isPresent());
        PostgresDialect dialect = (PostgresDialect) opt.getDialect();
        String tableName = opt.getTableName();
        String[] pkNames = opt.getKeyFields().get();
        int[] pkFields =
                Arrays.stream(pkNames)
                        .mapToInt(Arrays.asList(opt.getFieldNames())::indexOf)
                        .toArray();
        LogicalType[] pkTypes =
                Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
        final TypeSerializer<RowData> typeSerializer =
                rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
        final Function<RowData, RowData> valueTransform =
                ctx.getExecutionConfig().isObjectReuseEnabled()
                        ? typeSerializer::copy
                        : Function.identity();

        return new TableBufferReducedStatementExecutor(
                createInsertRowExecutor(
                        dialect,
                        tableName,
                        opt.getFieldNames(),
                        fieldTypes, executionOptions),
                createInsertRowExecutor(
                        dialect,
                        tableName,
                        opt.getFieldNames(),
                        fieldTypes, executionOptions),
                createInsertRowExecutor(
                        dialect,
                        tableName,
                        opt.getFieldNames(),
                        fieldTypes, executionOptions),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes, pkFields),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes, pkFields),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes, pkFields),
                createRowKeyExtractor(fieldTypes, pkFields),
                valueTransform);
    }

    private static JdbcBatchStatementExecutor<RowData> createInsertRowExecutor(
            PostgresDialect dialect,
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            JdbcExecutionOptions executionOptions) {
        String insertSql = dialect.getInsertIntoStatement(tableName, fieldNames);
        return createSimpleRowExecutor(tableName, dialect, fieldNames, fieldTypes, insertSql, executionOptions);
    }

    public static JdbcBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
            RuntimeContext ctx,
            PostgresDialect dialect,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            String sql,
            TypeInformation<RowData> rowDataTypeInfo,
            JdbcDmlOptions dmlOptions,
            JdbcExecutionOptions executionOptions,
            String tableIdentifier) {
        String tableName = JdbcMultiBatchingComm.getTableNameFromIdentifier(tableIdentifier);
        final TypeSerializer<RowData> typeSerializer =
                rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
        return new TableBufferedStatementExecutor(
                createSimpleRowExecutor(tableName, dialect, fieldNames, fieldTypes, sql,
                        executionOptions),
                ctx.getExecutionConfig().isObjectReuseEnabled()
                        ? typeSerializer::copy
                        : Function.identity());
    }

    private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(PostgresDialect dialect,
            String tableName, String[] pkNames, LogicalType[] pkTypes, int[] pkFields) {
        // LOG.info("create delete executor");
        return new TableBatchDeleteExecutor(dialect, pkNames, pkTypes, tableName, pkFields);
    }

    private static JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(
            String tableName, PostgresDialect dialect, String[] fieldNames, LogicalType[] fieldTypes, final String sql,
            JdbcExecutionOptions executionOptions) {
        final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));
        if (PostgresDialect.WRITE_MODE_COPY.equals(executionOptions.getWriteMode())) {
            LOG.info("createSimpleRowExecutor with mode copy");
            return new TableCopyStatementExecutor(dialect, tableName, fieldNames, fieldTypes,
                    executionOptions.getCopyDelimiter());
        } else {
            LOG.info("createSimpleRowExecutor with mode insert");
            return new TableSimpleStatementExecutor(
                    connection -> FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames),
                    rowConverter);
        }
    }

    private static Function<RowData, RowData> createRowKeyExtractor(
            LogicalType[] logicalTypes, int[] pkFields) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
        for (int i = 0; i < pkFields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
        }
        return row -> getPrimaryKey(row, fieldGetters);
    }

    private static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
        GenericRowData pkRow = new GenericRowData(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return pkRow;
    }

    public static JdbcOptions getExecJdbcOptions(JdbcOptions jdbcOptions, String tableIdentifier) {
        JdbcOptions jdbcExecOptions =
                JdbcOptions.builder()
                        .setDBUrl(jdbcOptions.getDbURL() + "/" + getDatabaseNameFromIdentifier(tableIdentifier))
                        .setTableName(getTableNameFromIdentifier(tableIdentifier))
                        .setDialect(jdbcOptions.getDialect())
                        .setParallelism(jdbcOptions.getParallelism())
                        .setConnectionCheckTimeoutSeconds(jdbcOptions.getConnectionCheckTimeoutSeconds())
                        .setDriverName(jdbcOptions.getDriverName())
                        .setUsername(jdbcOptions.getUsername().orElse(""))
                        .setPassword(jdbcOptions.getPassword().orElse(""))
                        .build();

        jdbcOption = jdbcExecOptions;

        // LOG.error("getting jdbc options, {} , {} , {} ", jdbcOptions.getDriverName(), jdbcOptions.getUsername()
        // .orElse(""), jdbcOptions.getPassword().orElse(""));
        return jdbcExecOptions;
    }

    /**
     * Get table name From tableIdentifier
     * tableIdentifier maybe: ${dbName}.${tbName} or ${dbName}.${schemaName}.${tbName}
     *
     * @param tableIdentifier The table identifier for which to get table name.
     */
    public static String getTableNameFromIdentifier(String tableIdentifier) {
        String[] fieldArray = tableIdentifier.split("\\.");
        if (2 == fieldArray.length) {
            return fieldArray[1];
        }
        if (3 == fieldArray.length) {
            return fieldArray[1] + "." + fieldArray[2];
        }
        return null;
    }

    /**
     * Get database name From tableIdentifier
     * tableIdentifier maybe: ${dbName}.${tbName} or ${dbName}.${schemaName}.${tbName}
     *
     * @param tableIdentifier The table identifier for which to get table name.
     */
    public static String getDatabaseNameFromIdentifier(String tableIdentifier) {
        String[] fileArray = tableIdentifier.split("\\.");
        return fileArray[0];
    }

}
