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

package org.apache.inlong.sort.jdbc.table;

import org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.inlong.sort.jdbc.executor.TableBufferReducedStatementExecutor;
import org.apache.inlong.sort.jdbc.executor.TableSimpleStatementExecutor;
import org.apache.inlong.sort.jdbc.executor.TableCopyStatementExecutor;
import org.apache.inlong.sort.jdbc.executor.TableBatchDeleteExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableInsertOrUpdateStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.jdbc.dialect.PostgresDialect;
import org.apache.inlong.sort.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.inlong.sort.jdbc.internal.JdbcMultiBatchingOutputFormat;
import org.apache.inlong.sort.jdbc.options.JdbcExecutionOptions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Copy from org.apache.flink:flink-connector-jdbc_2.11:1.13.5
 *
 * Builder for {@link JdbcBatchingOutputFormat} for Table/SQL.
 * Add an option `sink.ignore.changelog` to support insert-only mode without primaryKey.
 */
/**
 * Builder for {@link JdbcBatchingOutputFormat} for Table/SQL.
 */
public class JdbcDynamicOutputFormatBuilder implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDynamicOutputFormatBuilder.class);

    private static final long serialVersionUID = 1L;

    private JdbcOptions jdbcOptions;
    private JdbcExecutionOptions executionOptions;
    private JdbcDmlOptions dmlOptions;
    private boolean appendMode;
    private TypeInformation<RowData> rowDataTypeInformation;
    private DataType[] fieldDataTypes;
    private String inlongMetric;
    private String auditHostAndPorts;
    private String sinkMultipleFormat;
    private String databasePattern;
    private String tablePattern;
    private String schemaPattern;
    private SchemaUpdateExceptionPolicy schemaUpdateExceptionPolicy;
    private DirtyOptions dirtyOptions;
    private DirtySink<Object> dirtySink;
    private boolean enableSchemaChange;
    private String schemaChangePolicies;
    private boolean autoCreateTableWhenSnapshot;

    private int concurrencyWrite = 1;

    public JdbcDynamicOutputFormatBuilder() {

    }

    private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(PostgresDialect dialect,
            String tableName, String[] pkNames, LogicalType[] pkTypes, int[] pkFields) {
        return new TableBatchDeleteExecutor(dialect, pkNames, pkTypes, tableName, pkFields);
    }

    private static JdbcBatchStatementExecutor<RowData> createInsertOrUpdateExecutor(
            JdbcDialect dialect,
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            int[] pkFields,
            String[] pkNames,
            LogicalType[] pkTypes) {
        final String existStmt = dialect.getRowExistsStatement(tableName, pkNames);
        final String insertStmt = dialect.getInsertIntoStatement(tableName, fieldNames);
        final String updateStmt = dialect.getUpdateStatement(tableName, fieldNames, pkNames);
        return new TableInsertOrUpdateStatementExecutor(
                connection -> FieldNamedPreparedStatement.prepareStatement(
                        connection, existStmt, pkNames),
                connection -> FieldNamedPreparedStatement.prepareStatement(
                        connection, insertStmt, fieldNames),
                connection -> FieldNamedPreparedStatement.prepareStatement(
                        connection, updateStmt, fieldNames),
                dialect.getRowConverter(RowType.of(pkTypes)),
                dialect.getRowConverter(RowType.of(fieldTypes)),
                dialect.getRowConverter(RowType.of(fieldTypes)),
                createRowKeyExtractor(fieldTypes, pkFields));
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

    private JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
            JdbcDmlOptions opt,
            RuntimeContext ctx,
            TypeInformation<RowData> rowDataTypeInfo,
            LogicalType[] fieldTypes) {
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
                        fieldTypes),
                createInsertRowExecutor(
                        dialect,
                        tableName,
                        opt.getFieldNames(),
                        fieldTypes),
                createInsertRowExecutor(
                        dialect,
                        tableName,
                        opt.getFieldNames(),
                        fieldTypes),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes, pkFields),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes, pkFields),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes, pkFields),
                createRowKeyExtractor(fieldTypes, pkFields),
                valueTransform);
    }

    private JdbcBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
            RuntimeContext ctx,
            PostgresDialect dialect,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            String sql,
            TypeInformation<RowData> rowDataTypeInfo) {
        final TypeSerializer<RowData> typeSerializer =
                rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
        return new TableBufferedStatementExecutor(
                createSimpleRowExecutor(dmlOptions.getTableName(), dialect, fieldNames, fieldTypes, sql),
                ctx.getExecutionConfig().isObjectReuseEnabled()
                        ? typeSerializer::copy
                        : Function.identity());
    }

    private JdbcBatchStatementExecutor<RowData> createInsertRowExecutor(
            PostgresDialect dialect,
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes) {
        String insertSql = dialect.getInsertIntoStatement(tableName, fieldNames);
        return createSimpleRowExecutor(tableName, dialect, fieldNames, fieldTypes, insertSql);
    }

    private JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(String tableName, PostgresDialect dialect,
            String[] fieldNames, LogicalType[] fieldTypes, final String sql) {
        final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));
        if (PostgresDialect.WRITE_MODE_COPY.equals(getExecutionOptions().getWriteMode())) {
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

    public JdbcDynamicOutputFormatBuilder setAppendMode(boolean appendMode) {
        this.appendMode = appendMode;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setJdbcOptions(JdbcOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setJdbcExecutionOptions(
            JdbcExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public JdbcExecutionOptions getExecutionOptions() {
        return executionOptions;
    }

    public JdbcDynamicOutputFormatBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
        this.dmlOptions = dmlOptions;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setRowDataTypeInfo(
            TypeInformation<RowData> rowDataTypeInfo) {
        this.rowDataTypeInformation = rowDataTypeInfo;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setInLongMetric(String inlongMetric) {
        this.inlongMetric = inlongMetric;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setAuditHostAndPorts(String auditHostAndPorts) {
        this.auditHostAndPorts = auditHostAndPorts;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setDirtyOptions(DirtyOptions dirtyOptions) {
        this.dirtyOptions = dirtyOptions;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setDirtySink(DirtySink<Object> dirtySink) {
        this.dirtySink = dirtySink;
        return this;
    }

    public JdbcBatchingOutputFormat<RowData, ?, ?> build() {
        checkNotNull(jdbcOptions, "jdbc options can not be null");
        checkNotNull(dmlOptions, "jdbc dml options can not be null");
        checkNotNull(executionOptions, "jdbc execution options can not be null");

        final LogicalType[] logicalTypes =
                Arrays.stream(fieldDataTypes)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);
        if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0 && !appendMode) {
            // upsert query
            return new JdbcBatchingOutputFormat<>(
                    new SimpleJdbcConnectionProvider(jdbcOptions),
                    executionOptions,
                    ctx -> createBufferReduceExecutor(
                            dmlOptions, ctx, rowDataTypeInformation, logicalTypes),
                    JdbcBatchingOutputFormat.RecordExtractor.identity(),
                    inlongMetric,
                    auditHostAndPorts,
                    dirtyOptions,
                    dirtySink);
        } else {
            // append only query
            final String sql =
                    dmlOptions
                            .getDialect()
                            .getInsertIntoStatement(
                                    dmlOptions.getTableName(), dmlOptions.getFieldNames());

            return new JdbcBatchingOutputFormat<>(
                    new SimpleJdbcConnectionProvider(jdbcOptions),
                    executionOptions,
                    ctx -> createBufferReduceExecutor(
                            dmlOptions, ctx, rowDataTypeInformation, logicalTypes),
                    JdbcBatchingOutputFormat.RecordExtractor.identity(),
                    inlongMetric,
                    auditHostAndPorts,
                    dirtyOptions,
                    dirtySink);
        }
    }

    public JdbcDynamicOutputFormatBuilder setSinkMultipleFormat(String sinkMultipleFormat) {
        this.sinkMultipleFormat = sinkMultipleFormat;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setDatabasePattern(String databasePattern) {
        this.databasePattern = databasePattern;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setTablePattern(String tablePattern) {
        this.tablePattern = tablePattern;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setSchemaPattern(String schemaPattern) {
        this.schemaPattern = schemaPattern;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setSchemaUpdatePolicy(
            SchemaUpdateExceptionPolicy schemaUpdateExceptionPolicy) {
        this.schemaUpdateExceptionPolicy = schemaUpdateExceptionPolicy;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setEnableSchemaChange(boolean enableSchemaChange) {
        this.enableSchemaChange = enableSchemaChange;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setAutoCreateTableWhenSnapshot(boolean autoCreateTableWhenSnapshot) {
        this.autoCreateTableWhenSnapshot = autoCreateTableWhenSnapshot;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setSchemaChangePolicies(String schemaChangePolicies) {
        this.schemaChangePolicies = schemaChangePolicies;
        return this;
    }

    public JdbcDynamicOutputFormatBuilder setConcurrencyWrite(int concurrencyWrite) {
        this.concurrencyWrite = concurrencyWrite;
        return this;
    }

    public JdbcMultiBatchingOutputFormat<RowData, ?, ?> buildMulti() {
        checkNotNull(jdbcOptions, "jdbc options can not be null");
        checkNotNull(dmlOptions, "jdbc dml options can not be null");
        checkNotNull(executionOptions, "jdbc execution options can not be null");
        final DirtySinkHelper<Object> dirtySinkHelper = new DirtySinkHelper<>(dirtyOptions, dirtySink);
        return new JdbcMultiBatchingOutputFormat<>(
                new SimpleJdbcConnectionProvider(jdbcOptions),
                executionOptions,
                dmlOptions,
                appendMode,
                jdbcOptions,
                sinkMultipleFormat,
                databasePattern,
                tablePattern,
                schemaPattern,
                inlongMetric,
                auditHostAndPorts,
                schemaUpdateExceptionPolicy,
                dirtySinkHelper,
                enableSchemaChange,
                schemaChangePolicies,
                autoCreateTableWhenSnapshot,
                concurrencyWrite);
    }
}
