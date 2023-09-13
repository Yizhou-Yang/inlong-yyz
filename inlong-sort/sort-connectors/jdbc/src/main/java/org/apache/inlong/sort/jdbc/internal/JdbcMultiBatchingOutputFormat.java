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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableSimpleStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.statement.StatementFactory;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.sub.SinkTableMetricData;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.base.util.MetricStateUtils;
import org.apache.inlong.sort.jdbc.schema.JdbcSchemaSchangeHelper;
import org.apache.inlong.sort.jdbc.table.AbstractJdbcDialect;
import org.apache.inlong.sort.util.SchemaChangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.inlong.sort.base.Constants.DIRTY_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.DIRTY_RECORDS_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC_STATE_NAME;

/**
 * A JDBC multi-table outputFormat that supports batching records before writing records to databases.
 * Add an option `inlong.metric` to support metrics.
 */
public class JdbcMultiBatchingOutputFormat<In, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>>
        extends
            AbstractJdbcOutputFormat<In> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcMultiBatchingOutputFormat.class);
    private static final DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
    private static final DateTimeFormatter SQL_TIME_FORMAT;

    private final JdbcExecutionOptions executionOptions;
    private final String inlongMetric;
    private final String auditHostAndPorts;
    private final String sinkMultipleFormat;
    private final String databasePattern;
    private final String tablePattern;
    private final String schemaPattern;
    private final SchemaUpdateExceptionPolicy schemaUpdateExceptionPolicy;
    private final DirtySinkHelper<Object> dirtySinkHelper;
    private final JdbcDmlOptions dmlOptions;
    private final JdbcOptions jdbcOptions;
    private final boolean appendMode;
    private long totalDataSize = 0L;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient RuntimeContext runtimeContext;
    private transient JsonDynamicSchemaFormat jsonDynamicSchemaFormat;
    private transient Map<String, JdbcExec> jdbcExecMap = new HashMap<>();
    private transient Map<String, SimpleJdbcConnectionProvider> connectionExecProviderMap = new HashMap<>();
    private transient Map<String, RowType> rowTypeMap = new HashMap<>();
    private transient Map<String, List<String>> pkNameMap = new HashMap<>();
    private transient Map<String, List<GenericRowData>> recordsMap = new HashMap<>();
    private transient Map<String, Exception> tableExceptionMap = new HashMap<>();
    private transient Boolean stopWritingWhenTableException;

    private transient ListState<MetricState> metricStateListState;
    private transient MetricState metricState;
    private SinkTableMetricData sinkMetricData;
    private final boolean enableSchemaChange;
    private @Nullable final String schemaChangePolicies;
    private final boolean autoCreateTableWhenSnapshot;
    private JdbcSchemaSchangeHelper helper;
    private final Map<String, JsonNode> firstDataMap = new HashMap<>();

    static {
        SQL_TIME_FORMAT = (new DateTimeFormatterBuilder()).appendPattern("HH:mm:ss")
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
        SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
                (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T')
                        .append(SQL_TIME_FORMAT).appendPattern("'Z'").toFormatter();
    }

    public JdbcMultiBatchingOutputFormat(
            @Nonnull JdbcConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionOptions executionOptions,
            @Nonnull JdbcDmlOptions dmlOptions,
            @Nonnull boolean appendMode,
            @Nonnull JdbcOptions jdbcOptions,
            String sinkMultipleFormat,
            String databasePattern,
            String tablePattern,
            String schemaPattern,
            String inlongMetric,
            String auditHostAndPorts,
            SchemaUpdateExceptionPolicy schemaUpdateExceptionPolicy,
            DirtySinkHelper<Object> dirtySinkHelper,
            boolean enableSchemaChange,
            @Nullable String schemaChangePolicies,
            boolean autoCreateTableWhenSnapshot) {
        super(connectionProvider);
        this.executionOptions = checkNotNull(executionOptions);
        this.dmlOptions = dmlOptions;
        this.appendMode = appendMode;
        this.jdbcOptions = jdbcOptions;
        this.sinkMultipleFormat = sinkMultipleFormat;
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
        this.schemaPattern = schemaPattern;
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
        this.schemaUpdateExceptionPolicy = schemaUpdateExceptionPolicy;
        this.dirtySinkHelper = dirtySinkHelper;
        this.enableSchemaChange = enableSchemaChange;
        this.schemaChangePolicies = schemaChangePolicies;
        this.autoCreateTableWhenSnapshot = autoCreateTableWhenSnapshot;
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     *
     * @param taskNumber The number of the parallel instance.
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.runtimeContext = getRuntimeContext();
        MetricOption metricOption = MetricOption.builder()
                .withInlongLabels(inlongMetric)
                .withInlongAudit(auditHostAndPorts)
                .withInitRecords(metricState != null ? metricState.getMetricValue(NUM_RECORDS_OUT) : 0L)
                .withInitBytes(metricState != null ? metricState.getMetricValue(NUM_BYTES_OUT) : 0L)
                .withInitDirtyRecords(metricState != null ? metricState.getMetricValue(DIRTY_RECORDS_OUT) : 0L)
                .withInitDirtyBytes(metricState != null ? metricState.getMetricValue(DIRTY_BYTES_OUT) : 0L)
                .withRegisterMetric(MetricOption.RegisteredMetric.ALL)
                .build();
        if (metricOption != null) {
            sinkMetricData = new SinkTableMetricData(metricOption, runtimeContext.getMetricGroup());
            sinkMetricData.registerSubMetricsGroup(metricState);
        }
        jdbcExecMap = new HashMap<>();
        connectionExecProviderMap = new HashMap<>();
        pkNameMap = new HashMap<>();
        rowTypeMap = new HashMap<>();
        recordsMap = new HashMap<>();
        tableExceptionMap = new HashMap<>();
        stopWritingWhenTableException =
                schemaUpdateExceptionPolicy.equals(SchemaUpdateExceptionPolicy.ALERT_WITH_IGNORE)
                        || schemaUpdateExceptionPolicy.equals(SchemaUpdateExceptionPolicy.STOP_PARTIAL);
        dirtySinkHelper.open(new Configuration());
        jsonDynamicSchemaFormat =
                (JsonDynamicSchemaFormat) DynamicSchemaFormatFactory.getFormat(sinkMultipleFormat);
        helper = JdbcSchemaSchangeHelper.of(jsonDynamicSchemaFormat, enableSchemaChange,
                enableSchemaChange ? SchemaChangeUtils.deserialize(schemaChangePolicies) : null, databasePattern,
                schemaPattern, tablePattern, schemaUpdateExceptionPolicy, sinkMetricData, dirtySinkHelper,
                autoCreateTableWhenSnapshot, this, firstDataMap, rowTypeMap,
                executionOptions.getMaxRetries());
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("jdbc-upsert-output-format"));
            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (JdbcMultiBatchingOutputFormat.this) {
                                    if (!closed) {
                                        try {
                                            flush();
                                        } catch (Exception e) {
                                            LOG.info("Synchronized flush get Exception:", e);
                                        }
                                    }
                                }
                            },
                            executionOptions.getBatchIntervalMs(),
                            executionOptions.getBatchIntervalMs(),
                            TimeUnit.MILLISECONDS);
        }
    }

    /**
     * get or create  StatementExecutor for one table.
     *
     * @param tableIdentifier The tabxle identifier for which to get statementExecutor.
     */
    private JdbcExec getOrCreateStatementExecutor(
            String tableIdentifier) throws IOException {
        if (StringUtils.isBlank(tableIdentifier)) {
            return null;
        }
        JdbcExec jdbcExec = jdbcExecMap.get(tableIdentifier);
        if (null != jdbcExec) {
            return jdbcExec;
        }
        if (!pkNameMap.containsKey(tableIdentifier)) {
            getAndSetPkNamesFromDb(tableIdentifier);
        }
        RowType rowType = rowTypeMap.get(tableIdentifier);
        LogicalType[] logicalTypes = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
        String[] filedNames = rowType.getFields().stream()
                .map(RowType.RowField::getName)
                .toArray(String[]::new);
        TypeInformation<RowData> rowDataTypeInfo = InternalTypeInfo.of(rowType);
        List<String> pkNameList = null;
        if (null != pkNameMap.get(tableIdentifier)) {
            pkNameList = pkNameMap.get(tableIdentifier);
        }
        StatementExecutorFactory<JdbcExec> statementExecutorFactory;
        if (CollectionUtils.isNotEmpty(pkNameList) && !appendMode) {
            // upsert query
            JdbcDmlOptions createDmlOptions = JdbcDmlOptions.builder()
                    .withTableName(JdbcMultiBatchingComm.getTableNameFromIdentifier(tableIdentifier))
                    .withDialect(jdbcOptions.getDialect())
                    .withFieldNames(filedNames)
                    .withKeyFields(pkNameList.toArray(new String[pkNameList.size()]))
                    .build();
            statementExecutorFactory = ctx -> (JdbcExec) JdbcMultiBatchingComm.createBufferReduceExecutor(
                    createDmlOptions, ctx, rowDataTypeInfo, logicalTypes);

        } else {
            // append only query
            final String sql = dmlOptions
                    .getDialect()
                    .getInsertIntoStatement(JdbcMultiBatchingComm.getTableNameFromIdentifier(tableIdentifier),
                            filedNames);
            statementExecutorFactory = ctx -> (JdbcExec) JdbcMultiBatchingComm.createSimpleBufferedExecutor(
                    ctx,
                    dmlOptions.getDialect(),
                    filedNames,
                    logicalTypes,
                    sql,
                    rowDataTypeInfo);
        }

        jdbcExec = statementExecutorFactory.apply(getRuntimeContext());
        try {
            JdbcOptions jdbcExecOptions = JdbcMultiBatchingComm.getExecJdbcOptions(jdbcOptions, tableIdentifier);
            SimpleJdbcConnectionProvider tableConnectionProvider = new SimpleJdbcConnectionProvider(jdbcExecOptions);
            try {
                tableConnectionProvider.getOrEstablishConnection();
            } catch (SQLException e) {
                LOG.error("unable to open JDBC writer, tableIdentifier:{} err:", tableIdentifier, e);
                if (helper.handleResourceNotExists(tableIdentifier, e)) {
                    tableConnectionProvider.reestablishConnection();
                } else {
                    return null;
                }
            }
            connectionExecProviderMap.put(tableIdentifier, tableConnectionProvider);
            if (schemaUpdateExceptionPolicy.equals(SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE)) {
                try {
                    JdbcExec newExecutor = enhanceExecutor(jdbcExec, tableIdentifier);
                    if (newExecutor != null) {
                        jdbcExec = newExecutor;
                    }
                } catch (Exception e) {
                    LOG.warn("enhance executor failed for class :" + jdbcExec.getClass(), e);
                }
            }
            jdbcExec.prepareStatements(tableConnectionProvider.getConnection());
        } catch (Exception e) {
            return null;
        }
        jdbcExecMap.put(tableIdentifier, jdbcExec);
        return jdbcExec;
    }

    private JdbcExec enhanceExecutor(JdbcExec exec, String tableIdentifier)
            throws NoSuchFieldException, IllegalAccessException {
        // enhance the actual executor to tablemetricstatementexecutor
        Field subExecutor;
        if (exec instanceof TableBufferReducedStatementExecutor) {
            subExecutor = TableBufferReducedStatementExecutor.class.getDeclaredField("upsertExecutor");
        } else if (exec instanceof TableBufferedStatementExecutor) {
            subExecutor = TableBufferedStatementExecutor.class.getDeclaredField("statementExecutor");
        } else {
            throw new RuntimeException("table enhance failed, can't enhance " + exec.getClass());
        }
        subExecutor.setAccessible(true);
        TableSimpleStatementExecutor executor = (TableSimpleStatementExecutor) subExecutor.get(exec);
        Field statementFactory = TableSimpleStatementExecutor.class.getDeclaredField("stmtFactory");
        Field rowConverter = TableSimpleStatementExecutor.class.getDeclaredField("converter");
        statementFactory.setAccessible(true);
        rowConverter.setAccessible(true);
        final StatementFactory stmtFactory = (StatementFactory) statementFactory.get(executor);
        final JdbcRowConverter converter = (JdbcRowConverter) rowConverter.get(executor);
        TableMetricStatementExecutor newExecutor =
                new TableMetricStatementExecutor(stmtFactory, converter, dirtySinkHelper, sinkMetricData,
                        tableIdentifier, helper);
        newExecutor.setMultipleSink(true);
        if (exec instanceof TableBufferedStatementExecutor) {
            subExecutor = TableBufferedStatementExecutor.class.getDeclaredField("valueTransform");
            subExecutor.setAccessible(true);
            Function<RowData, RowData> valueTransform = (Function<RowData, RowData>) subExecutor.get(exec);
            newExecutor.setValueTransform(valueTransform);
            return (JdbcExec) newExecutor;
        }
        subExecutor.set(exec, newExecutor);
        return null;
    }

    public void getAndSetPkNamesFromDb(String tableIdentifier) {
        try {
            AbstractJdbcDialect jdbcDialect = (AbstractJdbcDialect) jdbcOptions.getDialect();
            List<String> pkNames = jdbcDialect.getPkNames(tableIdentifier, jdbcOptions, 3);
            pkNameMap.put(tableIdentifier, pkNames);
        } catch (SQLException e) {
            helper.handleResourceNotExists(tableIdentifier, e);
            AbstractJdbcDialect jdbcDialect = (AbstractJdbcDialect) jdbcOptions.getDialect();
            try {
                pkNameMap.put(tableIdentifier, jdbcDialect.getPkNames(tableIdentifier, jdbcOptions, 3));
            } catch (Exception ex) {
                LOG.warn("TableIdentifier:{} getAndSetPkNamesFromDb get err:", tableIdentifier, ex);
            }
        } catch (Exception e) {
            LOG.warn("TableIdentifier:{} getAndSetPkNamesFromDb get err:", tableIdentifier, e);
        }
    }

    private void checkFlushException() {
        if (schemaUpdateExceptionPolicy.equals(SchemaUpdateExceptionPolicy.THROW_WITH_STOP)
                && !tableExceptionMap.isEmpty()) {
            String tableErr = "Writing table get failed, tables are:";
            for (Map.Entry<String, Exception> entry : tableExceptionMap.entrySet()) {
                LOG.error("Writing table:{} get err:{}", entry.getKey(), entry.getValue().getMessage());
                tableErr = tableErr + entry.getKey() + ",";
            }
            throw new RuntimeException(tableErr.substring(0, tableErr.length() - 1));
        }
    }

    /**
     * Extract and write record to recordsMap(buffer)
     *
     * @param row The record to write.
     */
    @Override
    public final synchronized void writeRecord(In row) throws IOException {
        checkFlushException();
        if (row instanceof RowData) {
            RowData rowData = (RowData) row;
            JsonNode rootNode;
            try {
                rootNode = jsonDynamicSchemaFormat.deserialize(rowData.getBinary(0));
            } catch (Exception e) {
                LOG.error(String.format("deserialize error, raw data: %s", new String(rowData.getBinary(0))), e);
                if (SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE == schemaUpdateExceptionPolicy) {
                    handleDirtyData(new String(rowData.getBinary(0)),
                            "unknown.unknown.unknown", DirtyType.DESERIALIZE_ERROR, e);
                }
                return;
            }
            boolean isDDL = jsonDynamicSchemaFormat.extractDDLFlag(rootNode);
            if (isDDL) {
                helper.process(rowData.getBinary(0), rootNode);
                return;
            }
            String tableIdentifier;
            String database;
            String table;
            String schema;
            try {
                if (StringUtils.isBlank(schemaPattern)) {
                    database = jsonDynamicSchemaFormat.parse(rootNode, databasePattern);
                    table = jsonDynamicSchemaFormat.parse(rootNode, tablePattern);
                    tableIdentifier = StringUtils.join(database, ".", table);
                } else {
                    database = jsonDynamicSchemaFormat.parse(rootNode, databasePattern);
                    table = jsonDynamicSchemaFormat.parse(rootNode, tablePattern);
                    schema = jsonDynamicSchemaFormat.parse(rootNode, schemaPattern);
                    tableIdentifier = StringUtils.join(
                            database, ".",
                            schema, ".",
                            table);
                }
            } catch (Exception e) {
                LOG.info("Cal tableIdentifier get Exception:", e);
                return;
            }
            GenericRowData record;
            try {
                RowType rowType = jsonDynamicSchemaFormat.extractSchema(rootNode);
                if (rowType != null) {
                    if (null != rowTypeMap.get(tableIdentifier)) {
                        if (!rowType.equals(rowTypeMap.get(tableIdentifier))) {
                            attemptFlush();
                            helper.applySchemaChange(tableIdentifier, rowTypeMap.get(tableIdentifier), rowType);
                            rowTypeMap.put(tableIdentifier, rowType);
                            firstDataMap.put(tableIdentifier, rootNode);
                            updateOneExecutor(true, tableIdentifier);
                        }
                    } else {
                        rowTypeMap.put(tableIdentifier, rowType);
                        firstDataMap.put(tableIdentifier, rootNode);
                    }
                }
                JsonNode physicalData = jsonDynamicSchemaFormat.getPhysicalData(rootNode);
                List<Map<String, String>> physicalDataList = jsonDynamicSchemaFormat.jsonNode2Map(physicalData);
                record = generateRecord(tableIdentifier, rowType, physicalDataList.get(0));
                List<RowKind> rowKinds = jsonDynamicSchemaFormat
                        .opType2RowKind(jsonDynamicSchemaFormat.getOpType(rootNode));
                record.setRowKind(rowKinds.get(rowKinds.size() - 1));
            } catch (Exception e) {
                throw new RuntimeException("Generate record failed", e);
            }
            try {
                recordsMap.computeIfAbsent(tableIdentifier, k -> new ArrayList<>())
                        .add(record);
                batchCount++;
                if (executionOptions.getBatchSize() > 0
                        && batchCount >= executionOptions.getBatchSize()) {
                    flush();
                }
            } catch (Exception e) {
                throw new IOException("Writing records to JDBC failed.", e);
            }
        }
    }

    private void fillDirtyData(JdbcExec exec, String tableIdentifier) {
        String[] identifiers = tableIdentifier.split("\\.");
        String database;
        String table;
        String schema = null;
        if (identifiers.length == 2) {
            database = identifiers[0];
            table = identifiers[1];
        } else {
            database = identifiers[0];
            schema = identifiers[1];
            table = identifiers[2];
        }
        TableMetricStatementExecutor executor = null;
        try {
            Field subExecutor;
            if (exec instanceof TableMetricStatementExecutor) {
                executor = (TableMetricStatementExecutor) exec;
            } else if (exec instanceof TableBufferReducedStatementExecutor) {
                subExecutor = TableBufferReducedStatementExecutor.class.getDeclaredField("upsertExecutor");
                subExecutor.setAccessible(true);
                executor = (TableMetricStatementExecutor) subExecutor.get(exec);
            } else if (exec instanceof TableBufferedStatementExecutor) {
                subExecutor = TableBufferedStatementExecutor.class.getDeclaredField("statementExecutor");
                subExecutor.setAccessible(true);
                executor = (TableMetricStatementExecutor) subExecutor.get(exec);
            }
        } catch (Exception e) {
            LOG.error("parse executor failed" + e);
        }

        try {
            DirtyOptions dirtyOptions = dirtySinkHelper.getDirtyOptions();
            String dirtyLabel = DirtySinkHelper.regexReplace(dirtyOptions.getLabels(), DirtyType.BATCH_LOAD_ERROR, null,
                    database, table, schema);
            String dirtyLogTag =
                    DirtySinkHelper.regexReplace(dirtyOptions.getLogTag(), DirtyType.BATCH_LOAD_ERROR, null,
                            database, table, schema);
            String dirtyIdentifier =
                    DirtySinkHelper.regexReplace(dirtyOptions.getIdentifier(), DirtyType.BATCH_LOAD_ERROR,
                            null, database, table, schema);

            if (executor != null) {
                executor.setDirtyMetaData(dirtyLabel, dirtyLogTag, dirtyIdentifier);
            } else {
                LOG.error("null executor");
            }
        } catch (Exception e) {
            LOG.error("filling dirty metadata failed" + e);
        }
    }

    public void handleDirtyData(Object data, String tableIdentifier, DirtyType dirtyType, Throwable e) {
        String[] identifiers = tableIdentifier.split("\\.");
        String database;
        String table;
        String schema = null;
        if (identifiers.length == 2) {
            database = identifiers[0];
            table = identifiers[1];
        } else {
            database = identifiers[0];
            schema = identifiers[1];
            table = identifiers[2];
        }
        DirtyOptions dirtyOptions = dirtySinkHelper.getDirtyOptions();
        String dirtyLabel = DirtySinkHelper.regexReplace(dirtyOptions.getLabels(), DirtyType.BATCH_LOAD_ERROR, null,
                database, table, schema);
        String dirtyLogTag =
                DirtySinkHelper.regexReplace(dirtyOptions.getLogTag(), DirtyType.BATCH_LOAD_ERROR, null,
                        database, table, schema);
        String dirtyIdentifier =
                DirtySinkHelper.regexReplace(dirtyOptions.getIdentifier(), DirtyType.BATCH_LOAD_ERROR,
                        null, database, table, schema);
        dirtySinkHelper.invoke(data, dirtyType, dirtyLabel, dirtyLogTag, dirtyIdentifier, e);
    }

    /**
     * Convert fieldMap(data) to GenericRowData with rowType(schema)
     */
    protected GenericRowData generateRecord(String tableIdentifier, RowType rowType, Map<String, String> fieldMap) {
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        int arity = fieldNames.length;
        GenericRowData record = new GenericRowData(arity);
        for (int i = 0; i < arity; i++) {
            String fieldName = fieldNames[i];
            String fieldValue = fieldMap.get(fieldName);
            if (StringUtils.isBlank(fieldValue)) {
                record.setField(i, null);
            } else {
                try {
                    switch (rowType.getFields().get(i).getType().getTypeRoot()) {
                        case BIGINT:
                            record.setField(i, Long.valueOf(fieldValue));
                            break;
                        case BOOLEAN:
                            record.setField(i, Boolean.valueOf(fieldValue));
                            break;
                        case DOUBLE:
                            record.setField(i, Double.valueOf(fieldValue));
                            break;
                        case DECIMAL:
                            DecimalType decimalType = (DecimalType) rowType.getFields().get(i).getType();
                            record.setField(i, DecimalData.fromBigDecimal(new BigDecimal(fieldValue),
                                    decimalType.getPrecision(), decimalType.getScale()));
                            break;
                        case TIME_WITHOUT_TIME_ZONE:
                        case INTERVAL_DAY_TIME:
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                            LocalTime time = LocalTime.parse(fieldValue, formatter);
                            LocalDateTime dateTime = LocalDateTime.of(LocalDate.now(), time);
                            TimestampData timestampData = TimestampData.fromInstant(dateTime.toInstant(ZoneOffset.UTC));
                            record.setField(i, timestampData);
                            break;
                        case VARBINARY:
                        case BINARY:
                            record.setField(i, fieldValue.getBytes(StandardCharsets.UTF_8));
                            break;
                        // support mysql
                        case INTEGER:
                            record.setField(i, Integer.valueOf(fieldValue));
                            break;
                        case SMALLINT:
                            record.setField(i, Short.valueOf(fieldValue));
                            break;
                        case TINYINT:
                            record.setField(i, Byte.valueOf(fieldValue));
                            break;
                        case FLOAT:
                            record.setField(i, Float.valueOf(fieldValue));
                            break;
                        case DATE:
                            record.setField(i, (int) LocalDate.parse(fieldValue).toEpochDay());
                            break;
                        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                            TemporalAccessor parsedTimestampWithLocalZone =
                                    SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(fieldValue);
                            LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
                            LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());
                            record.setField(i, TimestampData.fromInstant(LocalDateTime.of(localDate, localTime)
                                    .toInstant(ZoneOffset.UTC)));
                            break;
                        case TIMESTAMP_WITHOUT_TIME_ZONE:
                            TimestampData timestamp;
                            try {
                                timestamp = TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse(fieldValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                            } catch (Exception ex) {
                                LOG.warn(
                                        "Parse value: {} by timestamp without time zone failed for field:{} of table:{}",
                                        fieldValue, fieldName, tableIdentifier, ex);
                                timestamp = TimestampData.fromInstant(Instant.parse(fieldValue));
                            }
                            record.setField(i, timestamp);
                            break;
                        default:
                            record.setField(i, StringData.fromString(fieldValue));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("Parse value: %s failed for field:%s of table:%s",
                                    fieldName, fieldValue, tableIdentifier),
                            e);
                }
            }
        }
        return record;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (sinkMetricData != null && metricStateListState != null) {
            MetricStateUtils.snapshotMetricStateForSinkMetricData(metricStateListState, sinkMetricData,
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (this.inlongMetric != null) {
            this.metricStateListState = context.getOperatorStateStore().getUnionListState(
                    new ListStateDescriptor<>(
                            INLONG_METRIC_STATE_NAME, TypeInformation.of(new TypeHint<MetricState>() {
                            })));

        }
        if (context.isRestored()) {
            metricState = MetricStateUtils.restoreMetricState(metricStateListState,
                    getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        checkFlushException();
        attemptFlush();
        batchCount = 0;
    }

    /**
     * Write all recorde from recordsMap to db
     *
     * First batch writing.
     * If batch-writing occur exception, then rewrite one-by-one retry-times set by user.
     */
    protected void attemptFlush() throws IOException {
        for (Map.Entry<String, List<GenericRowData>> entry : recordsMap.entrySet()) {
            String tableIdentifier = entry.getKey();
            boolean stopTableIdentifierWhenException = stopWritingWhenTableException
                    && (null != tableExceptionMap.get(tableIdentifier));
            if (stopTableIdentifierWhenException) {
                continue;
            }
            List<GenericRowData> tableIdRecordList = entry.getValue();
            if (CollectionUtils.isEmpty(tableIdRecordList)) {
                continue;
            }

            JdbcExec jdbcStatementExecutor = null;
            Boolean flushFlag = false;
            Exception tableException = null;
            boolean hasExecuteBatch = false;
            try {
                jdbcStatementExecutor = getOrCreateStatementExecutor(tableIdentifier);
                totalDataSize = 0L;
                for (GenericRowData record : tableIdRecordList) {
                    totalDataSize = totalDataSize + record.toString().getBytes(StandardCharsets.UTF_8).length;
                    jdbcStatementExecutor.addToBatch((JdbcIn) record);
                }
                if (dirtySinkHelper.getDirtySink() != null) {
                    fillDirtyData(jdbcStatementExecutor, tableIdentifier);
                }
                hasExecuteBatch = true;
                jdbcStatementExecutor.executeBatch();
                flushFlag = true;
                if (!schemaUpdateExceptionPolicy.equals(SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE)) {
                    outputMetrics(tableIdentifier, (long) tableIdRecordList.size(),
                            totalDataSize, false);
                } else {
                    try {
                        outputMetrics(tableIdRecordList.size(), tableIdentifier);
                    } catch (Exception e) {
                        LOG.error("dirty metric calculation exception:{}", e);
                        outputMetrics(tableIdentifier, (long) tableIdRecordList.size(),
                                totalDataSize, false);
                    }
                }
            } catch (SQLException e) {
                LOG.warn("execute batch error", e);
                if (hasExecuteBatch && helper.handleResourceNotExists(tableIdentifier, e)) {
                    try {
                        jdbcStatementExecutor.executeBatch();
                        tableIdRecordList.clear();
                        continue;
                    } catch (SQLException ex) {
                        tableException = ex;
                        LOG.warn("Flush all data for tableIdentifier:{} get err:", tableIdentifier, ex);
                        if (dirtySinkHelper.getDirtySink() == null) {
                            getAndSetPkFromErrMsg(tableIdentifier, e.getMessage());
                            updateOneExecutor(true, tableIdentifier);
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                            throw new IOException(
                                    "unable to flush; interrupted while doing another attempt", interruptedException);
                        }
                    }
                }
            } catch (Exception e) {
                tableException = e;
                LOG.warn("Flush all data for tableIdentifier:{} get err:", tableIdentifier, e);
                if (dirtySinkHelper.getDirtySink() == null) {
                    getAndSetPkFromErrMsg(tableIdentifier, e.getMessage());
                    updateOneExecutor(true, tableIdentifier);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }

            if (!flushFlag) {
                if (schemaUpdateExceptionPolicy.equals(SchemaUpdateExceptionPolicy.STOP_PARTIAL)) {
                    LOG.error("exception detected, skipping table " + tableIdentifier);
                    tableExceptionMap.put(tableIdentifier, tableException);
                    tableIdRecordList.clear();
                    continue;
                }

                if (schemaUpdateExceptionPolicy.equals(SchemaUpdateExceptionPolicy.THROW_WITH_STOP)) {
                    LOG.error("exception detected, restarting entire task");
                    throw new RuntimeException(tableException);
                }

                for (GenericRowData record : tableIdRecordList) {
                    for (int retryTimes = 1; retryTimes <= executionOptions.getMaxRetries(); retryTimes++) {
                        try {
                            jdbcStatementExecutor = getOrCreateStatementExecutor(tableIdentifier);
                            jdbcStatementExecutor.addToBatch((JdbcIn) record);
                            jdbcStatementExecutor.executeBatch();
                            Long totalDataSize =
                                    Long.valueOf(record.toString().getBytes(StandardCharsets.UTF_8).length);
                            outputMetrics(tableIdentifier, 1L, totalDataSize, false);
                            flushFlag = true;
                            break;
                        } catch (Exception e) {
                            LOG.warn("Flush one record tableIdentifier:{} ,retryTimes:{} get err:",
                                    tableIdentifier, retryTimes, e);
                            getAndSetPkFromErrMsg(e.getMessage(), tableIdentifier);
                            updateOneExecutor(true, tableIdentifier);
                            try {
                                Thread.sleep(1000 * retryTimes);
                            } catch (InterruptedException ex) {
                                Thread.currentThread().interrupt();
                                throw new IOException(
                                        "unable to flush; interrupted while doing another attempt", e);
                            }
                        }
                    }
                    if (!flushFlag && null != tableException) {
                        LOG.info("Put tableIdentifier:{} exception:{}",
                                tableIdentifier, tableException.getMessage());
                        if (dirtySinkHelper.getDirtySink() == null &&
                                !schemaUpdateExceptionPolicy.equals(SchemaUpdateExceptionPolicy.THROW_WITH_STOP)) {
                            outputMetrics(tableIdentifier, Long.valueOf(tableIdRecordList.size()),
                                    1L, true);
                        }
                        tableExceptionMap.put(tableIdentifier, tableException);
                        if (stopWritingWhenTableException) {
                            LOG.info("Stop write table:{} because occur exception",
                                    tableIdentifier);
                            break;
                        }
                    }
                }
                outputMetrics(tableIdentifier, (long) tableIdRecordList.size(), totalDataSize, false);
            }
            tableIdRecordList.clear();
        }
    }

    /**
     * Output metrics with estimate for pg or other type jdbc connectors.
     * tableIdentifier maybe: ${dbName}.${tbName} or ${dbName}.${schemaName}.${tbName}
     */
    public void outputMetrics(String tableIdentifier, long rowSize, long dataSize, boolean dirtyFlag) {
        String[] fieldArray = tableIdentifier.split("\\.");
        if (fieldArray.length == 3) {
            if (dirtyFlag) {
                sinkMetricData.outputDirtyMetrics(fieldArray[0], fieldArray[1], fieldArray[2],
                        rowSize, dataSize);
            } else {
                sinkMetricData.outputMetrics(fieldArray[0], fieldArray[1], fieldArray[2],
                        rowSize, dataSize);
            }
        } else if (fieldArray.length == 2) {
            if (dirtyFlag) {
                sinkMetricData.outputDirtyMetrics(fieldArray[0], null, fieldArray[1],
                        rowSize, dataSize);
            } else {
                sinkMetricData.outputMetrics(fieldArray[0], null, fieldArray[1],
                        rowSize, dataSize);
            }
        }
    }

    private void outputMetrics(long totalCount, String tableIdentifier)
            throws NoSuchFieldException, IllegalAccessException {
        String[] fieldArray = tableIdentifier.split("\\.");
        // throw an exception if the executor is not enhanced
        JdbcExec executor = jdbcExecMap.get(tableIdentifier);
        Field subExecutor;
        if (executor instanceof TableBufferReducedStatementExecutor) {
            subExecutor = TableBufferReducedStatementExecutor.class.getDeclaredField("upsertExecutor");
            subExecutor.setAccessible(true);
            executor = (JdbcExec) subExecutor.get(executor);
        } else if (executor instanceof TableBufferedStatementExecutor) {
            subExecutor = TableBufferedStatementExecutor.class.getDeclaredField("statementExecutor");
            subExecutor.setAccessible(true);
            executor = (JdbcExec) subExecutor.get(executor);
        }
        Field metricField = TableMetricStatementExecutor.class.getDeclaredField("metric");
        long[] metrics = (long[]) metricField.get(executor);
        long dirtyCount = metrics[2];
        long dirtySize = metrics[3];

        long cleanCount = totalCount - dirtyCount;
        long cleanSize = metrics[1];

        if (fieldArray.length == 3) {
            sinkMetricData.outputDirtyMetrics(fieldArray[0], fieldArray[1], fieldArray[2],
                    dirtyCount, dirtySize);
            sinkMetricData.outputMetrics(fieldArray[0], fieldArray[1], fieldArray[2],
                    cleanCount, cleanSize);
        } else if (fieldArray.length == 2) {
            sinkMetricData.outputDirtyMetrics(fieldArray[0], null, fieldArray[1],
                    dirtyCount, dirtySize);
            sinkMetricData.outputMetrics(fieldArray[0], null, fieldArray[1],
                    cleanCount, cleanSize);
        }
        metricField.set(executor, new long[4]);
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.warn("Writing records to JDBC failed.", e);
                    throw new RuntimeException("Writing records to JDBC failed.", e);
                }
            }

            try {
                if (null != jdbcExecMap) {
                    jdbcExecMap.forEach((tableIdentifier, jdbcExec) -> {
                        try {
                            jdbcExec.closeStatements();
                        } catch (SQLException e) {
                            LOG.error("jdbcExec executeBatch get err", e);
                        }
                    });
                }
            } catch (Exception e) {
                LOG.warn("Close JDBC writer failed.", e);
            }
        }
        super.close();
        checkFlushException();
    }

    public boolean getAndSetPkFromErrMsg(String errMsg, String tableIdentifier) {
        String rgex = "Detail: Key \\((.*?)\\)=\\(";
        Pattern pattern = Pattern.compile(rgex);
        Matcher m = pattern.matcher(errMsg);
        List<String> pkNameList = new ArrayList<>();
        if (m.find()) {
            String[] pkNameArray = m.group(1).split(",");
            for (String pkName : pkNameArray) {
                pkNameList.add(pkName.trim());
            }
            pkNameMap.put(tableIdentifier, pkNameList);
            return true;
        }
        return false;
    }

    public void updateOneExecutor(boolean reconnect, String tableIdentifier) {
        try {
            SimpleJdbcConnectionProvider tableConnectionProvider = connectionExecProviderMap.get(tableIdentifier);
            if (reconnect || null == tableConnectionProvider
                    || !tableConnectionProvider.isConnectionValid()) {
                JdbcExec tableJdbcExec = jdbcExecMap.get(tableIdentifier);
                if (null != tableJdbcExec) {
                    tableJdbcExec.closeStatements();
                    jdbcExecMap.remove(tableIdentifier);
                    getOrCreateStatementExecutor(tableIdentifier);
                }
            }
        } catch (SQLException | IOException e) {
            LOG.error("jdbcExec updateOneExecutor get err", e);
        }
    }

    public JdbcOptions getJdbcOptions() {
        return jdbcOptions;
    }

    /**
     * A factory for creating {@link JdbcBatchStatementExecutor} instance.
     *
     * @param <T> The type of instance.
     */
    public interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>>
            extends
                Function<RuntimeContext, T>,
                Serializable {

    }

}
