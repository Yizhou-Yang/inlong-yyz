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

package org.apache.inlong.sort.tchousex.flink.table;

import com.cdw.loader.core.CDWDataLoader;
import com.google.gson.Gson;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.inlong.sort.base.dirty.DirtyData;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.sub.SinkTableMetricData;
import org.apache.inlong.sort.base.schema.SchemaChangeHandleException;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.base.util.CalculateObjectSizeUtils;
import org.apache.inlong.sort.base.util.MetricStateUtils;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.apache.inlong.sort.tchousex.flink.catalog.Field;
import org.apache.inlong.sort.tchousex.flink.catalog.TCHouseXCatalog;
import org.apache.inlong.sort.tchousex.flink.catalog.Table;
import org.apache.inlong.sort.tchousex.flink.catalog.TableIdentifier;
import org.apache.inlong.sort.tchousex.flink.config.TCHouseXConfig;
import org.apache.inlong.sort.tchousex.flink.connection.JdbcClient;
import org.apache.inlong.sort.tchousex.flink.dml.TCHouseXJdbcDialect;
import org.apache.inlong.sort.tchousex.flink.model.TCHouseXData;
import org.apache.inlong.sort.tchousex.flink.option.TCHouseXOptions;
import org.apache.inlong.sort.tchousex.flink.option.TCHouseXSinkOptions;
import org.apache.inlong.sort.tchousex.flink.schema.TCHouseXSchemaChangeHelper;
import org.apache.inlong.sort.tchousex.flink.type.TCHouseXDataType;
import org.apache.inlong.sort.tchousex.flink.util.FlinkSchemaUtil;
import org.apache.inlong.sort.tchousex.flink.util.SchemaFormatUtil;
import org.apache.inlong.sort.tchousex.flink.util.TaskRunUtil;
import org.apache.inlong.sort.util.SchemaChangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.base.Constants.DIRTY_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.DIRTY_RECORDS_OUT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC_STATE_NAME;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;

public class TCHouseXDynamicOutputFormat<T> extends RichOutputFormat<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TCHouseXDynamicOutputFormat.class);
    private final String inlongMetric;
    private final String auditHostAndPorts;
    private final boolean multipleSink;
    private final String databasePattern;
    private final String tablePattern;
    private final String dynamicSchemaFormat;
    private final SchemaUpdateExceptionPolicy schemaUpdatePolicy;
    private final String[] fieldNames;
    private final LogicalType[] logicalTypes;
    private final List<String> primaryFields;
    private final boolean enableSchemaChange;
    @Nullable
    private final String schemaChangePolicies;
    private final DirtySinkHelper<Object> dirtySinkHelper;
    private boolean autoCreateTable;
    private TCHouseXSchemaChangeHelper schemaChangeHelper;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient JsonDynamicSchemaFormat jsonDynamicSchemaFormat;
    private transient SinkTableMetricData metricData;
    private transient ListState<MetricState> metricStateListState;
    private transient MetricState metricState;

    private JdbcClient jdbcClient;
    private TCHouseXCatalog tCHouseXCatalog;
    private Table table;
    private volatile RowData.FieldGetter[] fieldGetters;
    private final TCHouseXSinkOptions tCHouseXSinkOptions;
    private final TCHouseXOptions tCHouseXOptions;
    private final Map<TableIdentifier, TCHouseXData> batchMap = new ConcurrentHashMap<>();
    private final Map<String, CDWDataLoader> databaseMapLoader = new ConcurrentHashMap<>();
    private Long dataBytes = 0L;
    private Long dataRecords = 0L;
    private List<Integer> primaryFieldIndexes;
    private transient volatile boolean closed = false;
    private transient volatile boolean flushing = false;
    private final List<String> blackList = new ArrayList<>();
    private transient volatile Throwable flushException;
    private final Map<TableIdentifier, Exception> flushExceptionMap = new ConcurrentHashMap<>();
    private final Map<TableIdentifier, Table> tableIdMapTableInfo = new ConcurrentHashMap<>();
    private transient Gson gson;
    private Map<SchemaChangeType, SchemaChangePolicy> policyMap = new HashMap<>();
    private transient DateTimeFormatter formatter;

    public TCHouseXDynamicOutputFormat(TCHouseXOptions tCHouseXOptions, TCHouseXSinkOptions tCHouseXSinkOptions,
            String inlongMetric, String auditHostAndPorts, boolean multipleSink,
            String databasePattern, String tablePattern, String dynamicSchemaFormat,
            SchemaUpdateExceptionPolicy schemaUpdatePolicy, String[] fieldNames,
            LogicalType[] logicalTypes, List<String> primaryFields, boolean enableSchemaChange,
            @Nullable String schemaChangePolicies, DirtyOptions dirtyOptions,
            DirtySink<Object> dirtySink) {
        this.tCHouseXOptions = tCHouseXOptions;
        this.tCHouseXSinkOptions = tCHouseXSinkOptions;
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
        this.multipleSink = multipleSink;
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
        this.dynamicSchemaFormat = dynamicSchemaFormat;
        this.schemaUpdatePolicy = schemaUpdatePolicy;
        this.fieldNames = fieldNames;
        this.logicalTypes = logicalTypes;
        this.primaryFields = primaryFields;
        this.enableSchemaChange = enableSchemaChange;
        this.schemaChangePolicies = schemaChangePolicies;
        this.dirtySinkHelper = new DirtySinkHelper<>(dirtyOptions, dirtySink);
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Configures this output format. Since output formats are instantiated generically and hence
     * parameterless, this method is the place where the output formats set their basic fields based
     * on configuration values.
     *
     * <p>This method is always called first on a newly instantiated output format.
     *
     * @param parameters The configuration with all parameters.
     */
    @Override
    public void configure(Configuration parameters) {

    }

    /**
     * Opens a parallel instance of the output format to store the result of its parallel instance.
     *
     * <p>When this method is called, the output format it guaranteed to be configured.
     *
     * @param taskNumber The number of the parallel instance.
     * @param numTasks The number of parallel tasks.
     * @throws IOException Thrown, if the output could not be opened due to an I/O problem.
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        TCHouseXConfig tcHouseXConfig = new TCHouseXConfig(tCHouseXOptions.getHostName(), tCHouseXOptions.getPort(),
                tCHouseXOptions.getUsername(), tCHouseXOptions.getPassword());
        jdbcClient = new JdbcClient(tcHouseXConfig);
        tCHouseXCatalog = new TCHouseXCatalog(jdbcClient);
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
            metricData = new SinkTableMetricData(metricOption, getRuntimeContext().getMetricGroup());
            if (multipleSink) {
                metricData.registerSubMetricsGroup(metricState);
            }
        }
        dirtySinkHelper.open(new Configuration());
        if (multipleSink) {
            jsonDynamicSchemaFormat = SchemaFormatUtil.getSchemaFormat(dynamicSchemaFormat);
            policyMap =
                    enableSchemaChange ? SchemaChangeUtils.deserialize(schemaChangePolicies) : Collections.emptyMap();
            SchemaChangePolicy schemaChangePolicy = policyMap.get(SchemaChangeType.CREATE_TABLE);
            autoCreateTable = schemaChangePolicy == SchemaChangePolicy.ENABLE;
            schemaChangeHelper = new TCHouseXSchemaChangeHelper(jsonDynamicSchemaFormat,
                    enableSchemaChange, policyMap, databasePattern, tablePattern, schemaUpdatePolicy, metricData,
                    dirtySinkHelper, tCHouseXCatalog, tableIdMapTableInfo);
        } else {
            table = checkAndGetTable();
            tableIdMapTableInfo.put(TableIdentifier.parse(tCHouseXOptions.getTableIdentifier()), table);
            fieldGetters = FlinkSchemaUtil.createFieldGetters(logicalTypes);
            primaryFieldIndexes = getPrimaryFieldIndexes();
            CDWDataLoader cdwDataLoader = new CDWDataLoader();
            try {
                String database =
                        TableIdentifier.parse(tCHouseXOptions.getTableIdentifier()).getDatabase();
                cdwDataLoader.setCdwConfig(tCHouseXOptions.getHostName(), tCHouseXOptions.getPort(),
                        tCHouseXOptions.getUsername(), tCHouseXOptions.getPassword(), database);
                databaseMapLoader.put(database, cdwDataLoader);
            } catch (Exception e) {
                LOG.error("init cdwDataLoader error", e);
                throw new RuntimeException("init cdwDataLoader error");
            }
        }

        // interval flush
        if (tCHouseXSinkOptions.getSinkBatchIntervalMs() != 0 && tCHouseXSinkOptions.getSinkBatchSize() != 1) {
            this.scheduler = new ScheduledThreadPoolExecutor(1,
                    new ExecutorThreadFactory("tchouse-x-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (TCHouseXDynamicOutputFormat.this) {
                    if (!closed && !flushing) {
                        try {
                            flush();
                        } catch (Throwable e) {
                            flushException = e;
                        }
                    }
                }
            }, tCHouseXSinkOptions.getSinkBatchIntervalMs(), tCHouseXSinkOptions.getSinkBatchIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }
        gson = new Gson();
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.systemDefault());
    }

    private List<Integer> getPrimaryFieldIndexes() {
        List<Integer> indexes = new ArrayList<>();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            map.put(fieldNames[i], i);
        }
        for (String fieldName : primaryFields) {
            indexes.add(map.get(fieldName));
        }
        return indexes;
    }

    private Table checkAndGetTable() {
        table = tCHouseXCatalog.getTable(TableIdentifier.parse(tCHouseXOptions.getTableIdentifier()));
        int flinkTableFieldNum = fieldNames.length;
        int tCHouseXTableFieldNum = table.getFieldNum();
        LOG.info("flink table define field:{}\n", Arrays.asList(fieldNames));
        LOG.info("tCHouseX table {} define field:{}", table.getTableIdentifier(), table.getAllFields());
        if (flinkTableFieldNum != tCHouseXTableFieldNum) {
            throw new RuntimeException("flink table define field num not equal tCHouseX table define table field num");
        }
        List<Field> all = table.getAllFields();
        for (int i = 0; i < fieldNames.length; i++) {
            if (!fieldNames[i].equalsIgnoreCase(all.get(i).getName())) {
                LOG.warn("different field, flink table field:{} <-> tCHouseX table field:{}", fieldNames[i],
                        all.get(i).getName());
                throw new RuntimeException("flink table define field name is different with tCHouseX table define "
                        + "field");
            }
        }
        table.setPrimaryKeyFields(table.genPrimaryKeyFieldsByFieldName(primaryFields));
        return table;
    }

    private boolean checkFlushException(TableIdentifier tableIdentifier) {
        Exception ex = flushExceptionMap.get(tableIdentifier);
        if (ex == null || SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE == schemaUpdatePolicy) {
            return false;
        }
        if (!multipleSink || SchemaUpdateExceptionPolicy.THROW_WITH_STOP == schemaUpdatePolicy) {
            throw new RuntimeException("Writing records to tchouse-x failed, tableIdentifier=" +
                    tableIdentifier.getFullName(), ex);
        }
        return true;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to tchouse-x failed.", flushException);
        }
    }

    /**
     * Adds a record to the output.
     *
     * <p>When this method is called, the output format it guaranteed to be opened.
     *
     * @param record The records to add to the output.
     * @throws IOException Thrown, if the records could not be added to to an I/O problem.
     */
    @Override
    public void writeRecord(T record) throws IOException {
        checkFlushException();
        while (flushing) {
            checkFlushException();
            LOG.warn("flushing...wait");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("", e);
            }
        }
        if (multipleSink) {
            writeMultipleTable(record);
        } else {
            writeSingleTable(record);
        }
        boolean flush = (dataBytes >= tCHouseXSinkOptions.getSinkBatchBytes())
                || dataRecords >= tCHouseXSinkOptions.getSinkBatchSize();
        if (!closed && flush && !flushing) {
            flush();
        }
    }

    private void handleMultipleDirtyData(List<RowData> dirtyDataList, TableIdentifier tableIdentifier,
            Exception e, RowType rowType) {
        for (RowData rowData : dirtyDataList) {
            DirtyData.Builder<Object> builder = DirtyData.builder();
            try {
                String dirtyLabel = DirtySinkHelper.regexReplace(dirtySinkHelper.getDirtyOptions().getLabels(),
                        DirtyType.BATCH_LOAD_ERROR, null,
                        tableIdentifier.getDatabase(), tableIdentifier.getTable(), null);
                String dirtyLogTag =
                        DirtySinkHelper.regexReplace(dirtySinkHelper.getDirtyOptions().getLogTag(),
                                DirtyType.BATCH_LOAD_ERROR, null,
                                tableIdentifier.getDatabase(), tableIdentifier.getTable(), null);
                String dirtyIdentifier =
                        DirtySinkHelper.regexReplace(dirtySinkHelper.getDirtyOptions().getIdentifier(),
                                DirtyType.BATCH_LOAD_ERROR, null,
                                tableIdentifier.getDatabase(), tableIdentifier.getTable(), null);
                builder.setData(rowData)
                        .setLabels(dirtyLabel)
                        .setLogTag(dirtyLogTag)
                        .setIdentifier(dirtyIdentifier)
                        .setRowType(rowType)
                        .setDirtyMessage(e.getMessage());
                if (dirtySinkHelper.getDirtySink() != null) {
                    dirtySinkHelper.getDirtySink().invoke(builder.build());
                }
                if (metricData != null && dirtySinkHelper.getDirtyOptions().ignoreDirty()) {
                    metricData.outputDirtyMetricsWithEstimate(tableIdentifier.getDatabase(),
                            tableIdentifier.getTable(), 1, CalculateObjectSizeUtils.getDataSize(rowData));
                }
            } catch (Exception ex) {
                if (!dirtySinkHelper.getDirtyOptions().ignoreSideOutputErrors()) {
                    throw new RuntimeException(ex);
                }
                LOG.warn("Dirty sink failed", ex);
            }
        }
    }

    private void handleDirtyData(List<RowData> dirtyData, TableIdentifier tableIdentifier, DirtyType dirtyType,
            Exception e, RowType rowType) {
        DirtyOptions dirtyOptions = dirtySinkHelper.getDirtyOptions();
        if (multipleSink) {
            if (dirtyType == DirtyType.DESERIALIZE_ERROR) {
                LOG.error("database and table can't be identified, will use default ${database}${table}");
            } else {
                try {
                    handleMultipleDirtyData(dirtyData, tableIdentifier, e, rowType);
                } catch (Exception ex) {
                    if (!dirtyOptions.ignoreSideOutputErrors()) {
                        throw new RuntimeException(ex);
                    }
                }
                return;
            }
        }
        if (dirtyOptions.ignoreDirty()) {
            dirtySinkHelper.invoke(dirtyData, dirtyType, dirtyOptions.getLabels(), dirtyOptions.getLogTag(),
                    dirtyOptions.getIdentifier(), e);
            metricData.invokeDirty(1, CalculateObjectSizeUtils.getDataSize(dirtyData));
        }
    }

    private void writeMultipleTable(T record) {
        RowData rowData = (RowData) record;
        JsonNode rootNode;
        try {
            rootNode = jsonDynamicSchemaFormat.deserialize(rowData.getBinary(0));
        } catch (Exception e) {
            LOG.error(String.format("deserialize error, raw data: %s", new String(rowData.getBinary(0))), e);
            exceptionProcess(Collections.singletonList(rowData), new TableIdentifier("unknown", "unknown"),
                    DirtyType.DESERIALIZE_ERROR, e, null);
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("raw record: {}", gson.toJson(rootNode));
        }
        TableIdentifier tableId = null;
        try {
            String database = jsonDynamicSchemaFormat.parse(rootNode, databasePattern);
            String table = jsonDynamicSchemaFormat.parse(rootNode, tablePattern);
            tableId = TableIdentifier.of(database, table);
        } catch (Exception e) {
            LOG.error(String.format("Table identifier parse error, raw data: %s", gson.toJson(rootNode)), e);
            exceptionProcess(Collections.singletonList(rowData), new TableIdentifier("unknown", "unknown"),
                    DirtyType.DESERIALIZE_ERROR, e, null);
        }
        if (tableId != null && blackList.contains(tableId.getFullName())) {
            LOG.debug("ignore record as in black list: {}", blackList);
            return;
        }
        boolean isDDL = jsonDynamicSchemaFormat.extractDDLFlag(rootNode);
        if (isDDL) {
            execDDL(rowData.getBinary(0), rootNode, tableId);
        } else {
            execDML(rowData, rootNode, tableId);
        }
    }

    private void execDML(RowData rowData, JsonNode rootNode, TableIdentifier tableIdentifier) {
        if (checkFlushException(tableIdentifier)) {
            return;
        }
        try {
            List<String> pkListStr = jsonDynamicSchemaFormat.extractPrimaryKeyNames(rootNode);
            RowType rowType = jsonDynamicSchemaFormat.extractSchema(rootNode, pkListStr);
            if (LOG.isDebugEnabled()) {
                LOG.debug("row type: {}", gson.toJson(rowType));
            }
            Table existTable = tableIdMapTableInfo.get(tableIdentifier);
            if (null == existTable) {
                if (tCHouseXCatalog.tableExists(tableIdentifier)) {
                    existTable = tCHouseXCatalog.getTable(tableIdentifier);
                    tableIdMapTableInfo.put(tableIdentifier, existTable);
                }
            }
            Table tableFromSource = FlinkSchemaUtil.getTableByParseRowType(rowType, tableIdentifier, pkListStr);
            if (autoCreateTable && existTable == null) {
                tCHouseXCatalog.createTable(tableFromSource, true);
                existTable = tableFromSource;
                tableIdMapTableInfo.put(tableIdentifier, existTable);
            }
            if (FlinkSchemaUtil.tableIsSame(tableFromSource, existTable)) {
                List<RowData> rowDataList = jsonDynamicSchemaFormat.extractRowData(rootNode, rowType);
                processRowDataList(rowDataList, existTable, pkListStr, rowType);
            } else {
                if (handleSchemaChangeByData(tableFromSource, existTable)) {
                    List<RowData> rowDataList = jsonDynamicSchemaFormat.extractRowData(rootNode, rowType);
                    processRowDataList(rowDataList, existTable, pkListStr, rowType);
                    return;
                }
                LOG.warn("Table {} schema is different!", tableIdentifier);
                List<RowData> rowDataList = jsonDynamicSchemaFormat.extractRowData(rootNode, rowType);
                TCHouseXData tCHouseXData = batchMap.get(tableIdentifier);
                if (tCHouseXData == null) {
                    tCHouseXData = new TCHouseXData();
                    batchMap.put(tableIdentifier, tCHouseXData);
                }
                if (dirtySinkHelper.getDirtyOptions().ignoreDirty() && dirtySinkHelper.getDirtySink() != null) {
                    tCHouseXData.getRowDataList().add(rowData);
                    tCHouseXData.setRowType(rowType);
                }
                exceptionProcess(rowDataList, tableIdentifier, DirtyType.UNDEFINED, new RuntimeException("schema is "
                        + "different"), rowType);
            }

        } catch (Exception e) {
            LOG.warn("parseRecord exception", e);
            exceptionProcess(Collections.singletonList(rowData), tableIdentifier, DirtyType.DESERIALIZE_ERROR, e, null);
        }

    }

    /**
     * handle table schema change by data judge
     * now support add column
     *
     * @param tableFromSource
     * @param existTable
     * @return update existTable success return true, other return false
     */
    private boolean handleSchemaChangeByData(Table tableFromSource, Table existTable) {
        SchemaChangePolicy schemaChangePolicy = policyMap.get(SchemaChangeType.ADD_COLUMN);
        if (SchemaChangePolicy.ENABLE == schemaChangePolicy
                && tableFromSource.getAllFields().size() > existTable.getAllFields().size()) {
            Map<String, Field> tableFromSourceMap =
                    tableFromSource.getAllFields().stream()
                            .collect(Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), Function.identity()));
            for (int i = 0; i < existTable.getAllFields().size(); i++) {
                Field fieldForExistTable = existTable.getAllFields().get(i);
                Field fieldFromSource = tableFromSourceMap.get(fieldForExistTable.getName());
                if (fieldFromSource != null && fieldFromSource.getType().contains(TCHouseXDataType.TINYINT.getFormat())
                        && fieldForExistTable.getType().contains(TCHouseXDataType.BOOLEAN.getFormat())) {
                    LOG.info("tinyint(1) and boolean process as equal");
                    tableFromSourceMap.remove(fieldForExistTable.getName());
                    continue;
                }
                if (!fieldForExistTable.equals(fieldFromSource)) {
                    LOG.warn("tableFromSource field:{} is different with existTable field:{}", fieldFromSource,
                            fieldForExistTable);
                    return false;
                }
                tableFromSourceMap.remove(fieldForExistTable.getName());
            }
            AddColumns(existTable, tableFromSourceMap.values());
            existTable.setPrimaryKeyFields(tableFromSource.getPrimaryKeyFields());
        } else {
            if (schemaChangePolicy == null) {
                LOG.warn("Unsupported schemaChangePolicy");
                return false;
            }
            switch (schemaChangePolicy) {
                case LOG:
                    LOG.warn("Unsupported schemaChangePolicy");
                    break;
                case ERROR:
                    throw new SchemaChangeHandleException("Unsupported schemaChangePolicy");
                default:
            }
        }
        return false;
    }

    /**
     * add column
     *
     * @param existTable
     * @param fields
     */
    private void AddColumns(Table existTable, Collection<Field> fields) {
        for (Field field : fields) {
            existTable.getNormalFields().add(field);
        }
        tCHouseXCatalog.applySchemaChanges(existTable.getTableIdentifier(), fields, SchemaChangeType.ADD_COLUMN);
        Table table = tCHouseXCatalog.getTable(existTable.getTableIdentifier());
        tableIdMapTableInfo.put(existTable.getTableIdentifier(), table);
    }

    private void exceptionProcess(List<RowData> rowDataList, TableIdentifier tableIdentifier, DirtyType dirtyType,
            Exception e, RowType rowType) {
        if (SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE == schemaUpdatePolicy) {
            handleDirtyData(rowDataList, tableIdentifier, dirtyType, e, rowType);
        } else if (SchemaUpdateExceptionPolicy.STOP_PARTIAL == schemaUpdatePolicy) {
            blackList.add(tableIdentifier.getFullName());
            LOG.warn("blackList:{}", blackList);
        } else if (SchemaUpdateExceptionPolicy.THROW_WITH_STOP == schemaUpdatePolicy) {
            throw new RuntimeException(
                    String.format("writer failed for table:%s", tableIdentifier.getFullName()), e);
        } else {
            throw new RuntimeException(
                    String.format("SchemaUpdatePolicy %s does not support exception process!",
                            schemaUpdatePolicy));
        }
    }

    /**
     * process data for multi sink
     *
     * @param rowDataList
     * @param table
     */
    private void processRowDataList(List<RowData> rowDataList, Table table, List<String> pkListStr, RowType rowType) {
        List<Integer> primaryFieldIndexes = new ArrayList<>();
        Map<String, Integer> fieldNameMapSourcePosition = FlinkSchemaUtil.getFieldNameMapSourcePosition(rowType);
        for (String pkField : pkListStr) {
            primaryFieldIndexes.add(fieldNameMapSourcePosition.get(pkField.toLowerCase(Locale.ROOT)));
        }
        RowData.FieldGetter[] fieldGetters = FlinkSchemaUtil.createFieldGetters(rowType.getChildren());
        for (RowData rowData : rowDataList) {
            addRowData(rowData, table, primaryFieldIndexes, fieldGetters, rowType, fieldNameMapSourcePosition);
        }
    }

    /**
     * trigger flush before ddl for avoiding schema is different
     *
     * @param binary
     * @param rootNode
     */
    private void execDDL(byte[] binary, JsonNode rootNode, TableIdentifier tableId) {
        flush();
        schemaChangeHelper.process(binary, rootNode);
        if (schemaChangeHelper.ddlExecSuccess().get()) {
            CDWDataLoader cdwDataLoader = databaseMapLoader.get(tableId.getDatabase());
            if (cdwDataLoader != null) {
                cdwDataLoader.refreshTable(tableId.getTable().toLowerCase(Locale.ROOT));
            }
            metricData.outputMetrics(tableId.getDatabase(), tableId.getTable(), 1, 1);
            schemaChangeHelper.ddlExecSuccess().set(false);
        }
    }

    private void writeSingleTable(T record) {
        RowData rowData = (RowData) record;
        if (LOG.isDebugEnabled()) {
            LOG.debug("raw record: {}", gson.toJson(rowData));
        }
        TableIdentifier tableIdentifier = TableIdentifier.parse(tCHouseXOptions.getTableIdentifier());
        addRowData(rowData, tableIdMapTableInfo.get(tableIdentifier), this.primaryFieldIndexes, this.fieldGetters,
                RowType.of(logicalTypes), null);
    }

    private void addRowData(RowData rowData, Table tableInfo, List<Integer> primaryFieldIndexes,
            RowData.FieldGetter[] fieldGetters, RowType rowType, Map<String, Integer> fieldNameMapSourcePosition) {
        TableIdentifier tableIdentifier = tableInfo.getTableIdentifier();
        TCHouseXData tCHouseXData = batchMap.get(tableIdentifier);
        if (tCHouseXData == null) {
            tCHouseXData = new TCHouseXData();
            batchMap.put(tableIdentifier, tCHouseXData);
        }
        if (dirtySinkHelper.getDirtyOptions().ignoreDirty() && dirtySinkHelper.getDirtySink() != null) {
            tCHouseXData.getRowDataList().add(rowData);
            tCHouseXData.setRowType(rowType);
        }
        StringBuilder primaryKeyDataStr = new StringBuilder();
        for (Integer index : primaryFieldIndexes) {
            Object field = fieldGetters[index].getFieldOrNull(rowData);
            String data = field != null ? field.toString() : "";
            primaryKeyDataStr.append(data);
        }
        List<String> row = new ArrayList<>(fieldGetters.length);
        Map<Integer, TCHouseXDataType> fieldIndexMapType = tableInfo.getFieldIndexMapTCHouseXType();
        Map<String, List<String>> insertData = tCHouseXData.getInsertData();
        List<Field> allFields = tableInfo.getAllFields();
        for (int i = 0; i < allFields.size(); i++) {
            int index;
            if (fieldNameMapSourcePosition == null) {
                index = i;
            } else {
                index = fieldNameMapSourcePosition.get(allFields.get(i).getName());
            }
            Object field = fieldGetters[index].getFieldOrNull(rowData);
            if (field instanceof TimestampData && multipleSink) {
                TimestampData timeField = (TimestampData) field;
                field = formatter.format(new Timestamp(timeField.getMillisecond()).toInstant());
            }
            String data = field != null ? field.toString() : null;
            TCHouseXDataType tcHouseXDataType = fieldIndexMapType.get(i);
            if (tcHouseXDataType == TCHouseXDataType.BOOLEAN) {
                if ("1".equals(data)) {
                    data = "true";
                } else {
                    data = "false";
                }
            }
            if (primaryFieldIndexes.isEmpty()) {
                primaryKeyDataStr.append(data);
            }
            row.add(data);
        }
        if (rowData.getRowKind() == RowKind.INSERT) {
            insertData.put(primaryKeyDataStr.toString(), row);
            dataBytes += CalculateObjectSizeUtils.getDataSize(rowData);
        } else if (rowData.getRowKind() == RowKind.UPDATE_AFTER) {
            List<List<String>> updateData = tCHouseXData.getUpdateData();
            if (insertData.containsKey(primaryKeyDataStr.toString())) {
                insertData.put(primaryKeyDataStr.toString(), row);
            } else {
                updateData.add(row);
            }
            dataBytes += CalculateObjectSizeUtils.getDataSize(rowData);
        } else if (rowData.getRowKind() == RowKind.DELETE) {
            List<List<String>> deleteData = tCHouseXData.getDeleteData();
            if (insertData.containsKey(primaryKeyDataStr.toString())) {
                insertData.remove(primaryKeyDataStr.toString());
            } else {
                deleteData.add(row);
            }
            dataBytes += CalculateObjectSizeUtils.getDataSize(rowData);
        } else if (rowData.getRowKind() == RowKind.UPDATE_BEFORE) {
            tCHouseXData.getUpdateBeforeRecordNum().addAndGet(1);
        } else {
            LOG.warn("Do nothing for this rowKind");
        }
        dataRecords++;
    }

    /**
     * Method that marks the end of the life-cycle of parallel output instance. Should be used to
     * close channels and streams and release resources. After this method returns without an error,
     * the output is assumed to be correct.
     *
     * <p>When this method is called, the output format it guaranteed to be opened.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }
            try {
                flush();
            } catch (Exception e) {
                LOG.warn("Writing records to tchouse-x failed.", e);
                throw new RuntimeException("Writing records to tchosue-x failed.", e);
            } finally {
                jdbcClient.close();
            }
        }
        checkFlushException();
    }

    /**
     * Scheduled task refreshes and refreshes when the set number and size are reached, and refreshes at checkpoint.
     */
    public synchronized void flush() {
        checkFlushException();
        StopWatch stopWatch = StopWatch.createStarted();
        flushing = true;
        if (!hasRecords()) {
            flushing = false;
            return;
        }
        if (!blackList.isEmpty()) {
            blackList.forEach(v -> {
                String fullTableName = v.replace("`", "");
                batchMap.remove(TableIdentifier.parse(fullTableName));
            });
        }
        for (Entry<TableIdentifier, TCHouseXData> kvs : batchMap.entrySet()) {
            flushSingleTable(kvs.getKey(), kvs.getValue());
        }
        stopWatch.stop();
        LOG.info("flush a batch data dataRecords:{}, cost time:{}ms", dataRecords, stopWatch.getTime());
        resetStateAfterFlush();
        flushing = false;
    }

    private void resetStateAfterFlush() {
        dataRecords = 0L;
        dataBytes = 0L;
    }

    private void flushSingleTable(TableIdentifier tableIdentifier, TCHouseXData tCHouseXData) {
        if (checkFlushException(tableIdentifier)) {
            return;
        }
        Exception exceptionInfo = null;
        boolean failed = false;
        // jdbc for update and delete
        try (Connection connection = jdbcClient.getClient()) {
            TaskRunUtil.run(() -> {
                Statement statement = connection.createStatement();
                if (!tCHouseXData.getUpdateData().isEmpty()) {
                    for (List<String> updateRow : tCHouseXData.getUpdateData()) {
                        String updateStatement = TCHouseXJdbcDialect.getUpdateStatement(updateRow,
                                tableIdMapTableInfo.get(tableIdentifier));
                        LOG.debug("table:{} updateStatement:{}", tableIdentifier.getFullName(), updateStatement);
                        statement.addBatch(updateStatement);
                    }
                }
                if (!tCHouseXData.getDeleteData().isEmpty()) {
                    for (List<String> deleteRow : tCHouseXData.getDeleteData()) {
                        String deleteStatement = TCHouseXJdbcDialect.getDeleteStatement(deleteRow,
                                tableIdMapTableInfo.get(tableIdentifier));
                        LOG.debug("table:{} deleteStatement:{}", tableIdentifier.getFullName(), deleteStatement);
                        statement.addBatch(deleteStatement);
                    }
                }
                statement.executeBatch();
            }, tCHouseXSinkOptions.getSinkMaxRetries());
        } catch (Exception e) {
            LOG.error(String.format("jdbc flush fail for table %s", tableIdentifier.getFullName()), e);
            failed = true;
            exceptionInfo = e;
            flushExceptionMap.put(tableIdentifier, e);
        }

        // sdk for insert
        if (!failed && !tCHouseXData.getInsertData().isEmpty()) {
            try {
                CDWDataLoader cdwDataLoader = databaseMapLoader.get(tableIdentifier.getDatabase());
                if (cdwDataLoader == null) {
                    cdwDataLoader = new CDWDataLoader();
                    cdwDataLoader.setCdwConfig(tCHouseXOptions.getHostName(), tCHouseXOptions.getPort(),
                            tCHouseXOptions.getUsername(), tCHouseXOptions.getPassword(),
                            tableIdentifier.getDatabase());
                    databaseMapLoader.put(tableIdentifier.getDatabase(), cdwDataLoader);
                }
                if (!cdwDataLoader.isInitialized()) {
                    cdwDataLoader.setCdwConfig(tCHouseXOptions.getHostName(), tCHouseXOptions.getPort(),
                            tCHouseXOptions.getUsername(), tCHouseXOptions.getPassword(),
                            tableIdentifier.getDatabase());
                }
                CDWDataLoader finalCdwDataLoader = cdwDataLoader;
                TaskRunUtil.run(() -> {
                    List<List<String>> batchData = new ArrayList<>(tCHouseXData.getInsertData().values());
                    LOG.debug("table:{} flush batchData table:{}", tableIdentifier.getFullName(), batchData);
                    Table table = tableIdMapTableInfo.get(tableIdentifier);
                    List<String> columnNames;
                    if (table == null) {
                        columnNames = new ArrayList<>();
                    } else {
                        columnNames = table.getAllFieldName();
                    }
                    finalCdwDataLoader.writeData(tableIdentifier.getTable(), batchData, columnNames);
                    finalCdwDataLoader.commit(tableIdentifier.getTable());
                }, tCHouseXSinkOptions.getSinkMaxRetries());
            } catch (Exception e) {
                LOG.error(String.format("sdk flush fail for table %s", tableIdentifier.getFullName()), e);
                failed = true;
                exceptionInfo = e;
                flushExceptionMap.put(tableIdentifier, e);
            }
        }
        if (failed) {
            exceptionProcess(tCHouseXData.getRowDataList(), tableIdentifier, DirtyType.BATCH_LOAD_ERROR, exceptionInfo,
                    tCHouseXData.getRowType());
        } else {
            statisticsMetricData(tCHouseXData, tableIdentifier);
        }
        clear(tCHouseXData);
    }

    private void statisticsMetricData(TCHouseXData tCHouseXData, TableIdentifier tableIdentifier) {
        long dataRecords = 0L;
        long dataBytes = 0L;
        dataRecords += tCHouseXData.getUpdateData().size();
        dataRecords += tCHouseXData.getDeleteData().size();
        dataRecords += tCHouseXData.getInsertData().size();
        dataRecords += tCHouseXData.getUpdateBeforeRecordNum().get();
        dataBytes += CalculateObjectSizeUtils.getDataSize(tCHouseXData.getUpdateData());
        dataBytes += CalculateObjectSizeUtils.getDataSize(tCHouseXData.getDeleteData());
        dataBytes += CalculateObjectSizeUtils.getDataSize(tCHouseXData.getInsertData());
        if (multipleSink) {
            metricData.outputMetrics(tableIdentifier.getDatabase(), tableIdentifier.getTable(), dataRecords, dataBytes);
        } else {
            metricData.invoke(dataRecords, dataBytes);
        }
    }

    private void clear(TCHouseXData tCHouseXData) {
        tCHouseXData.getInsertData().clear();
        tCHouseXData.getDeleteData().clear();
        tCHouseXData.getUpdateData().clear();
        tCHouseXData.getRowDataList().clear();
        tCHouseXData.getUpdateBeforeRecordNum().set(0);
    }

    private boolean hasRecords() {
        if (batchMap.isEmpty()) {
            return false;
        }
        boolean hasRecords = false;
        for (TCHouseXData tCHouseXData : batchMap.values()) {
            if (!tCHouseXData.getInsertData().isEmpty() || !tCHouseXData.getUpdateData().isEmpty()
                    || !tCHouseXData.getDeleteData().isEmpty()) {
                hasRecords = true;
                break;
            }
        }
        return hasRecords;
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (metricData != null && metricStateListState != null) {
            MetricStateUtils.snapshotMetricStateForSinkMetricData(metricStateListState, metricData,
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }

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

    /**
     * Builder for {@link TCHouseXDynamicOutputFormat}.
     */
    public static class Builder {

        private TCHouseXOptions tCHouseXOptions;
        private TCHouseXSinkOptions tCHouseXSinkOptions;
        private String dynamicSchemaFormat;
        private String databasePattern;
        private String tablePattern;
        private SchemaUpdateExceptionPolicy schemaUpdatePolicy;
        private boolean multipleSink;
        private String inlongMetric;
        private String auditHostAndPorts;
        private DataType[] fieldDataTypes;
        private String[] fieldNames;
        private List<String> primaryFields;
        private DirtyOptions dirtyOptions;
        private DirtySink<Object> dirtySink;
        private boolean enableSchemaChange;
        private String schemaChangePolicies;

        public Builder() {
        }

        public Builder setTCHouseXOptions(TCHouseXOptions tCHouseXOptions) {
            this.tCHouseXOptions = tCHouseXOptions;
            return this;
        }

        public Builder setTCHouseXSinkOptions(TCHouseXSinkOptions tCHouseXSinkOptions) {
            this.tCHouseXSinkOptions = tCHouseXSinkOptions;
            return this;
        }

        public Builder setPrimaryFields(List<String> primaryFields) {
            this.primaryFields = primaryFields;
            return this;
        }

        public Builder setFieldDataTypes(DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setDynamicSchemaFormat(String dynamicSchemaFormat) {
            this.dynamicSchemaFormat = dynamicSchemaFormat;
            return this;
        }

        public Builder setDatabasePattern(String databasePattern) {
            this.databasePattern = databasePattern;
            return this;
        }

        public Builder setTablePattern(String tablePattern) {
            this.tablePattern = tablePattern;
            return this;
        }

        public Builder setMultipleSink(boolean multipleSink) {
            this.multipleSink = multipleSink;
            return this;
        }

        public Builder setInlongMetric(String inlongMetric) {
            this.inlongMetric = inlongMetric;
            return this;
        }

        public Builder setAuditHostAndPorts(String auditHostAndPorts) {
            this.auditHostAndPorts = auditHostAndPorts;
            return this;
        }

        public Builder setDirtyOptions(DirtyOptions dirtyOptions) {
            this.dirtyOptions = dirtyOptions;
            return this;
        }

        public Builder setDirtySink(DirtySink<Object> dirtySink) {
            this.dirtySink = dirtySink;
            return this;
        }

        public Builder setSchemaUpdatePolicy(
                SchemaUpdateExceptionPolicy schemaUpdatePolicy) {
            this.schemaUpdatePolicy = schemaUpdatePolicy;
            return this;
        }

        public Builder setEnableSchemaChange(boolean enableSchemaChange) {
            this.enableSchemaChange = enableSchemaChange;
            return this;
        }

        public Builder setSchemaChangePolicies(String schemaChangePolicies) {
            this.schemaChangePolicies = schemaChangePolicies;
            return this;
        }

        public TCHouseXDynamicOutputFormat<RowData> build() {
            LogicalType[] logicalTypes = null;
            if (!multipleSink) {
                logicalTypes = Arrays.stream(fieldDataTypes)
                        .map(DataType::getLogicalType).toArray(LogicalType[]::new);
            }
            return new TCHouseXDynamicOutputFormat<>(
                    tCHouseXOptions,
                    tCHouseXSinkOptions,
                    inlongMetric,
                    auditHostAndPorts,
                    multipleSink,
                    databasePattern,
                    tablePattern,
                    dynamicSchemaFormat,
                    schemaUpdatePolicy,
                    fieldNames,
                    logicalTypes,
                    primaryFields,
                    enableSchemaChange,
                    schemaChangePolicies,
                    dirtyOptions,
                    dirtySink);
        }
    }
}
