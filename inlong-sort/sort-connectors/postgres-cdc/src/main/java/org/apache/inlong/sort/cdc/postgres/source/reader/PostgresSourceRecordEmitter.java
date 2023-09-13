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

package org.apache.inlong.sort.cdc.postgres.source.reader;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.document.Array;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.cdc.base.debezium.DebeziumDeserializationSchema;
import org.apache.inlong.sort.cdc.base.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.inlong.sort.cdc.base.source.meta.offset.Offset;
import org.apache.inlong.sort.cdc.base.source.meta.offset.OffsetFactory;
import org.apache.inlong.sort.cdc.base.source.meta.split.SourceSplitState;
import org.apache.inlong.sort.cdc.base.source.metrics.SourceReaderMetrics;
import org.apache.inlong.sort.cdc.base.source.reader.IncrementalSourceReader;
import org.apache.inlong.sort.cdc.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.inlong.sort.cdc.postgres.connection.PostgreSQLJdbcConnectionOptions;
import org.apache.inlong.sort.cdc.postgres.connection.PostgreSQLJdbcConnectionProvider;
import org.apache.inlong.sort.cdc.postgres.debezium.internal.DebeziumChangeFetcher.TableChangeHolder;
import org.apache.inlong.sort.cdc.postgres.debezium.internal.TableImpl;
import org.apache.inlong.sort.cdc.postgres.manager.PostgreSQLQueryVisitor;
import org.apache.inlong.sort.cdc.postgres.source.config.PostgresSourceConfig;
import org.apache.inlong.sort.cdc.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.inlong.sort.cdc.base.source.meta.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static org.apache.inlong.sort.cdc.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;

import static org.apache.inlong.sort.cdc.base.util.SourceRecordUtils.getHistoryRecord;
import static org.apache.inlong.sort.cdc.base.util.SourceRecordUtils.isDataChangeRecord;
import static org.apache.inlong.sort.cdc.base.util.SourceRecordUtils.isSchemaChangeEvent;
import static org.apache.inlong.sort.cdc.base.util.SourceRecordUtils.isHeartbeatEvent;
import static org.apache.inlong.sort.cdc.postgres.debezium.internal.DebeziumChangeFetcher.DDL_UPDATE_INTERVAL;
import static org.apache.inlong.sort.cdc.postgres.debezium.internal.DebeziumChangeFetcher.initTableChange;

/**
 * The {@link RecordEmitter} implementation for {@link IncrementalSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the stream reader to
 * emit records rather than emit the records directly.
 * Copy from com.ververica:flink-cdc-base:2.3.0.
 */
public class PostgresSourceRecordEmitter<T>
        extends
            IncrementalSourceRecordEmitter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();
    private PostgreSQLQueryVisitor postgreSQLQueryVisitor;
    private Map<String, TableChangeHolder> tableChangeMap = new ConcurrentHashMap<>();

    public PostgresSourceRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory,
            PostgresSourceConfigFactory postgresSourceConfigFactory) {
        super(debeziumDeserializationSchema, sourceReaderMetrics, includeSchemaChanges, offsetFactory);
        PostgresSourceConfig postgresSourceConfig = postgresSourceConfigFactory.create(0);
        String url = String.format("jdbc:postgresql://%s:%s/%s", postgresSourceConfig.getHostname(),
                postgresSourceConfig.getPort(), postgresSourceConfig.getDatabaseName());
        PostgreSQLJdbcConnectionOptions jdbcConnectionOptions = new PostgreSQLJdbcConnectionOptions(url,
                postgresSourceConfig.getUsername(), postgresSourceConfig.getPassword());
        postgreSQLQueryVisitor = new PostgreSQLQueryVisitor(
                new PostgreSQLJdbcConnectionProvider(jdbcConnectionOptions));
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            LOG.debug("PostgresSourceRecordEmitter Process WatermarkEvent: {}; splitState = {}", element, splitState);
            Offset watermark = super.getOffsetPosition(element);
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                LOG.info("PostgresSourceRecordEmitter Set HighWatermark {} for {}", watermark, splitState);
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isStreamSplitState()) {
            LOG.debug("PostgresSourceRecordEmitter Process SchemaChangeEvent: {}; splitState = {}", element,
                    splitState);
            HistoryRecord historyRecord = getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asStreamSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                emitElement(element, output);
            }
        } else if (isDataChangeRecord(element)) {
            LOG.debug("PostgresSourceRecordEmitter Process DataChangeRecord: {}; splitState = {}", element, splitState);
            updateStartingOffsetForSplit(splitState, element);
            reportMetrics(element);
            if (splitState.isSnapshotSplitState()) {
                toSnapshotRecord(element);
            }
            debeziumDeserializationSchema.deserialize(element, new Collector<T>() {

                @Override
                public void collect(T record) {
                    Struct value = (Struct) element.value();
                    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                    String dbName = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
                    String schemaName = source.getString(AbstractSourceInfo.SCHEMA_NAME_KEY);
                    String tableName = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);
                    sourceReaderMetrics
                            .outputMetrics(dbName, schemaName, tableName, splitState.isSnapshotSplitState(), value);
                    output.collect(record);
                }

                @Override
                public void close() {

                }
            }, getTableChange(element));
        } else if (isHeartbeatEvent(element)) {
            LOG.debug("PostgresSourceRecordEmitterProcess Heartbeat: {}; splitState = {}", element, splitState);
            updateStartingOffsetForSplit(splitState, element);
        } else {
            // unknown element
            LOG.info(
                    "Meet unknown element {} for splitState = {}, just skip.", element, splitState);
        }
    }

    public static void toSnapshotRecord(SourceRecord element) {
        Struct messageStruct = (Struct) element.value();
        Struct sourceStruct = messageStruct.getStruct(FieldName.SOURCE);
        SnapshotRecord.TRUE.toSource(sourceStruct);
    }

    private TableChange getTableChange(SourceRecord record) {
        Envelope.Operation op = Envelope.operationFor(record);
        Schema valueSchema;
        if (op == Envelope.Operation.DELETE) {
            valueSchema = record.valueSchema().field(Envelope.FieldName.BEFORE).schema();
        } else {
            valueSchema = record.valueSchema().field(FieldName.AFTER).schema();
        }
        List<Field> fields = valueSchema.fields();

        TableId tableId = org.apache.inlong.sort.cdc.base.util.RecordUtils.getTableId(record);
        String schema = tableId.schema();
        String table = tableId.table();
        String id = tableId.identifier();

        TableChange tableChange;
        TableChangeHolder holder;
        if (!tableChangeMap.containsKey(id)) {
            List<Map<String, Object>> columns = this.postgreSQLQueryVisitor.getTableColumnsMetaData(schema, table);
            LOG.info("columns: {}", columns);
            tableChange = initTableChange(tableId, TableChangeType.CREATE, columns);
            holder = new TableChangeHolder();
            holder.tableChange = tableChange;
            holder.timestamp = System.currentTimeMillis();
            tableChangeMap.put(id, holder);
        } else {
            holder = tableChangeMap.get(id);
            tableChange = holder.tableChange;
            // diff fieldMap and columns of tableChange, to see if new column has been added
            if (System.currentTimeMillis() - holder.timestamp > DDL_UPDATE_INTERVAL
                    && holder.tableChange.getTable() instanceof TableImpl) {
                TableImpl tableImpl = (TableImpl) holder.tableChange.getTable();
                List<Column> columnList = tableImpl.columns();
                // only support add or remove columns
                if (columnList.size() != fields.size()) {
                    List<Map<String, Object>> columns = this.postgreSQLQueryVisitor.getTableColumnsMetaData(schema,
                            table);
                    LOG.info("columns: {}", columns);
                    tableChange = initTableChange(tableId, TableChangeType.CREATE, columns);
                    // update tableChange
                    holder.tableChange = tableChange;
                }
                holder.timestamp = System.currentTimeMillis();
                tableChangeMap.put(id, holder);
            }
        }
        return tableChange;
    }
}
