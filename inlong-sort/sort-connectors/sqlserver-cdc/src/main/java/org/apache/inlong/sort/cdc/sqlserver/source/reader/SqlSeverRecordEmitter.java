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

package org.apache.inlong.sort.cdc.sqlserver.source.reader;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.document.Array;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.base.enums.ReadPhase;
import org.apache.inlong.sort.cdc.sqlserver.base.source.metrics.SourceReaderMetrics;
import org.apache.inlong.sort.cdc.sqlserver.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.inlong.sort.cdc.sqlserver.debezium.DebeziumDeserializationSchema;
import org.apache.inlong.sort.cdc.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isHeartbeatEvent;
import static org.apache.inlong.sort.cdc.sqlserver.base.util.RecordUtils.isSchemaChangeEvent;

public class SqlSeverRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SqlSeverRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();
    private final boolean migrateAll;

    public SqlSeverRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            SourceConfig sourceConfig,
            OffsetFactory offsetFactory) {
        super(debeziumDeserializationSchema, sourceReaderMetrics, sourceConfig.isIncludeSchemaChanges(),
                offsetFactory);
        SqlServerSourceConfig sqlServerSourceConfig = (SqlServerSourceConfig) sourceConfig;
        this.migrateAll = sqlServerSourceConfig.isMigrateAll();
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            Offset watermark = super.getOffsetPosition(element.sourceOffset());
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isStreamSplitState()) {
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
            updateStreamSplitState(splitState, element);
            reportMetrics(element);
            final Map<TableId, TableChange> tableSchemas = splitState.toSourceSplit().getTableSchemas();
            final TableChange tableSchema =
                    tableSchemas.getOrDefault(getTableId(element), null);
            debeziumDeserializationSchema.deserialize(element, new Collector<T>() {

                @Override
                public void collect(T record) {
                    Struct value = (Struct) element.value();
                    if (migrateAll) {
                        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                        String dbName = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
                        String schemaName = source.getString(AbstractSourceInfo.SCHEMA_NAME_KEY);
                        String tableName = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);
                        sourceReaderMetrics
                                .outputMetrics(dbName, schemaName, tableName, splitState.isSnapshotSplitState(), value);
                    } else {
                        sourceReaderMetrics.outputMetrics(null, null, null, splitState.isSnapshotSplitState(), value);
                    }
                    output.collect(record);
                }

                @Override
                public void close() {

                }
            }, tableSchema);
        } else if (isHeartbeatEvent(element)) {
            LOG.info("Process Heartbeat: {}; splitState = {}", element, splitState);
            updateStreamSplitState(splitState, element);
        } else {
            // unknown element
            LOG.info("Meet unknown element {}, just skip.", element);
        }
    }

    private void updateStreamSplitState(SourceSplitState splitState, SourceRecord element) {
        if (splitState.isStreamSplitState()) {
            // record the time metric to enter the incremental phase
            if (sourceReaderMetrics != null) {
                sourceReaderMetrics.outputReadPhaseMetrics(ReadPhase.INCREASE_PHASE);
            }
            Offset position = getOffsetPosition(element);
            splitState.asStreamSplitState().setStartingOffset(position);
        }
    }

}
