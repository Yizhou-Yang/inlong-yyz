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

package org.apache.inlong.sort.cdc.oracle.source.reader;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.document.Array;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.base.enums.ReadPhase;
import org.apache.inlong.sort.cdc.oracle.debezium.DebeziumDeserializationSchema;
import org.apache.inlong.sort.cdc.oracle.source.meta.split.SourceSplitState;
import org.apache.inlong.sort.cdc.oracle.source.metrics.SourceReaderMetrics;
import org.apache.inlong.sort.cdc.oracle.source.utils.RecordUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.getFetchTimestamp;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.getMessageTimestamp;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isHeartbeatEvent;
import static org.apache.inlong.sort.cdc.oracle.source.utils.RecordUtils.isSchemaChangeEvent;

/**
 * The {@link RecordEmitter} implementation for {@link IncrementalSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the stream reader to
 * emit records rather than emit the records directly.
 * Copy from com.ververica:flink-cdc-base:2.4.1.
 */
public class IncrementalSourceRecordEmitter<T>
        implements
            RecordEmitter<SourceRecords, T, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    protected final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    protected final SourceReaderMetrics sourceReaderMetrics;
    protected final boolean includeSchemaChanges;
    protected final OutputCollector<T> outputCollector;
    protected final OffsetFactory offsetFactory;

    public IncrementalSourceRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            OffsetFactory offsetFactory) {
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.sourceReaderMetrics = sourceReaderMetrics;
        this.includeSchemaChanges = includeSchemaChanges;
        this.outputCollector = new OutputCollector<>(sourceReaderMetrics);
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
        while (elementIterator.hasNext()) {
            processElement(elementIterator.next(), output, splitState);
        }
    }

    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            LOG.trace("Process WatermarkEvent: {}; splitState = {}", element, splitState);
            Offset watermark = getWatermark(element);
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                LOG.trace("Set HighWatermark {} for {}", watermark, splitState);
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isStreamSplitState()) {
            LOG.trace("Process SchemaChangeEvent: {}; splitState = {}", element, splitState);
            HistoryRecord historyRecord = getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asStreamSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                final TableChange tableSchema = splitState.asStreamSplitState()
                        .getTableSchemas().getOrDefault(RecordUtils.getTableId(element), null);
                emitElement(element, splitState, output, tableSchema);
            }
        } else if (isDataChangeRecord(element)) {
            LOG.trace("Process DataChangeRecord: {}; splitState = {}", element, splitState);
            updateStartingOffsetForSplit(splitState, element);
            reportMetrics(element);
            final Map<TableId, TableChange> tableSchemas =
                    splitState.getSourceSplitBase().getTableSchemas();
            final TableChange tableSchema =
                    tableSchemas.getOrDefault(RecordUtils.getTableId(element), null);
            emitElement(element, splitState, output, tableSchema);
        } else if (isHeartbeatEvent(element)) {
            LOG.trace("Process Heartbeat: {}; splitState = {}", element, splitState);
            updateStartingOffsetForSplit(splitState, element);
        } else {
            // unknown element
            LOG.info(
                    "Meet unknown element {} for splitState = {}, just skip.", element, splitState);
        }
    }

    protected void updateStartingOffsetForSplit(SourceSplitState splitState, SourceRecord element) {
        if (splitState.isStreamSplitState()) {
            // record the time metric to enter the incremental phase
            if (sourceReaderMetrics != null) {
                sourceReaderMetrics.outputReadPhaseMetrics(ReadPhase.INCREASE_PHASE);
            }
            Offset position = getOffsetPosition(element);
            splitState.asStreamSplitState().setStartingOffset(position);
        }
    }

    private Offset getWatermark(SourceRecord watermarkEvent) {
        return getOffsetPosition(watermarkEvent.sourceOffset());
    }

    public Offset getOffsetPosition(SourceRecord dataRecord) {
        return getOffsetPosition(dataRecord.sourceOffset());
    }

    public Offset getOffsetPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return offsetFactory.newOffset(offsetStrMap);
    }

    protected void emitElement(SourceRecord element, SourceSplitState splitState, SourceOutput<T> output)
            throws Exception {
        emitElement(element, splitState, output, null);
    }

    protected void emitElement(SourceRecord element, SourceSplitState splitState,
            SourceOutput<T> output, TableChanges.TableChange tableSchema) throws Exception {
        outputCollector.output = output;
        outputCollector.element = element;
        outputCollector.splitState = splitState;
        debeziumDeserializationSchema.deserialize(element, outputCollector, tableSchema);
    }

    protected void reportMetrics(SourceRecord element) {
        long now = System.currentTimeMillis();
        // record the latest process time
        sourceReaderMetrics.recordProcessTime(now);
        Long messageTimestamp = getMessageTimestamp(element);

        if (messageTimestamp != null && messageTimestamp > 0L) {
            // report fetch delay
            Long fetchTimestamp = getFetchTimestamp(element);
            if (fetchTimestamp != null) {
                sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
            // report emit delay
            sourceReaderMetrics.recordEmitDelay(now - messageTimestamp);
        }
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceOutput<T> output;
        private SourceRecord element;
        private SourceSplitState splitState;
        private final SourceReaderMetrics sourceReaderMetrics;

        private OutputCollector(SourceReaderMetrics sourceReaderMetrics) {
            this.sourceReaderMetrics = sourceReaderMetrics;
        }

        @Override
        public void collect(T record) {
            Struct value = (Struct) element.value();
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            String dbName = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
            String schemaName = source.getString(AbstractSourceInfo.SCHEMA_NAME_KEY);
            String tableName = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);
            sourceReaderMetrics.outputMetrics(dbName, schemaName, tableName, splitState.isSnapshotSplitState(), value);
            output.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
