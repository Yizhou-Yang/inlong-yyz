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

import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.Array;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.inlong.sort.cdc.base.debezium.DebeziumDeserializationSchema;
import org.apache.inlong.sort.cdc.base.source.meta.offset.Offset;
import org.apache.inlong.sort.cdc.base.source.meta.offset.OffsetFactory;
import org.apache.inlong.sort.cdc.base.source.meta.split.SourceSplitState;
import org.apache.inlong.sort.cdc.base.source.metrics.SourceReaderMetrics;
import org.apache.inlong.sort.cdc.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.inlong.sort.cdc.base.util.RecordUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.inlong.sort.cdc.oracle.source.utils.OracleUtils.isSchemaChangeEvent;

/**
 * Inheriting from IncrementalSourceRecordEmitter, and override the processElement method to collect metric of Oracle.
 */
public class OracleRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(OracleRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    public OracleRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges, OffsetFactory offsetFactory) {
        super(debeziumDeserializationSchema, sourceReaderMetrics, includeSchemaChanges, offsetFactory);
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
            for (TableChange tableChange : changes) {
                splitState.asStreamSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                final TableChange tableSchema = splitState.asStreamSplitState()
                        .getTableSchemas().getOrDefault(RecordUtils.getTableId(element), null);
                emitElement(element, splitState, output, tableSchema);
            }
        } else if (isDataChangeRecord(element)) {
            if (splitState.isStreamSplitState()) {
                Offset position = getOffsetPosition(element);
                splitState.asStreamSplitState().setStartingOffset(position);
            }
            reportMetrics(element);
            final Map<TableId, TableChange> tableSchemas =
                    splitState.getSourceSplitBase().getTableSchemas();
            final TableChange tableSchema =
                    tableSchemas.getOrDefault(RecordUtils.getTableId(element), null);
            emitElement(element, splitState, output, tableSchema);
        } else {
            // unknown element
            LOG.info("Meet unknown element {}, just skip.", element);
        }
    }

}
