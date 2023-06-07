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

package org.apache.inlong.sort.cdc.mysql.source.reader;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import java.util.Comparator;
import java.util.Map.Entry;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.inlong.sort.base.metric.sub.SourceTableMetricData;
import org.apache.inlong.sort.cdc.mysql.debezium.DebeziumUtils;
import org.apache.inlong.sort.cdc.mysql.source.config.MySqlSourceConfig;
import org.apache.inlong.sort.cdc.mysql.source.events.BinlogSplitMetaEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.BinlogSplitMetaRequestEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.LatestFinishedSplitsSizeEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.LatestFinishedSplitsSizeRequestEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.SuspendBinlogReaderAckEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.SuspendBinlogReaderEvent;
import org.apache.inlong.sort.cdc.mysql.source.events.WakeupReaderEvent;
import org.apache.inlong.sort.cdc.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.inlong.sort.cdc.mysql.source.offset.BinlogOffset;
import org.apache.inlong.sort.cdc.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.inlong.sort.cdc.mysql.source.split.MySqlBinlogSplit;
import org.apache.inlong.sort.cdc.mysql.source.split.MySqlBinlogSplitState;
import org.apache.inlong.sort.cdc.mysql.source.split.MySqlMetricSplit;
import org.apache.inlong.sort.cdc.mysql.source.split.MySqlMetricSplit.MySqlTableMetric;
import org.apache.inlong.sort.cdc.mysql.source.split.MySqlSnapshotSplit;
import org.apache.inlong.sort.cdc.mysql.source.split.MySqlSnapshotSplitState;
import org.apache.inlong.sort.cdc.mysql.source.split.MySqlSplit;
import org.apache.inlong.sort.cdc.mysql.source.split.MySqlSplitState;
import org.apache.inlong.sort.cdc.mysql.source.utils.TableDiscoveryUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.cdc.mysql.source.events.WakeupReaderEvent.WakeUpTarget.SNAPSHOT_READER;
import static org.apache.inlong.sort.cdc.mysql.source.split.MySqlBinlogSplit.toNormalBinlogSplit;
import static org.apache.inlong.sort.cdc.mysql.source.split.MySqlBinlogSplit.toSuspendedBinlogSplit;
import static org.apache.inlong.sort.cdc.mysql.source.utils.ChunkUtils.chunkId;
import static org.apache.inlong.sort.cdc.mysql.source.utils.ChunkUtils.getNextMetaGroupId;

/**
 * The source reader for MySQL source splits.
 */
public class MySqlSourceReader<T>
        extends
            SingleThreadMultiplexSourceReaderBase<SourceRecord, T, MySqlSplit, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);

    private final MySqlSourceConfig sourceConfig;
    private final Map<String, MySqlSnapshotSplit> finishedUnackedSplits;
    private final Map<String, MySqlBinlogSplit> uncompletedBinlogSplits;
    private final int subtaskId;
    private final MySqlSourceReaderContext mySqlSourceReaderContext;
    private MySqlBinlogSplit suspendedBinlogSplit;
    private MySqlSourceReaderMetrics sourceReaderMetrics;

    public MySqlSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
            Supplier<MySqlSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, MySqlSplitState> recordEmitter,
            Configuration config,
            MySqlSourceReaderContext context,
            MySqlSourceConfig sourceConfig,
            MySqlSourceReaderMetrics sourceReaderMetrics) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context.getSourceReaderContext());
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedBinlogSplits = new HashMap<>();
        this.subtaskId = context.getSourceReaderContext().getIndexOfSubtask();
        this.mySqlSourceReaderContext = context;
        this.suspendedBinlogSplit = null;
        this.sourceReaderMetrics = sourceReaderMetrics;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected MySqlSplitState initializedState(MySqlSplit split) {
        if (split.isSnapshotSplit()) {
            return new MySqlSnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new MySqlBinlogSplitState(split.asBinlogSplit());
        }
    }

    @Override
    public List<MySqlSplit> snapshotState(long checkpointId) {
        List<MySqlSplit> stateSplits = super.snapshotState(checkpointId);

        // unfinished splits
        List<MySqlSplit> unfinishedSplits =
                stateSplits.stream()
                        .filter(split -> !finishedUnackedSplits.containsKey(split.splitId()))
                        .collect(Collectors.toList());

        // add finished snapshot splits that didn't receive ack yet
        unfinishedSplits.addAll(finishedUnackedSplits.values());

        // add binlog splits who are uncompleted
        unfinishedSplits.addAll(uncompletedBinlogSplits.values());

        // add suspended BinlogSplit
        if (suspendedBinlogSplit != null) {
            unfinishedSplits.add(suspendedBinlogSplit);
        }
        SourceTableMetricData sourceMetricData = sourceReaderMetrics.getSourceMetricData();
        LOG.info("inlong-metric-states snapshot sourceMetricData:{}", sourceMetricData);
        if (sourceMetricData != null) {
            long countNumBytesIn = sourceMetricData.getNumBytesIn().getCount();
            long countNumRecordsIn = sourceMetricData.getNumRecordsIn().getCount();
            Map<String, Long> readPhaseMetricMap = sourceMetricData.getReadPhaseMetricMap().entrySet().stream().collect(
                    Collectors.toMap(v -> v.getKey().getPhase(), e -> e.getValue().getReadPhase().getCount()));
            Map<String, MySqlTableMetric> tableMetricMap = sourceMetricData.getSubSourceMetricMap().entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey,
                            e -> new MySqlTableMetric(e.getValue().getNumRecordsIn().getCount(),
                                    e.getValue().getNumBytesIn().getCount())));
            unfinishedSplits
                    .add(new MySqlMetricSplit(countNumBytesIn, countNumRecordsIn, readPhaseMetricMap, tableMetricMap));
        }
        return unfinishedSplits;
    }

    @Override
    protected void onSplitFinished(Map<String, MySqlSplitState> finishedSplitIds) {
        boolean requestNextSplit = true;
        for (MySqlSplitState mySqlSplitState : finishedSplitIds.values()) {
            MySqlSplit mySqlSplit = mySqlSplitState.toMySqlSplit();
            if (mySqlSplit.isBinlogSplit()) {
                LOG.info(
                        "binlog split reader suspended due to newly added table, offset {}",
                        mySqlSplitState.asBinlogSplitState().getStartingOffset());
                mySqlSourceReaderContext.resetStopBinlogSplitReader();
                suspendedBinlogSplit = toSuspendedBinlogSplit(mySqlSplit.asBinlogSplit());
                context.sendSourceEventToCoordinator(new SuspendBinlogReaderAckEvent());
                // do not request next split when the reader is suspended, the suspended reader will
                // automatically request the next split after it has been wakeup
                requestNextSplit = false;
            } else {
                finishedUnackedSplits.put(mySqlSplit.splitId(), mySqlSplit.asSnapshotSplit());
            }
        }
        reportFinishedSnapshotSplitsIfNeed();
        LOG.info("request for new split finished unacked splits:{}", finishedUnackedSplits);
        if (requestNextSplit) {
            context.sendSplitRequest();
        }
    }

    @Override
    public void addSplits(List<MySqlSplit> splits) {
        // restore for finishedUnackedSplits
        List<MySqlSplit> unfinishedSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            LOG.info("Add Split: " + split);
            if (split.isMetricSplit()) {
                MySqlMetricSplit mysqlMetricSplit = (MySqlMetricSplit) split;
                LOG.info("inlong-metric-states restore metricSplit:{}", mysqlMetricSplit);
                sourceReaderMetrics.initMetrics(mysqlMetricSplit.getNumRecordsIn(),
                        mysqlMetricSplit.getNumBytesIn(), mysqlMetricSplit.getReadPhaseMetricMap(),
                        mysqlMetricSplit.getTableMetricMap());
                LOG.info("inlong-metric-states restore sourceReaderMetrics:{}",
                        sourceReaderMetrics.getSourceMetricData());
                continue;
            }
            if (split.isSnapshotSplit()) {
                MySqlSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                MySqlBinlogSplit binlogSplit = split.asBinlogSplit();
                // the binlog split is suspended
                if (binlogSplit.isSuspended()) {
                    suspendedBinlogSplit = binlogSplit;
                } else if (!binlogSplit.isCompletedSplit()) {
                    uncompletedBinlogSplits.put(split.splitId(), split.asBinlogSplit());
                    requestBinlogSplitMetaIfNeeded(split.asBinlogSplit());
                } else {
                    uncompletedBinlogSplits.remove(split.splitId());
                    MySqlBinlogSplit mySqlBinlogSplit =
                            discoverTableSchemasForBinlogSplit(split.asBinlogSplit());
                    mySqlBinlogSplit.getFinishedSnapshotSplitInfos()
                            .sort(Comparator.comparingInt(splitInfo -> chunkId(splitInfo.getSplitId())));
                    unfinishedSplits.add(mySqlBinlogSplit);
                }
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including binlog split) to SourceReaderBase
        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        }
    }

    private MySqlBinlogSplit discoverTableSchemasForBinlogSplit(MySqlBinlogSplit split) {
        final String splitId = split.splitId();
        if (split.getTableSchemas().isEmpty() || sourceConfig.isScanNewlyAddedTableEnabled()) {
            try (MySqlConnection jdbc =
                    DebeziumUtils.createMySqlConnection(sourceConfig.getDbzConfiguration())) {
                Map<TableId, TableChanges.TableChange> tableSchemas;
                if (split.getTableSchemas().isEmpty()) {
                    tableSchemas =
                            TableDiscoveryUtils.discoverSchemaForCapturedTableSchemas(sourceConfig, jdbc);
                    LOG.info("The table schema discovery for binlog split {} success", splitId);
                } else {
                    List<TableId> existedTables = new ArrayList<>(split.getTableSchemas().keySet());
                    tableSchemas =
                            TableDiscoveryUtils.discoverSchemaForNewAddedTables(existedTables, sourceConfig, jdbc);
                    LOG.info(
                            "The table schema discovery for new added tables of binlog split {} success",
                            split.splitId());
                }
                return MySqlBinlogSplit.fillTableSchemas(split, tableSchemas);
            } catch (SQLException e) {
                LOG.error("Failed to obtains table schemas due to {}", e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOG.warn(
                    "The binlog split {} has table schemas yet, skip the table schema discovery",
                    split);
            return split;
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            LOG.debug(
                    "The subtask {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            LOG.debug(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof BinlogSplitMetaEvent) {
            LOG.debug(
                    "The subtask {} receives binlog meta with group id {}.",
                    subtaskId,
                    ((BinlogSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetaDataForBinlogSplit((BinlogSplitMetaEvent) sourceEvent);
        } else if (sourceEvent instanceof SuspendBinlogReaderEvent) {
            mySqlSourceReaderContext.setStopBinlogSplitReader();
        } else if (sourceEvent instanceof WakeupReaderEvent) {
            WakeupReaderEvent wakeupReaderEvent = (WakeupReaderEvent) sourceEvent;
            if (wakeupReaderEvent.getTarget() == SNAPSHOT_READER) {
                context.sendSplitRequest();
            } else {
                if (suspendedBinlogSplit != null) {
                    context.sendSourceEventToCoordinator(
                            new LatestFinishedSplitsSizeRequestEvent());
                }
            }
        } else if (sourceEvent instanceof LatestFinishedSplitsSizeEvent) {
            if (suspendedBinlogSplit != null) {
                final int finishedSplitsSize =
                        ((LatestFinishedSplitsSizeEvent) sourceEvent).getLatestFinishedSplitsSize();
                final MySqlBinlogSplit binlogSplit =
                        toNormalBinlogSplit(suspendedBinlogSplit, finishedSplitsSize);
                suspendedBinlogSplit = null;
                this.addSplits(Collections.singletonList(binlogSplit));
            }
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
            for (MySqlSnapshotSplit split : finishedUnackedSplits.values()) {
                finishedOffsets.put(split.splitId(), split.getHighWatermark());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedOffsets);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.debug(
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    private void requestBinlogSplitMetaIfNeeded(MySqlBinlogSplit binlogSplit) {
        final String splitId = binlogSplit.splitId();
        if (!binlogSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            BinlogSplitMetaRequestEvent splitMetaRequestEvent =
                    new BinlogSplitMetaRequestEvent(splitId, nextMetaGroupId);
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("The meta of binlog split {} has been collected success", splitId);
            this.addSplits(Arrays.asList(binlogSplit));
        }
    }

    private void fillMetaDataForBinlogSplit(BinlogSplitMetaEvent metadataEvent) {
        MySqlBinlogSplit binlogSplit = uncompletedBinlogSplits.get(metadataEvent.getSplitId());
        if (binlogSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
                List<FinishedSnapshotSplitInfo> metaDataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(FinishedSnapshotSplitInfo::deserialize)
                                .collect(Collectors.toList());
                uncompletedBinlogSplits.put(
                        binlogSplit.splitId(),
                        MySqlBinlogSplit.appendFinishedSplitInfos(binlogSplit, metaDataGroup));

                LOG.info("Fill meta data of group {} to binlog split", metaDataGroup.size());
            } else {
                LOG.warn(
                        "Received out of oder binlog meta event for split {}, "
                                + "the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestBinlogSplitMetaIfNeeded(binlogSplit);
        } else {
            LOG.warn(
                    "Received binlog meta event for split {}, but the uncompleted split map does not contain it",
                    metadataEvent.getSplitId());
        }
    }

    @Override
    protected MySqlSplit toSplitType(String splitId, MySqlSplitState splitState) {
        return splitState.toMySqlSplit();
    }
}
