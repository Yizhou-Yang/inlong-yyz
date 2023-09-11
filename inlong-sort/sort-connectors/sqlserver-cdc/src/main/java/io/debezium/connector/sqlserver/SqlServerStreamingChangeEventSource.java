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

package io.debezium.connector.sqlserver;

import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.ChangeTable;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Copied from Debezium project(1.9.7.final) to add method {@link
 * SqlServerStreamingChangeEventSource#afterHandleLsn(SqlServerPartition, Lsn)}. Also implemented
 * {@link SqlServerStreamingChangeEventSource#execute( ChangeEventSourceContext, SqlServerPartition,
 * SqlServerOffsetContext)}. A {@link StreamingChangeEventSource} based on SQL Server change data
 * capture functionality. A main loop polls database DDL change and change data tables and turns
 * them into change events.
 *
 * <p>The connector uses CDC functionality of SQL Server that is implemented as as a process that
 * monitors source table and write changes from the table into the change table.
 *
 * <p>The main loop keeps a pointer to the LSN of changes that were already processed. It queries
 * all change tables and get result set of changes. It always finds the smallest LSN across all
 * tables and the change is converted into the event message and sent downstream. The process
 * repeats until all result sets are empty. The LSN is marked and the procedure repeats.
 *
 * <p>The schema changes detection follows the procedure recommended by SQL Server CDC
 * documentation. The database operator should create one more capture process (and table) when a
 * table schema is updated. The code detects presence of two change tables for a single source
 * table. It decides which table is the new one depending on LSNs stored in them. The loop streams
 * changes from the older table till there are events in new table with the LSN larger than in the
 * old one. Then the change table is switched and streaming is executed from the new one.
 */
public class SqlServerStreamingChangeEventSource
        implements
            StreamingChangeEventSource<SqlServerPartition, SqlServerOffsetContext> {

    private static final Pattern MISSING_CDC_FUNCTION_CHANGES_ERROR =
            Pattern.compile("Invalid object name '(.*)\\.cdc.fn_cdc_get_all_changes_(.*)'\\.");

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SqlServerStreamingChangeEventSource.class);

    private static final Duration DEFAULT_INTERVAL_BETWEEN_COMMITS = Duration.ofMinutes(1);
    private static final int INTERVAL_BETWEEN_COMMITS_BASED_ON_POLL_FACTOR = 3;

    /** Connection used for reading CDC tables. */
    private final SqlServerConnection dataConnection;

    /**
     * A separate connection for retrieving details of the schema changes; without it, adaptive
     * buffering will not work.
     *
     * @link
     *     https://docs.microsoft.com/en-us/sql/connect/jdbc/using-adaptive-buffering?view=sql-server-2017#guidelines-for-using-adaptive-buffering
     */
    private final SqlServerConnection metadataConnection;

    private final EventDispatcher<SqlServerPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;
    private final Duration pollInterval;
    private final SqlServerConnectorConfig connectorConfig;

    private final ElapsedTimeStrategy pauseBetweenCommits;
    private final Map<SqlServerPartition, SqlServerStreamingExecutionContext> streamingExecutionContexts;

    public SqlServerStreamingChangeEventSource(
            SqlServerConnectorConfig connectorConfig,
            SqlServerConnection dataConnection,
            SqlServerConnection metadataConnection,
            EventDispatcher<SqlServerPartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            SqlServerDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.pollInterval = connectorConfig.getPollInterval();
        final Duration intervalBetweenCommitsBasedOnPoll =
                this.pollInterval.multipliedBy(INTERVAL_BETWEEN_COMMITS_BASED_ON_POLL_FACTOR);
        this.pauseBetweenCommits =
                ElapsedTimeStrategy.constant(
                        clock,
                        DEFAULT_INTERVAL_BETWEEN_COMMITS.compareTo(
                                intervalBetweenCommitsBasedOnPoll) > 0
                                        ? DEFAULT_INTERVAL_BETWEEN_COMMITS.toMillis()
                                        : intervalBetweenCommitsBasedOnPoll.toMillis());
        this.pauseBetweenCommits.hasElapsed();
        this.streamingExecutionContexts = new HashMap<>();
    }

    @Override
    public void execute(
            ChangeEventSourceContext context,
            SqlServerPartition partition,
            SqlServerOffsetContext offsetContext)
            throws InterruptedException {
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);

        LOGGER.info("Starting streaming");

        while (context.isRunning()) {
            boolean streamedEvents = executeIteration(context, partition, offsetContext);

            if (!streamedEvents) {
                metronome.pause();
            }
        }

        LOGGER.info("Finished streaming");
    }

    @Override
    public boolean executeIteration(
            ChangeEventSourceContext context,
            SqlServerPartition partition,
            SqlServerOffsetContext offsetContext)
            throws InterruptedException {
        if (connectorConfig.getSnapshotMode().equals(SnapshotMode.INITIAL_ONLY)) {
            LOGGER.info("Streaming is not enabled in current configuration");
            return false;
        }

        final String databaseName = partition.getDatabaseName();

        try {
            final SqlServerStreamingExecutionContext streamingExecutionContext =
                    streamingExecutionContexts.getOrDefault(
                            partition,
                            new SqlServerStreamingExecutionContext(
                                    new PriorityQueue<>(
                                            (x, y) -> x.getStopLsn().compareTo(y.getStopLsn())),
                                    new AtomicReference<>(),
                                    offsetContext.getChangePosition(),
                                    new AtomicBoolean(false),
                                    // LSN should be increased for the first run only immediately
                                    // after snapshot completion
                                    // otherwise we might skip an incomplete transaction after
                                    // restart
                                    offsetContext.isSnapshotCompleted()));

            if (!streamingExecutionContexts.containsKey(partition)) {
                streamingExecutionContexts.put(partition, streamingExecutionContext);
                LOGGER.info(
                        "Last position recorded in offsets is {}[{}]",
                        offsetContext.getChangePosition(),
                        offsetContext.getEventSerialNo());
            }

            final Queue<SqlServerChangeTable> schemaChangeCheckpoints =
                    streamingExecutionContext.getSchemaChangeCheckpoints();
            final AtomicReference<SqlServerChangeTable[]> tablesSlot =
                    streamingExecutionContext.getTablesSlot();
            final TxLogPosition lastProcessedPositionOnStart = offsetContext.getChangePosition();
            final long lastProcessedEventSerialNoOnStart = offsetContext.getEventSerialNo();
            final AtomicBoolean changesStoppedBeingMonotonic =
                    streamingExecutionContext.getChangesStoppedBeingMonotonic();
            final int maxTransactionsPerIteration =
                    connectorConfig.getMaxTransactionsPerIteration();

            TxLogPosition lastProcessedPosition =
                    streamingExecutionContext.getLastProcessedPosition();

            if (context.isRunning()) {
                commitTransaction();
                final Lsn toLsn =
                        getToLsn(
                                dataConnection,
                                databaseName,
                                lastProcessedPosition,
                                maxTransactionsPerIteration);

                // Shouldn't happen if the agent is running, but it is better to guard against such
                // situation
                if (!toLsn.isAvailable()) {
                    LOGGER.warn(
                            "No maximum LSN recorded in the database; please ensure that the SQL Server Agent is running");
                    return false;
                }
                // There is no change in the database
                if (toLsn.compareTo(lastProcessedPosition.getCommitLsn()) <= 0
                        && streamingExecutionContext.getShouldIncreaseFromLsn()) {
                    LOGGER.debug("No change in the database");
                    return false;
                }

                // Reading interval is inclusive so we need to move LSN forward but not for first
                // run as TX might not be streamed completely
                final Lsn fromLsn =
                        lastProcessedPosition.getCommitLsn().isAvailable()
                                && streamingExecutionContext.getShouldIncreaseFromLsn()
                                        ? dataConnection.incrementLsn(
                                                databaseName, lastProcessedPosition.getCommitLsn())
                                        : lastProcessedPosition.getCommitLsn();
                streamingExecutionContext.setShouldIncreaseFromLsn(true);

                while (!schemaChangeCheckpoints.isEmpty()) {
                    migrateTable(partition, schemaChangeCheckpoints, offsetContext);
                }
                if (!dataConnection.getNewChangeTables(databaseName, fromLsn, toLsn).isEmpty()) {
                    final SqlServerChangeTable[] tables =
                            getChangeTablesToQuery(partition, offsetContext, toLsn);
                    tablesSlot.set(tables);
                    for (SqlServerChangeTable table : tables) {
                        if (table.getStartLsn().isBetween(fromLsn, toLsn)) {
                            LOGGER.info("Schema will be changed for {}", table);
                            schemaChangeCheckpoints.add(table);
                        }
                    }
                }
                if (tablesSlot.get() == null) {
                    tablesSlot.set(getChangeTablesToQuery(partition, offsetContext, toLsn));
                }
                try {
                    dataConnection.getChangesForTables(
                            databaseName,
                            tablesSlot.get(),
                            fromLsn,
                            toLsn,
                            resultSets -> {
                                long eventSerialNoInInitialTx = 1;
                                final int tableCount = resultSets.length;
                                final SqlServerChangeTablePointer[] changeTables =
                                        new SqlServerChangeTablePointer[tableCount];
                                final SqlServerChangeTable[] tables = tablesSlot.get();

                                for (int i = 0; i < tableCount; i++) {
                                    changeTables[i] =
                                            new SqlServerChangeTablePointer(
                                                    tables[i],
                                                    resultSets[i],
                                                    connectorConfig.getSourceTimestampMode());
                                    changeTables[i].next();
                                }

                                for (;;) {
                                    SqlServerChangeTablePointer tableWithSmallestLsn = null;
                                    for (SqlServerChangeTablePointer changeTable : changeTables) {
                                        if (changeTable.isCompleted()) {
                                            continue;
                                        }
                                        if (tableWithSmallestLsn == null
                                                || changeTable.compareTo(tableWithSmallestLsn) < 0) {
                                            tableWithSmallestLsn = changeTable;
                                        }
                                    }
                                    if (tableWithSmallestLsn == null) {
                                        // No more LSNs available
                                        break;
                                    }

                                    if (!(tableWithSmallestLsn.getChangePosition().isAvailable()
                                            && tableWithSmallestLsn
                                                    .getChangePosition()
                                                    .getInTxLsn()
                                                    .isAvailable())) {
                                        LOGGER.error(
                                                "Skipping change {} as its LSN is NULL which is not expected",
                                                tableWithSmallestLsn);
                                        tableWithSmallestLsn.next();
                                        continue;
                                    }

                                    if (tableWithSmallestLsn.isNewTransaction()
                                            && changesStoppedBeingMonotonic.get()) {
                                        LOGGER.info(
                                                "Resetting changesStoppedBeingMonotonic as transaction changes");
                                        changesStoppedBeingMonotonic.set(false);
                                    }

                                    // After restart for changes that are not monotonic to avoid
                                    // data loss
                                    if (tableWithSmallestLsn
                                            .isCurrentPositionSmallerThanPreviousPosition()) {
                                        LOGGER.info(
                                                "Disabling skipping changes due to not monotonic order of changes");
                                        changesStoppedBeingMonotonic.set(true);
                                    }

                                    // After restart for changes that were executed before the last
                                    // committed offset
                                    if (!changesStoppedBeingMonotonic.get()
                                            && tableWithSmallestLsn
                                                    .getChangePosition()
                                                    .compareTo(lastProcessedPositionOnStart) < 0) {
                                        LOGGER.info(
                                                "Skipping change {} as its position is smaller than the last recorded position {}",
                                                tableWithSmallestLsn,
                                                lastProcessedPositionOnStart);
                                        tableWithSmallestLsn.next();
                                        continue;
                                    }
                                    // After restart for change that was the last committed and
                                    // operations in it before the last committed offset
                                    if (!changesStoppedBeingMonotonic.get()
                                            && tableWithSmallestLsn
                                                    .getChangePosition()
                                                    .compareTo(lastProcessedPositionOnStart) == 0
                                            && eventSerialNoInInitialTx <= lastProcessedEventSerialNoOnStart) {
                                        LOGGER.info(
                                                "Skipping change {} as its order in the transaction {} is smaller than or equal to the last recorded operation {}[{}]",
                                                tableWithSmallestLsn,
                                                eventSerialNoInInitialTx,
                                                lastProcessedPositionOnStart,
                                                lastProcessedEventSerialNoOnStart);
                                        eventSerialNoInInitialTx++;
                                        tableWithSmallestLsn.next();
                                        continue;
                                    }
                                    if (tableWithSmallestLsn
                                            .getChangeTable()
                                            .getStopLsn()
                                            .isAvailable()
                                            && tableWithSmallestLsn
                                                    .getChangeTable()
                                                    .getStopLsn()
                                                    .compareTo(
                                                            tableWithSmallestLsn
                                                                    .getChangePosition()
                                                                    .getCommitLsn()) <= 0) {
                                        LOGGER.debug(
                                                "Skipping table change {} as its stop LSN is smaller than the last recorded LSN {}",
                                                tableWithSmallestLsn,
                                                tableWithSmallestLsn.getChangePosition());
                                        tableWithSmallestLsn.next();
                                        continue;
                                    }
                                    LOGGER.trace("Processing change {}", tableWithSmallestLsn);
                                    LOGGER.trace(
                                            "Schema change checkpoints {}",
                                            schemaChangeCheckpoints);
                                    if (!schemaChangeCheckpoints.isEmpty()) {
                                        if (tableWithSmallestLsn
                                                .getChangePosition()
                                                .getCommitLsn()
                                                .compareTo(
                                                        schemaChangeCheckpoints
                                                                .peek()
                                                                .getStartLsn()) >= 0) {
                                            migrateTable(
                                                    partition,
                                                    schemaChangeCheckpoints,
                                                    offsetContext);
                                        }
                                    }
                                    final TableId tableId =
                                            tableWithSmallestLsn
                                                    .getChangeTable()
                                                    .getSourceTableId();
                                    final int operation = tableWithSmallestLsn.getOperation();
                                    final Object[] data = tableWithSmallestLsn.getData();

                                    // UPDATE consists of two consecutive events, first event
                                    // contains
                                    // the row before it was updated and the second the row after
                                    // it was updated
                                    int eventCount = 1;
                                    if (operation == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE) {
                                        if (!tableWithSmallestLsn.next()
                                                || tableWithSmallestLsn
                                                        .getOperation() != SqlServerChangeRecordEmitter.OP_UPDATE_AFTER) {
                                            throw new IllegalStateException(
                                                    "The update before event at "
                                                            + tableWithSmallestLsn
                                                                    .getChangePosition()
                                                            + " for table "
                                                            + tableId
                                                            + " was not followed by after event.\n Please report this as a bug together with a events around given LSN.");
                                        }
                                        eventCount = 2;
                                    }
                                    final Object[] dataNext =
                                            (operation == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE)
                                                    ? tableWithSmallestLsn.getData()
                                                    : null;

                                    offsetContext.setChangePosition(
                                            tableWithSmallestLsn.getChangePosition(), eventCount);
                                    offsetContext.event(
                                            tableWithSmallestLsn
                                                    .getChangeTable()
                                                    .getSourceTableId(),
                                            connectorConfig
                                                    .getSourceTimestampMode()
                                                    .getTimestamp(
                                                            clock,
                                                            tableWithSmallestLsn.getResultSet()));

                                    dispatcher.dispatchDataChangeEvent(
                                            partition,
                                            tableId,
                                            new SqlServerChangeRecordEmitter(
                                                    partition,
                                                    offsetContext,
                                                    operation,
                                                    data,
                                                    dataNext,
                                                    clock));
                                    tableWithSmallestLsn.next();
                                }
                            });
                    streamingExecutionContext.setLastProcessedPosition(
                            TxLogPosition.valueOf(toLsn));
                    // Terminate the transaction otherwise CDC could not be disabled for tables
                    dataConnection.rollback();
                    // Determine whether to continue streaming in sqlserver cdc snapshot phase
                    afterHandleLsn(partition, toLsn);
                } catch (SQLException e) {
                    tablesSlot.set(
                            processErrorFromChangeTableQuery(databaseName, e, tablesSlot.get()));
                }
            }
        } catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }

        return true;
    }

    private void commitTransaction() throws SQLException {
        // When reading from read-only Always On replica the default and only transaction isolation
        // is snapshot. This means that CDC metadata are not visible for long-running transactions.
        // It is thus necessary to restart the transaction before every read.
        // For R/W database it is important to execute regular commits to maintain the size of
        // TempDB
        if (connectorConfig.isReadOnlyDatabaseConnection() || pauseBetweenCommits.hasElapsed()) {
            dataConnection.commit();
            metadataConnection.commit();
        }
    }

    private void migrateTable(
            SqlServerPartition partition,
            final Queue<SqlServerChangeTable> schemaChangeCheckpoints,
            SqlServerOffsetContext offsetContext)
            throws InterruptedException, SQLException {
        final SqlServerChangeTable newTable = schemaChangeCheckpoints.poll();
        LOGGER.info("Migrating schema to {}", newTable);
        Table oldTableSchema = schema.tableFor(newTable.getSourceTableId());
        Table tableSchema =
                metadataConnection.getTableSchemaFromTable(partition.getDatabaseName(), newTable);
        if (oldTableSchema.equals(tableSchema)) {
            LOGGER.info("Migration skipped, no table schema changes detected.");
            return;
        }
        dispatcher.dispatchSchemaChangeEvent(
                partition,
                newTable.getSourceTableId(),
                new SqlServerSchemaChangeEventEmitter(
                        partition,
                        offsetContext,
                        newTable,
                        tableSchema,
                        SchemaChangeEventType.ALTER));
        newTable.setSourceTable(tableSchema);
    }

    private SqlServerChangeTable[] processErrorFromChangeTableQuery(
            String databaseName, SQLException exception, SqlServerChangeTable[] currentChangeTables)
            throws Exception {
        final Matcher m = MISSING_CDC_FUNCTION_CHANGES_ERROR.matcher(exception.getMessage());
        if (m.matches() && m.group(1).equals(databaseName)) {
            final String captureName = m.group(2);
            LOGGER.info("Table is no longer captured with capture instance {}", captureName);
            return Arrays.stream(currentChangeTables)
                    .filter(x -> !x.getCaptureInstance().equals(captureName))
                    .toArray(SqlServerChangeTable[]::new);
        }
        throw exception;
    }

    private SqlServerChangeTable[] getChangeTablesToQuery(
            SqlServerPartition partition, SqlServerOffsetContext offsetContext, Lsn toLsn)
            throws SQLException, InterruptedException {
        final String databaseName = partition.getDatabaseName();
        final List<SqlServerChangeTable> changeTables =
                dataConnection.getChangeTables(databaseName, toLsn);
        if (changeTables.isEmpty()) {
            LOGGER.warn(
                    "No table has enabled CDC or security constraints prevents getting the list of change tables");
        }

        final Map<TableId, List<SqlServerChangeTable>> includeListChangeTables =
                changeTables.stream()
                        .filter(
                                changeTable -> {
                                    if (connectorConfig
                                            .getTableFilters()
                                            .dataCollectionFilter()
                                            .isIncluded(changeTable.getSourceTableId())) {
                                        return true;
                                    } else {
                                        LOGGER.info(
                                                "CDC is enabled for table {} but the table is not whitelisted by connector",
                                                changeTable);
                                        return false;
                                    }
                                })
                        .collect(Collectors.groupingBy(ChangeTable::getSourceTableId));

        if (includeListChangeTables.isEmpty()) {
            LOGGER.warn(
                    "No whitelisted table has enabled CDC, whitelisted table list does not contain any table with CDC enabled or no table match the white/blacklist filter(s)");
        }

        final List<SqlServerChangeTable> tables = new ArrayList<>();
        for (List<SqlServerChangeTable> captures : includeListChangeTables.values()) {
            SqlServerChangeTable currentTable = captures.get(0);
            if (captures.size() > 1) {
                SqlServerChangeTable futureTable;
                if (captures.get(0).getStartLsn().compareTo(captures.get(1).getStartLsn()) < 0) {
                    futureTable = captures.get(1);
                } else {
                    currentTable = captures.get(1);
                    futureTable = captures.get(0);
                }
                currentTable.setStopLsn(futureTable.getStartLsn());
                futureTable.setSourceTable(
                        dataConnection.getTableSchemaFromTable(databaseName, futureTable));
                tables.add(futureTable);
                LOGGER.info(
                        "Multiple capture instances present for the same table: {} and {}",
                        currentTable,
                        futureTable);
            }
            if (schema.tableFor(currentTable.getSourceTableId()) == null) {
                LOGGER.info(
                        "Table {} is new to be monitored by capture instance {}",
                        currentTable.getSourceTableId(),
                        currentTable.getCaptureInstance());
                // We need to read the source table schema - nullability information cannot be
                // obtained from change table
                // There might be no start LSN in the new change table at this time so current
                // timestamp is used
                offsetContext.event(currentTable.getSourceTableId(), Instant.now());
                dispatcher.dispatchSchemaChangeEvent(
                        partition,
                        currentTable.getSourceTableId(),
                        new SqlServerSchemaChangeEventEmitter(
                                partition,
                                offsetContext,
                                currentTable,
                                dataConnection.getTableSchemaFromTable(databaseName, currentTable),
                                SchemaChangeEventType.CREATE));
            }

            // If a column was renamed, then the old capture instance had been dropped and a new one
            // created. In consequence, a table with out-dated schema might be assigned here.
            // A proper value will be set when migration happens.
            currentTable.setSourceTable(schema.tableFor(currentTable.getSourceTableId()));
            tables.add(currentTable);
        }

        return tables.toArray(new SqlServerChangeTable[tables.size()]);
    }

    /**
     * @return the log sequence number up until which the connector should query changes from the
     *     database.
     */
    private Lsn getToLsn(
            SqlServerConnection connection,
            String databaseName,
            TxLogPosition lastProcessedPosition,
            int maxTransactionsPerIteration)
            throws SQLException {

        if (maxTransactionsPerIteration == 0) {
            return connection.getMaxTransactionLsn(databaseName);
        }

        final Lsn fromLsn = lastProcessedPosition.getCommitLsn();

        if (!fromLsn.isAvailable()) {
            return connection.getNthTransactionLsnFromBeginning(
                    databaseName, maxTransactionsPerIteration);
        }

        return connection.getNthTransactionLsnFromLast(
                databaseName, fromLsn, maxTransactionsPerIteration);
    }

    /** expose control to the user to stop the connector. */
    protected void afterHandleLsn(SqlServerPartition partition, Lsn toLsn) {
        // do nothing
    }
}
