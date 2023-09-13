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

package org.apache.inlong.sort.jdbc.executor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Currently, this statement executor is only used for table/sql to buffer insert/update/delete
 * events, and reduce them in buffer before submit to external database.
 */
public final class TableBufferReducedStatementExecutor
        implements
            JdbcBatchStatementExecutor<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(TableBufferReducedStatementExecutor.class);

    private final JdbcBatchStatementExecutor<RowData> insertExecutor;
    private final JdbcBatchStatementExecutor<RowData> deleteExecutor;
    private final JdbcBatchStatementExecutor<RowData> updateInsertExecutor;
    private final JdbcBatchStatementExecutor<RowData> updateDeleteExecutor;
    private final JdbcBatchStatementExecutor<RowData> insertDeleteExecutor;
    private final JdbcBatchStatementExecutor<RowData> backUpInsertExecutor;
    private final Function<RowData, RowData> keyExtractor;
    private final Function<RowData, RowData> valueTransform;
    private Connection connection;
    // the mapping is [KEY, <+/-, VALUE>]
    private final Map<RowData, Tuple2<RowKind, RowData>> reduceBuffer = new HashMap<>();

    public TableBufferReducedStatementExecutor(
            JdbcBatchStatementExecutor<RowData> insertExecutor,
            JdbcBatchStatementExecutor<RowData> backUpInsertExecutor,
            JdbcBatchStatementExecutor<RowData> updateInsertExecutor,
            JdbcBatchStatementExecutor<RowData> updateDeleteExecutor,
            JdbcBatchStatementExecutor<RowData> deleteExecutor,
            JdbcBatchStatementExecutor<RowData> insertDeleteExecutor,
            Function<RowData, RowData> keyExtractor,
            Function<RowData, RowData> valueTransform) {
        this.insertExecutor = insertExecutor;
        this.deleteExecutor = deleteExecutor;
        this.keyExtractor = keyExtractor;
        this.valueTransform = valueTransform;
        this.updateInsertExecutor = updateInsertExecutor;
        this.updateDeleteExecutor = updateDeleteExecutor;
        this.insertDeleteExecutor = insertDeleteExecutor;
        this.backUpInsertExecutor = backUpInsertExecutor;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        insertExecutor.prepareStatements(connection);
        deleteExecutor.prepareStatements(connection);
        updateInsertExecutor.prepareStatements(connection);
        updateDeleteExecutor.prepareStatements(connection);
        insertDeleteExecutor.prepareStatements(connection);
        backUpInsertExecutor.prepareStatements(connection);
        this.connection = connection;
    }

    @Override
    public void addToBatch(RowData record) throws SQLException {
        RowData key = keyExtractor.apply(record);
        RowData value = valueTransform.apply(record); // copy or not
        reduceBuffer.put(key, Tuple2.of(record.getRowKind(), value));
    }

    @Override
    public void executeBatch() throws SQLException {
        // LOG.warn("using custom table buffer reduced executor!");
        for (Map.Entry<RowData, Tuple2<RowKind, RowData>> entry : reduceBuffer.entrySet()) {
            switch (entry.getValue().f0) {
                case INSERT:
                    insertDeleteExecutor.addToBatch(entry.getKey());
                    insertExecutor.addToBatch(entry.getValue().f1);
                    backUpInsertExecutor.addToBatch(entry.getValue().f1);
                    break;
                case UPDATE_AFTER:
                    updateDeleteExecutor.addToBatch(entry.getKey());
                    updateInsertExecutor.addToBatch(entry.getValue().f1);
                    break;
                case DELETE:
                    deleteExecutor.addToBatch(entry.getKey());
                    break;
                case UPDATE_BEFORE:
                    break;// 直接丢弃
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER,"
                                            + " DELETE, but get: %s.",
                                    entry.getValue().f0));
            }
        }
        try {
            insertExecutor.executeBatch();
        } catch (SQLException e) {
            LOG.warn("buffer sql exception", e);
            executeInsertBatchCommit();
        } finally {
            /**
             * 清空 insertDeleteExecutor  backUpInsertExecutor，避免内存泄漏
             */
            if (backUpInsertExecutor instanceof TableCopyStatementExecutor) {
                ((TableCopyStatementExecutor) backUpInsertExecutor).clearBatch();
            } else {
                ((TableSimpleStatementExecutor) backUpInsertExecutor).clearBatch();
            }
            ((TableBatchDeleteExecutor) insertDeleteExecutor).clearBatch();
        }
        deleteExecutor.executeBatch();
        try {
            connection.setAutoCommit(false);
            updateDeleteExecutor.executeBatch();
            updateInsertExecutor.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            if (!e.getMessage().contains("duplicate key")) {
                LOG.error("second time copy error", e);
                throw e;
            }
            LOG.error("second time unique constraint");
        } finally {
            connection.setAutoCommit(true);
        }

        reduceBuffer.clear();
    }

    public void executeInsertBatchCommit() throws SQLException {
        try {
            connection.setAutoCommit(false);
            insertDeleteExecutor.executeBatch();
            backUpInsertExecutor.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();

            // Retry fails, always throw exceptions
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        insertExecutor.closeStatements();
        deleteExecutor.closeStatements();
        updateInsertExecutor.closeStatements();
        updateDeleteExecutor.closeStatements();
        insertDeleteExecutor.closeStatements();
        backUpInsertExecutor.closeStatements();
    }
}
