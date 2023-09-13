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

import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.inlong.sort.jdbc.dialect.PostgresDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class TableBatchDeleteExecutor implements JdbcBatchStatementExecutor<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(TableBatchDeleteExecutor.class);
    private Connection connection;

    private transient PreparedStatement st;
    private final String[] pkNames;
    private final LogicalType[] pkTypes;
    private List<RowData> rowData = new ArrayList<>();
    private final String tableName;
    private final int[] pkFields;
    private final int MAX_PREPARE = 10000;
    private final PostgresDialect dialect;

    /**
     * Keep in mind object reuse: if it's on then key extractor may be required to return new
     * object.
     */
    public TableBatchDeleteExecutor(PostgresDialect dialect, String[] pkNames,
            LogicalType[] pkTypes, String tableName, int[] pkFields) {
        this.dialect = dialect;
        this.pkNames = pkNames;
        this.pkTypes = pkTypes;
        this.tableName = tableName;
        this.pkFields = pkFields;
    }

    @Override
    public void prepareStatements(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void addToBatch(RowData record) {
        // LOG.error("TableBatchDeleteExecutor addToBatch");
        rowData.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        // LOG.warn("delete executor executing batch");
        int deletedCount = 0;
        int round = (rowData.size() + MAX_PREPARE - 1) / MAX_PREPARE;
        for (int i = 0; i < round; ++i) {
            int start = i * MAX_PREPARE;
            int end = Math.min((i + 1) * MAX_PREPARE, rowData.size());
            st = dialect.getDeleteStatement(connection, tableName, pkNames, pkTypes,
                    rowData.subList(start, end), pkFields);
            if (st == null) {
                continue;
            }
            deletedCount += st.executeUpdate();
            st.close();
        }
        LOG.info("batch = {}, round = {}, Deleted count = {}", rowData.size(), round, deletedCount);
        rowData.clear();
    }

    public void clearBatch() {
        rowData.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
    }
}
