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
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.inlong.sort.jdbc.dialect.PostgresDialect;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * A {@link JdbcBatchStatementExecutor} that simply adds the records into batches of {@link
 * java.sql.PreparedStatement} and doesn't buffer records in memory. Only used in Table/SQL API.
 */
public final class TableCopyStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(TableCopyStatementExecutor.class);
    private static final char QUOTE = '"';
    private static final char ESCAPE = '\\';
    private static final char NEWLINE = '\n';
    private transient FieldNamedPreparedStatement st;
    private CopyManager mgr;
    private final String tableName;
    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;
    private List<RowData> rowData = new ArrayList<>();
    private String sql;

    private PipedOutputStream pipeOut = null;
    private PipedInputStream pipeIn = null;
    private final ExecutorService threadPool;
    private final String copyDelimiter;
    private final PostgresDialect dialect;

    /**
     * Keep in mind object reuse: if it's on then key extractor may be required to return new
     * object.
     */
    public TableCopyStatementExecutor(
            PostgresDialect dialect,
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            String copyDelimiter) {
        this.dialect = dialect;
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        threadPool = Executors.newFixedThreadPool(2);
        this.copyDelimiter = copyDelimiter;
    }

    public static String escapeString(String data) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < data.length(); ++i) {
            char c = data.charAt(i);
            switch (c) {
                case 0x00:
                    LOG.warn("字符串中发现非法字符 0x00，已经将其删除");
                    continue;
                case QUOTE:
                case ESCAPE:
                    sb.append(ESCAPE);
                default:
            }
            sb.append(c);
        }
        return sb.toString();
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        LOG.error("## prepareStatements");
        mgr = new CopyManager((BaseConnection) connection);
        sql = dialect.getCopySql(this.tableName, Arrays.asList(this.fieldNames), 0, copyDelimiter);
    }

    @Override
    public void addToBatch(RowData record) throws SQLException {
        rowData.add(record);
    }

    /**
     * Any occurrence within the value of a QUOTE character or the ESCAPE
     * character is preceded by the escape character.
     */

    @Override
    public void executeBatch() throws SQLException {
        if (rowData.size() < 1) {
            return;
        }
        pipeOut = new PipedOutputStream();
        try {
            pipeIn = new PipedInputStream(pipeOut);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FutureTask<Long> copyResult = new FutureTask<Long>(new Callable<Long>() {

            @Override
            public Long call() throws Exception {
                try {
                    long count = mgr.copyIn(sql, pipeIn);
                    LOG.info(tableName + " printing copy batch size " + count);
                    return count;
                } catch (SQLException e) {
                    if (!e.getMessage().contains("duplicate key value violates unique")) {
                        LOG.error("### SQLException", e);
                    }
                    throw new SQLException(e.getMessage());
                } catch (Exception e) {
                    LOG.error("### Ohter Exception", e);
                    throw e;
                } finally {
                    try {
                        pipeIn.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        });
        threadPool.submit(copyResult);
        LOG.info("printing copy rowdata size " + rowData.size());
        byte[] data = null;
        try {
            for (int i = 0; i < rowData.size(); i++) {
                RowData record = rowData.get(i);
                try {
                    data = serializeRecord(record);
                } catch (Exception e) {
                    LOG.error("record: {}", record);
                    LOG.error(Arrays.toString(e.getStackTrace()));
                    LOG.error("serializeRecord error", e);
                    throw new SQLException(e);
                }
                pipeOut.write(data);
            }
            pipeOut.flush();
            pipeOut.close();
        } catch (Exception e) {
            LOG.error(Arrays.toString(e.getStackTrace()));
            LOG.error("init copy write error", e);
            throw new SQLException(e);
        } finally {
            try {
                pipeOut.close();
            } catch (Exception e) {
                // ignore if failed to close pipe
            }
            pipeOut = null;
            rowData.clear();
        }
        try {
            copyResult.get();
        } catch (Exception e) {
            if (data != null && !e.getMessage().contains("duplicate key value violates unique")) {
                LOG.error("exception data({}):\n[{}]\n[{}]", data.length, new String(data), data);
            }
            throw new SQLException(e);
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        mgr = null;
    }

    /**
     * Non-printable characters are inserted as '\nnn' (octal) and '\' as '\\'.
     */
    protected String escapeBinary(byte[] data) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < data.length; ++i) {
            if (data[i] == '\\') {
                sb.append('\\');
                sb.append('\\');
            } else if (data[i] < 0x20 || data[i] > 0x7e) {
                byte b = data[i];
                char[] val = new char[3];
                val[2] = (char) ((b & 07) + '0');
                b >>= 3;
                val[1] = (char) ((b & 07) + '0');
                b >>= 3;
                val[0] = (char) ((b & 03) + '0');
                sb.append('\\');
                sb.append(val);
            } else {
                sb.append((char) (data[i]));
            }
        }

        return sb.toString();
    }

    protected byte[] serializeRecord(RowData rowData) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index < rowData.getArity(); index++) {
            LogicalType type = fieldTypes[index];
            if (!rowData.isNullAt(index)) {
                switch (type.getTypeRoot()) {
                    case BOOLEAN:
                        sb.append(rowData.getBoolean(index));
                        break;
                    case TINYINT:
                        sb.append(rowData.getByte(index));
                        break;
                    case SMALLINT:
                        sb.append(rowData.getShort(index));
                        break;
                    case INTEGER:
                    case INTERVAL_YEAR_MONTH:
                        sb.append(rowData.getInt(index));
                        break;
                    case BIGINT:
                    case INTERVAL_DAY_TIME:
                        sb.append(rowData.getLong(index));
                        break;
                    case FLOAT:
                        sb.append(rowData.getFloat(index));
                        break;
                    case DOUBLE:
                        sb.append(rowData.getDouble(index));
                        break;
                    case CHAR:
                    case VARCHAR:
                        StringData value = rowData.getString(index);
                        sb.append(QUOTE);
                        sb.append(escapeString(value.toString()));
                        sb.append(QUOTE);
                        break;
                    case BINARY:
                    case VARBINARY:
                        byte[] bb = rowData.getBinary(index);
                        if (bb != null) {
                            sb.append(escapeBinary(bb));
                        }
                        break;
                    case DATE:
                        Date dd = java.sql.Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(index)));
                        sb.append(String.format(
                                "%d-%02d-%02d", dd.getYear() + 1900, dd.getMonth() + 1, dd.getDate()));
                        break;
                    case TIME_WITHOUT_TIME_ZONE: {
                        final int timestampPrecision = ((TimeType) type).getPrecision();
                        TimestampData timeValue = rowData.getTimestamp(index, timestampPrecision);
                        Time tt = Time.valueOf(timeValue.toLocalDateTime().toLocalTime());
                        sb.append(tt);
                        break;
                    }
                    case TIMESTAMP_WITHOUT_TIME_ZONE: {
                        final int timestampPrecision = ((TimestampType) type).getPrecision();
                        TimestampData timeValue = rowData.getTimestamp(index, timestampPrecision);
                        sb.append(timeValue.toTimestamp());
                        break;
                    }
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                        final int timestampPrecision = ((LocalZonedTimestampType) type).getPrecision();
                        TimestampData timeValue = rowData.getTimestamp(index, timestampPrecision);
                        sb.append(timeValue.toTimestamp());
                        break;
                    }
                    case DECIMAL:
                        final int decimalPrecision = ((DecimalType) type).getPrecision();
                        final int decimalScale = ((DecimalType) type).getScale();
                        DecimalData decimalData = rowData.getDecimal(index, decimalPrecision, decimalScale);
                        if (decimalData != null) {
                            sb.append(decimalData.toBigDecimal());
                        }
                        break;
                    case ARRAY:
                    case MAP:
                    case MULTISET:
                    case ROW:
                    case RAW:
                    default:
                        throw new UnsupportedOperationException("Unsupported type:" + type);
                }
            } else {
                sb.append("\\NULL");
            }
            if (index < rowData.getArity() - 1) {
                sb.append(copyDelimiter);
            }
        }
        sb.append(NEWLINE);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public void clearBatch() {
        if (rowData != null) {
            rowData.clear();
        }
    }

}