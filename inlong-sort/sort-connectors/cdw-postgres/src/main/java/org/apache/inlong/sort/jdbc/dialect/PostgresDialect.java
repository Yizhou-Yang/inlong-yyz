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

package org.apache.inlong.sort.jdbc.dialect;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.inlong.sort.base.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.inlong.sort.jdbc.converter.postgres.PostgresRowConverter;
import org.apache.inlong.sort.jdbc.table.AbstractJdbcDialect;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** JDBC dialect for PostgreSQL. */
public class PostgresDialect extends AbstractJdbcDialect {

    private static final long serialVersionUID = 1L;

    private static final String QUERY_PRIMARY_KEY_SQL = "SELECT\n"
            + "string_agg(pg_attribute.attname, ',') AS pkColumn,\n"
            + " pg_class.relname AS tableName\n"
            + "            FROM\n"
            + "    pg_index, pg_class, pg_attribute\n"
            + "            WHERE\n"
            + "    pg_class.oid = ?::regclass AND\n"
            + "    indrelid = pg_class.oid AND\n"
            + "    pg_attribute.attrelid = pg_class.oid AND\n"
            + "    pg_attribute.attnum = any(pg_index.indkey) AND\n"
            + "    indisprimary \n "
            + "    GROUP BY \n"
            + "    pg_class.relname, pg_index.indkey;";

    // Define MAX/MIN precision of TIMESTAMP type according to PostgreSQL docs:
    // https://www.postgresql.org/docs/12/datatype-datetime.html
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of DECIMAL type according to PostgreSQL docs:
    // https://www.postgresql.org/docs/12/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
    private static final int MAX_DECIMAL_PRECISION = 1000;
    private static final int MIN_DECIMAL_PRECISION = 1;

    private static final long CHAR_MAX_LENGTH = 10485760;
    private static final long VACHAR_MAX_LENGTH = 10485760;

    public static final String WRITE_MODE_COPY = "copy";
    public static final String DEFAULT_COPY_DELIMITER = "|";

    private static final Set<String> RESOURCE_EXISTS_ERROS =
            new HashSet<>(Arrays.asList("42P04", "42P06", "42P07", "42701"));

    private static final Pattern UNKNOWN_COLUMN_PATTERN = Pattern.compile(".*ERROR"
            + ": column \"?'?([a-zA-Z0-9_]+)\"?'? of relation .* does not exist.*", Pattern.DOTALL);

    public static final Logger LOG = LoggerFactory.getLogger(PostgresDialect.class);

    public static void setStatement(
            LogicalType type,
            RowData val,
            int index,
            PreparedStatement statement,
            int statementIndex)
            throws SQLException {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                statement.setBoolean(statementIndex, val.getBoolean(index));
                break;
            case TINYINT:
                statement.setByte(statementIndex, val.getByte(index));
                break;
            case SMALLINT:
                statement.setShort(statementIndex, val.getShort(index));
                break;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                statement.setInt(statementIndex, val.getInt(index));
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                statement.setLong(statementIndex, val.getLong(index));
                break;
            case FLOAT:
                statement.setFloat(statementIndex, val.getFloat(index));
                break;
            case DOUBLE:
                statement.setDouble(statementIndex, val.getDouble(index));
                break;
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                statement.setString(statementIndex, val.getString(index).toString());
                break;
            case BINARY:
            case VARBINARY:
                statement.setBytes(statementIndex, val.getBinary(index));
                break;
            case DATE:
                statement.setDate(
                        statementIndex, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
                break;
            case TIME_WITHOUT_TIME_ZONE:
                statement.setTime(
                        statementIndex,
                        Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                statement.setTimestamp(
                        statementIndex, val.getTimestamp(index, timestampPrecision).toTimestamp());
                break;
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                statement.setBigDecimal(
                        statementIndex,
                        val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
                break;
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new PostgresRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.postgresql.Driver");
    }

    /** Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres. */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        List<String> uniqueKeyFieldList = Arrays.asList(uniqueKeyFields);
        String updateClause =
                Arrays.stream(fieldNames).filter(f -> !uniqueKeyFieldList.contains(f))
                        .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        String upsertCause = getInsertIntoStatement(tableName, fieldNames);
        if (StringUtils.isNotEmpty(updateClause)) {
            upsertCause = upsertCause + " ON CONFLICT ("
                    + uniqueColumns
                    + ")"
                    + " DO UPDATE SET "
                    + updateClause;
        }
        return Optional.of(upsertCause);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String dialectName() {
        return "PostgreSQL";
    }

    @Override
    public int maxDecimalPrecision() {
        return MAX_DECIMAL_PRECISION;
    }

    @Override
    public int minDecimalPrecision() {
        return MIN_DECIMAL_PRECISION;
    }

    @Override
    public int maxTimestampPrecision() {
        return MAX_TIMESTAMP_PRECISION;
    }

    @Override
    public int minTimestampPrecision() {
        return MIN_TIMESTAMP_PRECISION;
    }

    @Override
    public List<LogicalTypeRoot> unsupportedTypes() {
        // The data types used in PostgreSQL are list at:
        // https://www.postgresql.org/docs/12/datatype.html

        // TODO: We can't convert BINARY data type to
        // PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
        // LegacyTypeInfoDataTypeConverter.
        return Arrays.asList(
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                LogicalTypeRoot.INTERVAL_DAY_TIME,
                LogicalTypeRoot.MULTISET,
                LogicalTypeRoot.MAP,
                LogicalTypeRoot.ROW,
                LogicalTypeRoot.DISTINCT_TYPE,
                LogicalTypeRoot.STRUCTURED_TYPE,
                LogicalTypeRoot.NULL,
                LogicalTypeRoot.RAW,
                LogicalTypeRoot.SYMBOL,
                LogicalTypeRoot.UNRESOLVED);
    }

    @Override
    public PreparedStatement setQueryPrimaryKeySql(Connection conn,
            String tableIdentifier) throws SQLException {
        PreparedStatement st = conn.prepareStatement(QUERY_PRIMARY_KEY_SQL);
        st.setString(1, formatTableIdentifier(tableIdentifier));
        return st;
    }

    @Override
    public String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT INTO "
                + formatTableIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    public PreparedStatement getDeleteStatement(
            Connection connection,
            String tableName,
            String[] pkNames,
            LogicalType[] pkTypes,
            List<RowData> rowDataList,
            int[] pkfields)
            throws SQLException {
        if (rowDataList == null || rowDataList.size() < 1 || pkNames == null || pkNames.length < 1) {
            return null;
        }
        StringBuilder record = new StringBuilder();
        record.append("(");
        for (int i = 0; i < rowDataList.size(); i++) {
            RowData rowData = rowDataList.get(i);
            record.append("(");
            for (int index = 0; index < rowData.getArity(); index++) {
                record.append("?");
                if (index < rowData.getArity() - 1) {
                    record.append(",");
                }
            }
            record.append(")");
            if (i < rowDataList.size() - 1) {
                record.append(",");
            }
        }
        record.append(")");
        String finalSql =
                String.format("DELETE FROM %s WHERE (%s) IN %s", formatTableIdentifier(tableName),
                        constructColumnNameList(Arrays.asList(pkNames)), record);
        // LOG.info("DELETE STATEMENT:" + finalSql);
        PreparedStatement statement = connection.prepareStatement(finalSql);
        int currentIndex = 1;
        for (RowData rowData : rowDataList) {
            for (int index = 0; index < pkNames.length; index++) {
                /*
                 * bug: pkfields[index] corresponds to the position in original schema, but for delete statement, the
                 * rowdata is always related key values original code: setStatement(pkTypes[index], rowData,
                 * pkField[index], statement, currentIndex++);
                 */
                setStatement(pkTypes[index], rowData, index, statement, currentIndex++);
            }
        }
        return statement;
    }

    public String getCopySql(String tableName, List<String> columnList, int segment_reject_limit,
            String copyDelimiter) {
        StringBuilder sb = new StringBuilder().append("COPY ").append(formatTableIdentifier(tableName)).append("(")
                .append(constructColumnNameList(columnList))
                .append(") FROM STDIN WITH DELIMITER '").append(copyDelimiter)
                .append("' CSV QUOTE '\"' ESCAPE E'\\\\' NULL '\\NULL'");

        if (segment_reject_limit >= 2) {
            sb.append(" LOG ERRORS SEGMENT REJECT LIMIT ").append(segment_reject_limit).append(";");
        } else {
            sb.append(";");
        }

        String sql = sb.toString();
        LOG.info("getCopySql {}", sql);
        return sql;
    }

    private static String constructColumnNameList(List<String> columnList) {
        List<String> columns = new ArrayList<String>();

        for (String column : columnList) {
            if (column.endsWith("\"") && column.startsWith("\"")) {
                columns.add(column);
            } else {
                columns.add("\"" + column + "\"");
            }
        }

        return StringUtils.join(columns, ",");
    }

    public static PreparedStatement getSingleFieldDeleteStatement(
            Connection connection,
            String tableName,
            String[] pkNames,
            LogicalType[] pkTypes,
            List<RowData> rowDataList)
            throws SQLException {
        String finalSql =
                String.format(
                        "DELETE FROM %s where %s in (%s)",
                        innerQuoteIdentifier(tableName),
                        innerQuoteIdentifier(pkNames[0]),
                        rowDataList.stream().map(rowData -> "?").collect(Collectors.joining(",")));
        PreparedStatement st = connection.prepareStatement(finalSql);
        int currentIndex = 1;
        for (RowData rowdata : rowDataList) {
            setStatement(pkTypes[0], rowdata, 0, st, currentIndex++);
        }
        return st;
    }

    public static String innerQuoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String convert2DatabaseDataType(LogicalType flinkType) {
        String type;
        switch (flinkType.getTypeRoot()) {
            case BIGINT:
            case BOOLEAN:
            case DECIMAL:
            case TIME_WITHOUT_TIME_ZONE:
            case SMALLINT:
            case DATE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                type = flinkType.asSummaryString();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                type = "TIMESTAMPTZ";
                break;
            case DOUBLE:
                type = "DOUBLE PRECISION";
                break;
            // support mysql
            case INTEGER:
                type = "INTEGER";
                break;
            case TINYINT:
                type = "SMALLINT";
                break;
            case FLOAT:
                type = "REAL";
                break;
            case VARCHAR:
                VarCharType varCharType = (VarCharType) flinkType;
                if (varCharType.getLength() <= VACHAR_MAX_LENGTH) {
                    type = String.format("VARCHAR(%s)", varCharType.getLength());
                } else {
                    type = "TEXT";
                }
                break;
            case CHAR:
                CharType charType = (CharType) flinkType;
                if (charType.getLength() <= CHAR_MAX_LENGTH) {
                    type = String.format("CHAR(%s)", charType.getLength());
                } else {
                    type = "TEXT";
                }
                break;
            default:
                type = "VARCHAR";
        }
        return type;
    }

    @Override
    public boolean parseUnknownDatabase(SQLException e) {
        return "3D000".equals(e.getSQLState());
    }

    @Override
    public boolean parseUnkownTable(SQLException e) {
        return "42P01".equals(e.getSQLState());
    }

    @Override
    public boolean parseUnkownSchema(SQLException e) {
        return "3F000".equals(e.getSQLState());
    }

    @Override
    public boolean parseUnkownColumn(SQLException e) {
        return UNKNOWN_COLUMN_PATTERN.matcher(e.getMessage()).matches();
    }

    @Override
    public String extractUnkownColumn(SQLException e) {
        Matcher matcher = UNKNOWN_COLUMN_PATTERN.matcher(e.getMessage());
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    @Override
    public String getDefaultDatabase() {
        return "postgres";
    }

    @Override
    public boolean parseResourceExistsError(SQLException e) {
        return RESOURCE_EXISTS_ERROS.contains(e.getSQLState());
    }

    @Override
    public String escape(String origin) {
        return String.format("%s%s%s", Constants.DOUBLE_QUOTES, origin, Constants.DOUBLE_QUOTES);
    }

    @Override
    public boolean isResourceNotExists(SQLException e) {
        LOG.warn("Handle isResourceNotExists due to", e);
        return parseUnknownDatabase(e) || parseUnkownSchema(e) || parseUnkownTable(e) || parseUnkownColumn(e);
    }

    @Override
    public String buildCreateTableStatement(String tableIdentifier, List<String> primaryKeys, RowType rowType,
            String comment) {
        StringBuilder sb = new StringBuilder();
        StringBuilder comments = new StringBuilder();
        String table = formatTableIdentifier(tableIdentifier);
        sb.append("CREATE TABLE IF NOT EXISTS ").append(table).append(" (\n");
        Iterator<RowField> iterator = rowType.getFields().iterator();
        while (iterator.hasNext()) {
            RowField column = iterator.next();
            sb.append("\t").append(escape(column.getName())).append(" ")
                    .append(convert2DatabaseDataType(column.getType()));
            String columnComment = column.getDescription().orElse(Constants.ADD_COLUMN_COMMENT);
            comments.append("\nCOMMENT ON COLUMN ").append(table).append(".")
                    .append(escape(column.getName())).append(" IS ").append(quote(columnComment)).append(";");
            if (iterator.hasNext()) {
                sb.append(",\n");
            }
        }
        StringBuilder distributeKeys = new StringBuilder();
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            sb.append(",\n\t PRIMARY KEY (");
            for (String primaryKey : primaryKeys) {
                sb.append(escape(primaryKey)).append(",");
                distributeKeys.append(escape(primaryKey)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1).append(")");
            distributeKeys.deleteCharAt(distributeKeys.length() - 1);
        }
        sb.append("\n)");
        if (distributeKeys.length() > 0) {
            sb.append("\tDISTRIBUTE BY SHARD(").append(distributeKeys).append(")");
        }
        if (StringUtils.isNotBlank(comment)) {
            comments.append("\nCOMMENT ON TABLE ").append(table).append(" IS ").append(quote(comment)).append(";");
        }
        sb.append(";").append(comments);
        return sb.toString();
    }

    @Override
    public String buildAddColumnStatement(String tableIdentifier, List<RowField> addFields,
            Map<String, String> positionMap) {
        StringBuilder sb = new StringBuilder();
        String table = formatTableIdentifier(tableIdentifier);
        StringBuilder comments = new StringBuilder();
        for (RowField field : addFields) {
            String comment = field.getDescription().orElse(Constants.ADD_COLUMN_COMMENT);
            sb.append("ADD COLUMN IF NOT EXISTS ")
                    .append(escape(field.getName())).append(" ")
                    .append(convert2DatabaseDataType(field.getType()));
            sb.append(",");
            comments.append("\nCOMMENT ON COLUMN ").append(table).append(".")
                    .append(escape(field.getName())).append(" IS ").append(quote(comment)).append(";");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(";").append(comments);
        return sb.toString();
    }
}
