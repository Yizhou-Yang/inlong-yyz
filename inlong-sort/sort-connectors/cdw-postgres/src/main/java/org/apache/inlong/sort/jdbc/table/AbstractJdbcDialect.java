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

package org.apache.inlong.sort.jdbc.table;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.jdbc.internal.JdbcMultiBatchingComm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Default JDBC dialects implements for validate.
 */
public abstract class AbstractJdbcDialect implements JdbcDialect {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcDialect.class);
    public static final String PRIMARY_KEY_COLUMN = "pkColumn";

    @Override
    public void validate(TableSchema schema) throws ValidationException {
        for (int i = 0; i < schema.getFieldCount(); i++) {
            DataType dt = schema.getFieldDataType(i).get();
            String fieldName = schema.getFieldName(i).get();

            // TODO: We can't convert VARBINARY(n) data type to
            // PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
            // LegacyTypeInfoDataTypeConverter
            // when n is smaller than Integer.MAX_VALUE
            if (unsupportedTypes().contains(dt.getLogicalType().getTypeRoot())
                    || (dt.getLogicalType() instanceof VarBinaryType
                            && Integer.MAX_VALUE != ((VarBinaryType) dt.getLogicalType()).getLength())) {
                throw new ValidationException(
                        String.format(
                                "The %s dialect doesn't support type: %s.",
                                dialectName(), dt.toString()));
            }

            // only validate precision of DECIMAL type for blink planner
            if (dt.getLogicalType() instanceof DecimalType) {
                int precision = ((DecimalType) dt.getLogicalType()).getPrecision();
                if (precision > maxDecimalPrecision() || precision < minDecimalPrecision()) {
                    throw new ValidationException(
                            String.format(
                                    "The precision of field '%s' is out of the DECIMAL "
                                            + "precision range [%d, %d] supported by %s dialect.",
                                    fieldName,
                                    minDecimalPrecision(),
                                    maxDecimalPrecision(),
                                    dialectName()));
                }
            }

            // only validate precision of DECIMAL type for blink planner
            if (dt.getLogicalType() instanceof TimestampType) {
                int precision = ((TimestampType) dt.getLogicalType()).getPrecision();
                if (precision > maxTimestampPrecision() || precision < minTimestampPrecision()) {
                    throw new ValidationException(
                            String.format(
                                    "The precision of field '%s' is out of the TIMESTAMP "
                                            + "precision range [%d, %d] supported by %s dialect.",
                                    fieldName,
                                    minTimestampPrecision(),
                                    maxTimestampPrecision(),
                                    dialectName()));
                }
            }
        }
    }

    public abstract int maxDecimalPrecision();

    public abstract int minDecimalPrecision();

    public abstract int maxTimestampPrecision();

    public abstract int minTimestampPrecision();

    /**
     * Defines the unsupported types for the dialect.
     *
     * @return a list of logical type roots.
     */
    public abstract List<LogicalTypeRoot> unsupportedTypes();

    public abstract PreparedStatement setQueryPrimaryKeySql(Connection conn,
            String tableIdentifier) throws SQLException;

    /**
     * get getPkNames from query db.tb
     *
     * @return a list of PkNames.
     */
    public List<String> getPkNames(String tableIdentifier,
            JdbcOptions jdbcOptions) throws SQLException, ClassNotFoundException {
        PreparedStatement st = null;
        JdbcOptions jdbcExecOptions = JdbcMultiBatchingComm.getExecJdbcOptions(jdbcOptions, tableIdentifier);
        SimpleJdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(jdbcExecOptions);
        List<String> pkNames = null;
        try {
            Connection conn = connectionProvider.getOrEstablishConnection();
            st = setQueryPrimaryKeySql(conn, tableIdentifier);
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                pkNames = Arrays.asList(rs.getString(PRIMARY_KEY_COLUMN).split(","));
            }
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException ex) {
                    LOG.warn("Close statement failed", ex);
                }
            }
            connectionProvider.closeConnection();
        }
        LOG.info("Get pkNames: {}", pkNames);
        return pkNames;
    }

    public List<String> getPkNames(String tableIdentifier, JdbcOptions jdbcOptions, int retryTimes) throws Exception {
        for (int i = 0; i < retryTimes; i++) {
            try {
                return getPkNames(tableIdentifier, jdbcOptions);
            } catch (ClassNotFoundException e) {
                throw e;
            } catch (Exception e) {
                if (i + 1 < retryTimes) {
                    Thread.sleep(1000);
                } else {
                    throw e;
                }
            }
        }
        return null;
    }

    public abstract boolean parseUnknownDatabase(SQLException e);

    public abstract boolean parseUnkownTable(SQLException e);

    public abstract boolean parseUnkownSchema(SQLException e);

    public abstract boolean parseUnkownColumn(SQLException e);

    public abstract String extractUnkownColumn(SQLException e);

    public String buildCreateDatabaseStatement(String database) {
        return "CREATE DATABASE " + escape(database);
    }

    public String buildCreateTableStatement(String tableIdentifier, List<String> primaryKeys, RowType rowType,
            String comment) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(formatTableIdentifier(tableIdentifier)).append(" (\n");
        Iterator<RowField> iterator = rowType.getFields().iterator();
        while (iterator.hasNext()) {
            RowField column = iterator.next();
            sb.append("\t").append(escape(column.getName())).append(" ")
                    .append(convert2DatabaseDataType(column.getType()));
            String columnComment = column.getDescription().orElse(Constants.ADD_COLUMN_COMMENT);
            sb.append(" COMMENT ").append(quote(columnComment));
            if (iterator.hasNext()) {
                sb.append(",\n");
            }
        }
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            sb.append(",\n\t PRIMARY KEY (");
            for (String primaryKey : primaryKeys) {
                sb.append(escape(primaryKey)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
        }
        sb.append("\n)");
        if (StringUtils.isNotBlank(comment)) {
            sb.append(" COMMENT ").append(quote(comment));
        }
        return sb.toString();
    }

    public String buildCreateSchemaStatement(String schema) {
        return "CREATE SCHEMA IF NOT EXISTS " + escape(schema);
    }

    public abstract String getDefaultDatabase();

    public String buildAddColumnStatement(String tableIdentifier, List<RowField> addFields,
            Map<String, String> positionMap) {
        StringBuilder sb = new StringBuilder();
        for (RowField field : addFields) {
            String comment = field.getDescription().orElse(Constants.ADD_COLUMN_COMMENT);
            sb.append("ADD COLUMN IF NOT EXISTS ")
                    .append(escape(field.getName())).append(" ")
                    .append(convert2DatabaseDataType(field.getType()))
                    .append(" COMMENT ").append(quote(comment));
            if (positionMap.containsKey(field.getName())) {
                String afterField = positionMap.get(field.getName());
                if (afterField == null) {
                    sb.append(" FIRST");
                } else {
                    sb.append(" AFTER ").append(escape(afterField));
                }
            }
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public abstract String convert2DatabaseDataType(LogicalType flinkType);

    public String buildAddColumnStatement(String tableIdentifier, List<RowField> addFields) {
        return buildAddColumnStatement(tableIdentifier, addFields, new HashMap<>());
    }

    public String buildAlterTableStatement(String tableIdentifier) {
        return "ALTER TABLE " + formatTableIdentifier(tableIdentifier);
    }

    public abstract boolean parseResourceExistsError(SQLException e);

    public String formatTableIdentifier(String tableIdentifier) {
        StringBuilder sb = new StringBuilder();
        for (String item : tableIdentifier.split("\\.")) {
            sb.append(escape(item)).append(".");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public String escape(String origin) {
        return String.format("`%s`", origin);
    }

    public String quote(String value) {
        if (value == null) {
            return "'null'";
        }
        if (!value.startsWith(Constants.APOSTROPHE) && !value.startsWith(Constants.DOUBLE_QUOTES)) {
            return String.format("'%s'", value);
        }
        return value;
    }
}
