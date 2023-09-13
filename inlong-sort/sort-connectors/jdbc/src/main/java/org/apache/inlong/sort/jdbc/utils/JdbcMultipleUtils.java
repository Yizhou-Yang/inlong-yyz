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

package org.apache.inlong.sort.jdbc.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.inlong.sort.jdbc.table.AbstractJdbcDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Jdbc Utils
 */
public final class JdbcMultipleUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMultipleUtils.class);

    private static final int ZERO = 0;

    private static final int ONE = 1;

    private static final int TWO = 2;

    private static final int THREE = 3;

    private static final String DEFAULT_DATABASE = "DEFAULT";

    public static List<String> getPkNamesFromDatabase(JdbcOptions jdbcOptions, String tableIdentifier) {
        try {
            AbstractJdbcDialect jdbcDialect = (AbstractJdbcDialect) jdbcOptions.getDialect();
            return jdbcDialect.getPkNames(tableIdentifier, jdbcOptions);
        } catch (Exception e) {
            LOGGER.warn("Get pkNames from database for: {} failed", tableIdentifier, e);
        }
        return null;
    }

    /**
     * Get table From tableIdentifier
     * tableIdentifier maybe: ${dbName}.${tbName} or ${dbName}.${schemaName}.${tbName} or ${tableName}
     *
     * @param tableIdentifier The table identifier for which to get table name.
     */
    public static String getTableFromIdentifier(String tableIdentifier) {
        String[] fields = tableIdentifier.split("\\.");
        if (TWO == fields.length) {
            return fields[ONE];
        }
        if (THREE == fields.length) {
            return fields[ONE] + "." + fields[TWO];
        }
        return tableIdentifier;
    }

    /**
     * Get schema from tableIdentifier, this is only used to cache the {@link JdbcConnectionProvider} with database
     * tableIdentifier maybe: ${dbName}.${tbName} or ${dbName}.${schemaName}.${tbName} or ${tableName}
     *
     * @param tableIdentifier The table identifier for which to get table name.
     */
    public static String getSchemaFromIdentifier(String tableIdentifier) {
        String[] fields = tableIdentifier.split("\\.");
        if (THREE == fields.length) {
            return fields[ONE];
        }
        return null;
    }

    /**
     * Get database from tableIdentifier, this is only used to cache the {@link JdbcConnectionProvider} with database
     * tableIdentifier maybe: ${dbName}.${tbName} or ${dbName}.${schemaName}.${tbName} or ${tableName}
     *
     * @param tableIdentifier The table identifier for which to get table name.
     */
    public static String getDatabaseFromIdentifier(String tableIdentifier, String defaultDatabase) {
        String[] fields = tableIdentifier.split("\\.");
        if (fields.length > ONE) {
            return fields[ZERO];
        }
        if (defaultDatabase == null) {
            return DEFAULT_DATABASE;
        }
        return defaultDatabase;
    }

    /**
     * Get database from tableIdentifier, this is only used to cache the {@link JdbcConnectionProvider} with database
     * tableIdentifier maybe: ${dbName}.${tbName} or ${dbName}.${schemaName}.${tbName} or ${tableName}
     *
     * @param tableIdentifier The table identifier for which to get table name.
     */
    public static String getDatabaseFromIdentifier(String tableIdentifier) {
        String[] fields = tableIdentifier.split("\\.");
        if (fields.length > ONE) {
            return fields[ZERO];
        }
        return DEFAULT_DATABASE;
    }

    public static String buildTableIdentifier(String database, String schema, String table) {
        if (StringUtils.isBlank(schema)) {
            return String.format("%s.%s", database, table);
        } else {
            return String.format("%s.%s.%s", database, schema, table);
        }
    }

    /**
     * Get execution jdbc options
     * @param jdbcOptions The jdbc options {@link JdbcOptions}
     * @param database The database
     * @param table The table name
     * @return The execution jdbc options
     */
    public static JdbcOptions getExecJdbcOptions(JdbcOptions jdbcOptions, String database, String table) {
        return JdbcOptions.builder()
                .setDBUrl(jdbcOptions.getDbURL() + "/" + database)
                .setTableName(table)
                .setDialect(jdbcOptions.getDialect())
                .setParallelism(jdbcOptions.getParallelism())
                .setConnectionCheckTimeoutSeconds(jdbcOptions.getConnectionCheckTimeoutSeconds())
                .setDriverName(jdbcOptions.getDriverName())
                .setUsername(jdbcOptions.getUsername().orElse(""))
                .setPassword(jdbcOptions.getPassword().orElse(""))
                .build();
    }

    private JdbcMultipleUtils() {
    }
}
