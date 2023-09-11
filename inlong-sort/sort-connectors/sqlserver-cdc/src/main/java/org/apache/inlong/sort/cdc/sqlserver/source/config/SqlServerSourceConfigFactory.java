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

package org.apache.inlong.sort.cdc.sqlserver.source.config;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfigFactory;
import com.ververica.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnector;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory for creating {@link SqlServerSourceConfig}. */
public class SqlServerSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final String DATABASE_SERVER_NAME = "sqlserver_transaction_log_source";
    private static final String DRIVER_ClASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private String inlongMetric;
    private String inlongAudit;
    private Duration heartbeatInterval;
    private boolean migrateAll;

    public JdbcSourceConfigFactory inlongAudit(String inlongAudit) {
        this.inlongAudit = inlongAudit;
        return this;
    }

    public JdbcSourceConfigFactory inlongMetric(String inlongMetric) {
        this.inlongMetric = inlongMetric;
        return this;
    }

    public JdbcSourceConfigFactory heartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public JdbcSourceConfigFactory migrateAll(boolean migrateAll) {
        this.migrateAll = migrateAll;
        return this;
    }

    @Override
    public SqlServerSourceConfig create(int subtask) {
        Properties props = new Properties();
        props.setProperty("connector.class", SqlServerConnector.class.getCanonicalName());

        // set database history impl to flink database history
        props.setProperty(
                "database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);

        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the SQL Server database
        // server that you want Debezium to capture. The logical name should be unique across
        // all other connectors, since it is used as a prefix for all Kafka topic names
        // emanating from this connector. Only alphanumeric characters and underscores should be
        // used.
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        props.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatInterval.toMillis()));

        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }

        switch (startupOptions.startupMode) {
            case INITIAL:
                props.setProperty("snapshot.mode", "initial");
                break;
            case LATEST_OFFSET:
                props.setProperty("snapshot.mode", "schema_only");
                break;
            default:
                throw new UnsupportedOperationException();
        }

        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        Configuration dbzConfiguration = Configuration.from(props);
        return new SqlServerSourceConfig(
                startupOptions,
                databaseList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                props,
                dbzConfiguration,
                DRIVER_ClASS_NAME,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                inlongMetric,
                inlongAudit,
                migrateAll);
    }
}
