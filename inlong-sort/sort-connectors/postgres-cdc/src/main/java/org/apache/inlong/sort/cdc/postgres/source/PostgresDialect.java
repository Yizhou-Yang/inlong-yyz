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

package org.apache.inlong.sort.cdc.postgres.source;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.inlong.sort.cdc.base.config.JdbcSourceConfig;
import org.apache.inlong.sort.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.inlong.sort.cdc.base.relational.connection.JdbcConnectionFactory;
import org.apache.inlong.sort.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.inlong.sort.cdc.base.source.assigner.splitter.ChunkSplitter;
import org.apache.inlong.sort.cdc.base.source.meta.offset.Offset;
import org.apache.inlong.sort.cdc.base.source.meta.split.SourceSplitBase;
import org.apache.inlong.sort.cdc.base.source.reader.external.FetchTask;
import org.apache.inlong.sort.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.inlong.sort.cdc.postgres.source.config.PostgresSourceConfig;
import org.apache.inlong.sort.cdc.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.inlong.sort.cdc.postgres.source.fetch.PostgresScanFetchTask;
import org.apache.inlong.sort.cdc.postgres.source.fetch.PostgresSourceFetchTaskContext;
import org.apache.inlong.sort.cdc.postgres.source.fetch.PostgresStreamFetchTask;
import org.apache.inlong.sort.cdc.postgres.source.utils.PgSchema;
import org.apache.inlong.sort.cdc.postgres.source.utils.TableDiscoveryUtils;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.postgresql.PostgresObjectFactory.newPostgresValueConverterBuilder;
import static io.debezium.connector.postgresql.Utils.currentOffset;

/** The dialect for Postgres. */
public class PostgresDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final PostgresSourceConfig sourceConfig;

    private transient PgSchema schema;

    @Nullable
    private PostgresStreamFetchTask streamFetchTask;

    @Nullable
    private transient JdbcSourceConfig taskSourceConfig;

    @Nullable
    private transient JdbcSourceFetchTaskContext fetchTaskContext;

    public PostgresDialect(PostgresSourceConfigFactory configFactory) {
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        PostgresConnectorConfig dbzConfig = postgresSourceConfig.getDbzConnectorConfig();

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        PostgresConnection jdbc =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(),
                        valueConverterBuilder,
                        new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()));

        try {
            jdbc.connect();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        return jdbc;
    }

    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return currentOffset((PostgresConnection) jdbc);

        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // from Postgres docs:
        //
        // SQL is case insensitive about key words and identifiers,
        // except when identifiers are double-quoted to preserve the case
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new PostgresChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    // there is always a single database provided
                    // A database is the highest level of organizational structure in the PostgreSQL database management
                    // system.
                    // A database contains one or more schemas.
                    // Data within a database is isolated, and tables, views, and other objects from different databases
                    // cannot directly access each other.
                    // When you connect to PostgreSQL, you need to specify the database you want to connect to. Queries
                    // running in one database cannot directly access data in another database.
                    // Databases are created and dropped using the CREATE DATABASE and DROP DATABASE commands.
                    sourceConfig.getDatabaseList().get(0),
                    jdbc,
                    ((PostgresSourceConfig) sourceConfig).getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            // fetch table schemas
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChange tableSchema = queryTableSchema(jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new PostgresConnectionPoolFactory();
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (schema == null) {
            schema = new PgSchema((PostgresConnection) jdbc, sourceConfig);
        }
        return schema.getTableSchema(tableId);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new PostgresScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = new PostgresStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    @Override
    public JdbcSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
        return new PostgresSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public FetchTask.Context createFetchTaskContext(SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig,
            boolean reuse) {
        if (!reuse) {
            return createFetchTaskContext(sourceSplitBase, taskSourceConfig);
        }
        if (this.taskSourceConfig == null || this.fetchTaskContext == null
                || !this.taskSourceConfig.equals(taskSourceConfig)) {
            this.taskSourceConfig = taskSourceConfig;
            this.fetchTaskContext = new PostgresSourceFetchTaskContext(taskSourceConfig, this);
        }
        return this.fetchTaskContext;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (streamFetchTask != null) {
            streamFetchTask.commitCurrentOffset();
        }
    }
}
