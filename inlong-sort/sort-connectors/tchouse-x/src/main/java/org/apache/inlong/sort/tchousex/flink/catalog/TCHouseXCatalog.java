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

package org.apache.inlong.sort.tchousex.flink.catalog;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.apache.inlong.sort.schema.TableChange;
import org.apache.inlong.sort.tchousex.flink.connection.JdbcClient;
import org.apache.inlong.sort.tchousex.flink.util.FlinkSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TCHouseXCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(TCHouseXCatalog.class);

    private JdbcClient jdbcClient;

    public TCHouseXCatalog(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     * @throws CatalogException in case of any runtime exception
     */
    public List<String> listDatabases() throws CatalogException {
        List<String> databaseList = new ArrayList<>();
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();
            ResultSet set = statement.executeQuery("show databases");
            while (set.next()) {
                databaseList.add(set.getString(1));
            }
        } catch (Exception e) {
            LOG.error("show databases exception", e);
            throw new CatalogException("show databases exception");
        }
        return databaseList;
    }

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    public boolean databaseExists(String databaseName) throws CatalogException {
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();
            statement.execute("use " + databaseName);
        } catch (Exception e) {
            LOG.error("databaseExists exception: ", e);
            return false;
        }
        return true;
    }

    /**
     * Create a database.
     *
     * @param name Name of the database to be created
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *         exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *         nothing.
     * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists
     *         is false
     * @throws CatalogException in case of any runtime exception
     */
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();
            statement.execute("create database " + name);
            statement.close();
        } catch (Exception e) {
            LOG.error("createDatabase exception: ", e);
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(name, name);
            }
        }
    }

    /**
     * Drop a database.
     *
     * @param name Name of the database to be dropped.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *         false, throw an exception, if set to true, do nothing.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    public void dropDatabase(String name, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();
            statement.execute("drop database " + name);
        } catch (Exception e) {
            LOG.error("dropDatabase exception: ", e);
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(name, name);
            }
        }
    }

    /**
     * Get names of all tables and views under this database. An empty list is returned if none
     * exists.
     *
     * @param databaseName
     * @return a list of the names of all tables and views in this database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(databaseName, databaseName);
        }
        List<String> tableList = new ArrayList<>();
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();
            statement.execute("use " + databaseName);
            ResultSet set = statement.executeQuery("show tables");
            while (set.next()) {
                tableList.add(set.getString(1));
            }
        } catch (Exception e) {
            LOG.error("listTables exception", e);
            throw new DatabaseNotExistException(databaseName, databaseName);
        }
        return tableList;
    }

    /**
     * Returns a {@link Table}.
     *
     * @param tableIdentifier the table or view
     * @return The requested table or view
     * @throws CatalogException in case of any runtime exception
     */
    public Table getTable(TableIdentifier tableIdentifier) throws CatalogException {
        if (!tableExists(tableIdentifier)) {
            throw new CatalogException(tableIdentifier.getFullName() + " not exist");
        }
        Table table = new Table();
        table.setTableIdentifier(tableIdentifier);
        String fullName = tableIdentifier.getFullName();
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();
            String sql = "DESCRIBE FORMATTED " + fullName;
            List<Field> normalColList = new ArrayList();
            List<Field> partitionColList = new ArrayList();
            ResultSet resultSet = statement.executeQuery(sql);
            boolean partitionColStart = false;
            String name;
            while (resultSet.next()) {
                name = resultSet.getString("name");
                if (name.startsWith("#")) {
                    if ("# Partition Information".equals(name)) {
                        partitionColStart = true;
                    } else if ("# Detailed Table Information".equals(name)) {
                        break;
                    }
                } else if (!name.isEmpty()) {
                    Field column = new Field();
                    column.setName(name);
                    column.setType(resultSet.getString("type"));
                    column.setComment(resultSet.getString("comment"));
                    if (partitionColStart) {
                        partitionColList.add(column);
                    } else {
                        normalColList.add(column);
                    }
                }
            }
            table.setNormalFields(normalColList);
            table.setPartitionFields(partitionColList);
        } catch (Exception e) {
            LOG.warn("getTable exception", e);
            throw new CatalogException(fullName + " not exist", e);
        }
        return table;
    }

    /**
     * Check if a table or view exists in this catalog.
     *
     * @param tableIdentifier the table or view
     * @return true if the given table exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    public boolean tableExists(TableIdentifier tableIdentifier) throws CatalogException {
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();
            statement.execute("desc " + tableIdentifier.getFullName());
        } catch (Exception e) {
            LOG.warn("tableExists exception: ", e);
            return false;
        }
        return true;
    }

    /**
     * Drop a table or view.
     *
     * @param tableIdentifier table or view to be dropped
     * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist: if
     *         set to false, throw an exception, if set to true, do nothing.
     * @throws CatalogException in case of any runtime exception
     */
    public void dropTable(TableIdentifier tableIdentifier, boolean ignoreIfNotExists) throws CatalogException {
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();
            statement.execute("drop table " + tableIdentifier.getFullName());
        } catch (Exception e) {
            LOG.warn("dropTable exception: ", e);
            if (!ignoreIfNotExists) {
                throw new CatalogException(tableIdentifier.getFullName() + " not exists", e);
            }
        }
    }

    /**
     * Creates a new table or view.
     * only support no partition table
     *
     * @param table the table definition
     * @param ignoreIfExists flag to specify behavior when a table or view already exists at the
     *         given path: if set to false, it throws a TableAlreadyExistException, if set to true, do
     *         nothing.
     * @throws CatalogException in case of any runtime exception
     */
    public void createTable(Table table, boolean ignoreIfExists)
            throws CatalogException {
        TableIdentifier tableIdentifier = table.getTableIdentifier();
        if (!databaseExists(tableIdentifier.getDatabase())) {
            try {
                createDatabase(tableIdentifier.getDatabase(), true);
            } catch (DatabaseAlreadyExistException e) {
                LOG.warn("", e);
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.warn("", e);
            }
        }
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();

            StringBuilder createTableSql = new StringBuilder("create table ");
            createTableSql.append(tableIdentifier.getFullName());
            createTableSql.append(" ( ");
            for (Field field : table.getNormalFields()) {
                createTableSql.append(field.getName()).append(" ").append(field.getType()).append(" ,");
            }
            createTableSql.deleteCharAt(createTableSql.lastIndexOf(","));
            createTableSql.append(" )");
            LOG.info("create table sql:{}", createTableSql);
            statement.execute(createTableSql.toString());
        } catch (Exception e) {
            LOG.warn("createTable exception: ", e);
            if (!ignoreIfExists) {
                throw new CatalogException("createTable exception", e);
            }
        }
    }

    public void applySchemaChanges(TableIdentifier tableIdentifier, List<TableChange> tableChanges) {
        try {
            for (TableChange change : tableChanges) {
                if (change instanceof TableChange.AddColumn) {
                    apply(tableIdentifier, (TableChange.AddColumn) change);
                } else {
                    throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
                }
            }
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                // try catch exception for replay ddl binlog
                LOG.warn("ddl exec exception", e);
            } else {
                throw e;
            }
        }
    }

    public void applySchemaChanges(TableIdentifier tableIdentifier, Collection<Field> fields,
            SchemaChangeType schemaChangeType) {
        try {
            if (schemaChangeType == SchemaChangeType.ADD_COLUMN) {
                for (Field field : fields) {
                    apply(tableIdentifier, field);
                }
            } else {
                throw new UnsupportedOperationException("Cannot apply unknown table change: " + schemaChangeType);
            }
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                // try catch exception for replay ddl binlog
                LOG.warn("ddl exec exception", e);
            } else {
                throw e;
            }
        }
    }

    public void apply(TableIdentifier tableIdentifier, TableChange.AddColumn add) {
        String type = FlinkSchemaUtil.convertTypeFromLogicalToDataType(add.dataType());
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();

            StringBuilder addColumnSql = new StringBuilder("alter table ");
            addColumnSql.append(tableIdentifier.getFullName());
            addColumnSql.append(" add column ");
            addColumnSql.append(add.fieldNames()[add.fieldNames().length - 1]);
            addColumnSql.append(" ");
            addColumnSql.append(type);
            LOG.info("add column sql:{}", addColumnSql);
            statement.execute(addColumnSql.toString());
        } catch (Exception e) {
            LOG.warn("add column exception: ", e);
            throw new CatalogException("add column exception", e);
        }
    }

    public void apply(TableIdentifier tableIdentifier, Field field) {
        try (Connection connection = jdbcClient.getClient()) {
            Statement statement = connection.createStatement();

            StringBuilder addColumnSql = new StringBuilder("alter table ");
            addColumnSql.append(tableIdentifier.getFullName());
            addColumnSql.append(" add column ");
            addColumnSql.append(field.getName());
            addColumnSql.append(" ");
            addColumnSql.append(field.getType());
            LOG.info("add column sql:{}", addColumnSql);
            statement.execute(addColumnSql.toString());
        } catch (Exception e) {
            LOG.warn("add column exception: ", e);
            throw new CatalogException("add column exception", e);
        }
    }
}
