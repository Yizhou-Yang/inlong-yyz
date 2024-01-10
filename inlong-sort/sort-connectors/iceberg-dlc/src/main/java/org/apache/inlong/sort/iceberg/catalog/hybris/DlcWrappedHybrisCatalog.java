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

package org.apache.inlong.sort.iceberg.catalog.hybris;

import com.qcloud.dlc.common.Constants;
import com.qcloud.dlc.metastore.DLCClientFactory;
import com.qcloud.dlc.metastore.DLCDataCatalogMetastoreClient;
import com.tencentcloudapi.dlc.v20210125.DlcClient;
import com.tencentcloudapi.dlc.v20210125.models.AssignMangedTablePropertiesRequest;
import com.tencentcloudapi.dlc.v20210125.models.AssignMangedTablePropertiesResponse;
import com.tencentcloudapi.dlc.v20210125.models.Property;
import com.tencentcloudapi.dlc.v20210125.models.TableBaseInfo;
import com.tencentcloudapi.dlc.v20210125.models.TColumn;
import com.tencentcloudapi.dlc.v20210125.models.TPartition;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.auth.DlcCloudCredentialsProvider;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.inlong.sort.iceberg.utils.DLCUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The only changed point is HiveClientPool, from
 * {@link HiveMetaStoreClient} to {@link DLCDataCatalogMetastoreClient}
 */
public class DlcWrappedHybrisCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {

    public static final String LIST_ALL_TABLES = "list-all-tables";
    public static final String LIST_ALL_TABLES_DEFAULT = "false";
    private static final String DATASOURCE_CONNECTION_NAME = "DataLakeCatalog";
    private static final String TABLE_TYPE = "table";
    private static final String ICEBERG_FORMAT = "iceberg";
    private static final String PRIMARY_KEYS = "primary.keys";
    private static final String PRIMARY_SPLIT_QUOTA = ",";
    public static final Set<String> SUPPORTED_WAREHOUSE = new HashSet<String>() {

        {
            add("lakefs://");
            add("cosn://");
        }
    };
    public static final Set<String> DLC_WHITELIST_PARAMS = Stream.of(
            Constants.DLC_REGION_CONF,
            Constants.DLC_ENDPOINT,
            Constants.DLC_REGION_CONF,
            Constants.DLC_SECRET_ID_CONF,
            Constants.DLC_SECRET_KEY_CONF,
            DlcCloudCredentialsProvider.END_POINT,
            DlcCloudCredentialsProvider.SECRET_ID,
            DlcCloudCredentialsProvider.SECRET_KEY,
            DlcCloudCredentialsProvider.REGION,
            DlcCloudCredentialsProvider.USER_APPID,
            DlcCloudCredentialsProvider.REQUEST_IDENTITY_TOKEN,
            CosNConfigKeys.COSN_USERINFO_SECRET_ID_KEY,
            CosNConfigKeys.COSN_USERINFO_SECRET_KEY_KEY,
            CosNConfigKeys.COSN_REGION_PREV_KEY,
            CosNConfigKeys.COSN_CREDENTIALS_PROVIDER,
            "fs.lakefs.impl",
            "fs.cosn.impl",
            "fs.cosn.posix_bucket.fs.impl",
            DLCUtils.JWT_SECRET,
            DLCUtils.EXECUTOR_SECRET_ID,
            DLCUtils.EXECUTOR_SECRET_KEY,
            DLCUtils.GATEWAY_URL,
            DLCUtils.OWNER_UIN,
            DLCUtils.OPERATOR_UIN,
            Constants.DLC_CREDENTIAL_PROVIDER_CLASS_CONF).collect(Collectors.toSet());

    private static final Logger LOG = LoggerFactory.getLogger(DlcWrappedHybrisCatalog.class);

    private String name;
    private Configuration conf;
    private FileIO fileIO;
    private ClientPool<DLCDataCatalogMetastoreClient, TException> clients;
    private boolean listAllTables = false;
    private boolean isDlcManagedTable;

    public DlcWrappedHybrisCatalog() {
    }

    @Override
    public void initialize(String inputName, Map<String, String> properties) {
        this.name = inputName;
        if (conf == null) {
            LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
            this.conf = new Configuration();
        }

        // dlc auth
        properties.entrySet().stream()
                .filter(entry -> DLC_WHITELIST_PARAMS.contains(entry.getKey()))
                .forEach(entry -> this.conf.set(entry.getKey(), entry.getValue()));

        if (properties.containsKey(CatalogProperties.URI)) {
            this.conf.set(Constants.DLC_ENDPOINT, properties.get(CatalogProperties.URI));
            this.conf.set(DlcCloudCredentialsProvider.END_POINT, properties.get(CatalogProperties.URI));
        }

        if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
            String warehouse = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
            if (SUPPORTED_WAREHOUSE.stream().noneMatch(warehousePre -> warehouse.startsWith(warehousePre))) {
                throw new IllegalArgumentException(
                        "Illegal warehouse location, supportted location:" + SUPPORTED_WAREHOUSE);
            }
            this.conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
            isDlcManagedTable = false;
        } else {
            isDlcManagedTable = true;
        }

        this.listAllTables = Boolean.parseBoolean(properties.getOrDefault(LIST_ALL_TABLES, LIST_ALL_TABLES_DEFAULT));

        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        this.fileIO = fileIOImpl == null
                ? new HadoopFileIO(conf)
                : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

        this.clients = new CachedClientPool(conf, properties);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        Preconditions.checkArgument(isValidateNamespace(namespace),
                "Missing database in namespace: %s", namespace);
        String database = namespace.level(0);

        try {
            List<String> tableNames = clients.run(client -> client.getAllTables(database));
            List<TableIdentifier> tableIdentifiers;

            if (listAllTables) {
                tableIdentifiers = tableNames.stream()
                        .map(t -> TableIdentifier.of(namespace, t))
                        .collect(Collectors.toList());
            } else {
                List<Table> tableObjects = clients.run(client -> client.getTableObjectsByName(database, tableNames));
                tableIdentifiers = tableObjects.stream()
                        .filter(table -> table.getParameters() != null
                                && BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE
                                        .equalsIgnoreCase(
                                                table.getParameters()
                                                        .get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)))
                        .map(table -> TableIdentifier.of(namespace, table.getTableName()))
                        .collect(Collectors.toList());
            }

            LOG.debug("Listing of namespace: {} resulted in the following tables: {}", namespace, tableIdentifiers);
            return tableIdentifiers;

        } catch (UnknownDBException e) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);

        } catch (TException e) {
            throw new RuntimeException("Failed to list all tables under namespace " + namespace, e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to listTables", e);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
        try {
            this.loadTable(identifier);
            return true;
        } catch (NoSuchTableException var3) {
            return false;
        }
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        if (!isValidIdentifier(identifier)) {
            return false;
        }

        String database = identifier.namespace().level(0);

        TableOperations ops = newTableOps(identifier);
        TableMetadata lastMetadata;
        if (purge && ops.current() != null) {
            lastMetadata = ops.current();
        } else {
            lastMetadata = null;
        }

        try {
            clients.run(client -> {
                client.dropTable(database, identifier.name(),
                        false /* do not delete data */,
                        false /* throw NoSuchObjectException if the table doesn't exist */);
                return null;
            });

            if (purge && lastMetadata != null) {
                CatalogUtil.dropTableData(ops.io(), lastMetadata);
            }

            LOG.info("Dropped table: {}", identifier);
            return true;

        } catch (NoSuchTableException | NoSuchObjectException e) {
            LOG.info("Skipping drop, table does not exist: {}", identifier, e);
            return false;

        } catch (TException e) {
            throw new RuntimeException("Failed to drop " + identifier, e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to dropTable", e);
        }
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier originalTo) {
        if (!isValidIdentifier(from)) {
            throw new NoSuchTableException("Invalid identifier: %s", from);
        }

        TableIdentifier to = removeCatalogName(originalTo);
        Preconditions.checkArgument(isValidIdentifier(to), "Invalid identifier: %s", to);

        String toDatabase = to.namespace().level(0);
        String fromDatabase = from.namespace().level(0);
        String fromName = from.name();

        try {
            Table table = clients.run(client -> client.getTable(fromDatabase, fromName));
            HiveTableOperations.validateTableIsIceberg(table, fullTableName(name, from));

            table.setDbName(toDatabase);
            table.setTableName(to.name());

            clients.run(client -> {
                client.alter_table(fromDatabase, fromName, table);
                return null;
            });

            LOG.info("Renamed table from {}, to {}", from, to);

        } catch (NoSuchObjectException e) {
            throw new NoSuchTableException("Table does not exist: %s", from);

        } catch (AlreadyExistsException e) {
            throw new org.apache.iceberg.exceptions.AlreadyExistsException("Table already exists: %s", to);

        } catch (TException e) {
            throw new RuntimeException("Failed to rename " + from + " to " + to, e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to rename", e);
        }
    }

    @Override
    public org.apache.iceberg.Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
        Preconditions.checkArgument(isValidIdentifier(identifier), "Invalid identifier: %s", identifier);

        // Throw an exception if this table already exists in the catalog.
        if (tableExists(identifier)) {
            throw new org.apache.iceberg.exceptions.AlreadyExistsException("Table already exists: %s", identifier);
        }

        TableOperations ops = newTableOps(identifier);
        InputFile metadataFile = fileIO.newInputFile(metadataFileLocation);
        TableMetadata metadata = TableMetadataParser.read(ops.io(), metadataFile);
        ops.commit(null, metadata);

        return new BaseTable(ops, identifier.toString());
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        Preconditions.checkArgument(
                !namespace.isEmpty(),
                "Cannot create namespace with invalid name: %s", namespace);
        Preconditions.checkArgument(isValidateNamespace(namespace),
                "Cannot support multi part namespace in Hive Metastore: %s", namespace);

        try {
            clients.run(client -> {
                client.createDatabase(convertToDatabase(namespace, meta));
                return null;
            });

            LOG.info("Created namespace: {}", namespace);

        } catch (AlreadyExistsException e) {
            throw new org.apache.iceberg.exceptions.AlreadyExistsException(e, "Namespace '%s' already exists!",
                    namespace);

        } catch (TException e) {
            throw new RuntimeException("Failed to create namespace " + namespace + " in Hive Metastore", e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted in call to createDatabase(name) " + namespace + " in Hive Metastore", e);
        }
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        if (!isValidateNamespace(namespace) && !namespace.isEmpty()) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        if (!namespace.isEmpty()) {
            return ImmutableList.of();
        }
        try {
            List<Namespace> namespaces = clients.run(DLCDataCatalogMetastoreClient::getAllDatabases)
                    .stream()
                    .map(Namespace::of)
                    .collect(Collectors.toList());

            LOG.debug("Listing namespace {} returned tables: {}", namespace, namespaces);
            return namespaces;

        } catch (TException e) {
            throw new RuntimeException("Failed to list all namespace: " + namespace + " in Hive Metastore", e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted in call to getAllDatabases() " + namespace + " in Hive Metastore", e);
        }
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
        if (!isValidateNamespace(namespace)) {
            return false;
        }

        try {
            clients.run(client -> {
                client.dropDatabase(namespace.level(0),
                        false /* deleteData */,
                        false /* ignoreUnknownDb */,
                        false /* cascade */);
                return null;
            });

            LOG.info("Dropped namespace: {}", namespace);
            return true;

        } catch (InvalidOperationException e) {
            throw new NamespaceNotEmptyException(e, "Namespace %s is not empty. One or more tables exist.", namespace);

        } catch (NoSuchObjectException e) {
            return false;

        } catch (TException e) {
            throw new RuntimeException("Failed to drop namespace " + namespace + " in Hive Metastore", e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted in call to drop dropDatabase(name) " + namespace + " in Hive Metastore", e);
        }
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        Map<String, String> parameter = Maps.newHashMap();

        parameter.putAll(loadNamespaceMetadata(namespace));
        parameter.putAll(properties);
        Database database = convertToDatabase(namespace, parameter);

        alterHiveDataBase(namespace, database);
        LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);

        // Always successful, otherwise exception is thrown
        return true;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        Map<String, String> parameter = Maps.newHashMap();

        parameter.putAll(loadNamespaceMetadata(namespace));
        properties.forEach(key -> parameter.put(key, null));
        Database database = convertToDatabase(namespace, parameter);

        alterHiveDataBase(namespace, database);
        LOG.debug("Successfully removed properties {} from {}", properties, namespace);

        // Always successful, otherwise exception is thrown
        return true;
    }

    private void alterHiveDataBase(Namespace namespace, Database database) {
        try {
            clients.run(client -> {
                client.alterDatabase(namespace.level(0), database);
                return null;
            });

        } catch (NoSuchObjectException | UnknownDBException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);

        } catch (TException e) {
            throw new RuntimeException(
                    "Failed to list namespace under namespace: " + namespace + " in Hive Metastore", e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted in call to getDatabase(name) " + namespace + " in Hive Metastore", e);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        if (!isValidateNamespace(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        try {
            Database database = clients.run(client -> client.getDatabase(namespace.level(0)));
            Map<String, String> metadata = convertToMetadata(database);
            LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());
            return metadata;

        } catch (NoSuchObjectException | UnknownDBException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);

        } catch (TException e) {
            throw new RuntimeException(
                    "Failed to list namespace under namespace: " + namespace + " in Hive Metastore", e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted in call to getDatabase(name) " + namespace + " in Hive Metastore", e);
        }
    }

    @Override
    protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
        return tableIdentifier.namespace().levels().length == 1;
    }

    private TableIdentifier removeCatalogName(TableIdentifier to) {
        if (isValidIdentifier(to)) {
            return to;
        }

        // check if the identifier includes the catalog name and remove it
        if (to.namespace().levels().length == 2 && name().equalsIgnoreCase(to.namespace().level(0))) {
            return TableIdentifier.of(Namespace.of(to.namespace().level(1)), to.name());
        }

        // return the original unmodified
        return to;
    }

    private boolean isValidateNamespace(Namespace namespace) {
        return namespace.levels().length == 1;
    }

    @Override
    public TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new HiveTableOperations(conf, clients, fileIO, name, dbName, tableName);
    }

    // Because the database table logic of dlc does not follow the table creation logic of hive, it needs to be
    // modified here
    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        // This is a little edgy since we basically duplicate the HMS location generation logic.
        // Sadly I do not see a good way around this if we want to keep the order of events, like:
        // - Create meta files
        // - Create the metadata in HMS, and this way committing the changes

        // 'warehouse' parameters determine whether the table is managed table
        // Managed table:
        // - dlc client internally obtained warehouse address
        // Not managed table:
        // - stick to the {WAREHOUSE_DIR}/{DB_NAME}.db/{TABLE_NAME} path
        String warehouseLocation = getWarehouseLocation();
        if (StringUtils.isNotEmpty(warehouseLocation)
                && !ConfVars.METASTOREWAREHOUSE.defaultStrVal.equals(warehouseLocation)) {
            return String.format(
                    "%s/%s.db/%s",
                    warehouseLocation,
                    tableIdentifier.namespace().levels()[0],
                    tableIdentifier.name());
        }

        try {
            String warehouse = clients.run(
                    client -> client.getTableLocation(tableIdentifier.namespace().levels()[0], tableIdentifier.name()));

            return warehouse;

        } catch (TException e) {
            throw new RuntimeException(String.format("Metastore operation failed for %s", tableIdentifier), e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during commit", e);
        }
    }

    private String getWarehouseLocation() {
        return conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    }

    private Map<String, String> convertToMetadata(Database database) {

        Map<String, String> meta = Maps.newHashMap();

        meta.putAll(database.getParameters());
        if (StringUtils.isNotEmpty(database.getLocationUri())) {
            meta.put("location", database.getLocationUri());
        }
        if (database.getDescription() != null) {
            meta.put("comment", database.getDescription());
        }

        return meta;
    }

    Database convertToDatabase(Namespace namespace, Map<String, String> meta) {
        if (!isValidateNamespace(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        Database database = new Database();
        Map<String, String> parameter = Maps.newHashMap();

        database.setName(namespace.level(0));
        String location = getWarehouseLocation();
        if (StringUtils.isNotEmpty(location)) {
            database.setLocationUri(new Path(getWarehouseLocation(), namespace.level(0)).toString() + ".db");
        }
        meta.forEach((key, value) -> {
            if (key.equals("comment")) {
                database.setDescription(value);
            } else if (key.equals("location")) {
                database.setLocationUri(value);
            } else {
                if (value != null) {
                    parameter.put(key, value);
                }
            }
        });
        database.setParameters(parameter);

        return database;
    }

    public org.apache.iceberg.Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec,
            String location, Map<String, String> properties) {
        LOG.debug("DlcWrappedHybrisCatalog createTable: invoke dlc createTable api!");

        DLCClientFactory dlcClientFactory = new DLCClientFactory(this.conf);
        DlcClient client;
        try {
            client = dlcClientFactory.newClient();
        } catch (Exception e) {
            LOG.error("dlc client create error", e);
            return null;
        }

        AssignMangedTablePropertiesRequest assignMangedTablePropertiesRequest =
                new AssignMangedTablePropertiesRequest();
        String databaseName = "", tableName = "";
        if (identifier != null) {
            tableName = identifier.name();
            if (identifier.hasNamespace() && identifier.namespace().levels() != null
                    && identifier.namespace().levels().length > 0) {
                databaseName = identifier.namespace().levels()[0];
            } else {
                LOG.warn("createTable: no namespace or namespace has no levels!");
            }
            LOG.info("createTable databaseName:" + databaseName + ", tableName:" + tableName);

            TableBaseInfo tableBaseInfo = new TableBaseInfo();
            tableBaseInfo.setTableName(tableName);
            tableBaseInfo.setDatabaseName(databaseName);
            tableBaseInfo.setDatasourceConnectionName(DATASOURCE_CONNECTION_NAME);
            tableBaseInfo.setType(TABLE_TYPE);
            tableBaseInfo.setTableFormat(ICEBERG_FORMAT);
            assignMangedTablePropertiesRequest.setTableBaseInfo(tableBaseInfo);

            // generate create table column.
            List<Types.NestedField> columns = schema.columns();
            if (columns != null) {
                int columnSize = columns.size();
                TColumn[] tColumns = new TColumn[columnSize];
                for (int i = 0; i < columnSize; i++) {
                    Types.NestedField nestedField = columns.get(i);

                    TColumn tColumn = new TColumn();
                    tColumn.setName(nestedField.name());
                    tColumn.setType(nestedField.type().typeId().name());
                    tColumn.setComment(nestedField.doc());
                    tColumn.setNotNull(nestedField.isRequired());

                    tColumns[i] = tColumn;
                }
                assignMangedTablePropertiesRequest.setColumns(tColumns);
            } else {
                LOG.debug("no columns!");
            }

            // generate create table partition.
            if (spec != null && !PartitionSpec.unpartitioned().equals(spec)) {
                List<PartitionField> partitionFieldList = spec.fields();

                int partitionSize = partitionFieldList.size();
                TPartition[] tPartitions = new TPartition[partitionSize];
                for (int i = 0; i < partitionSize; i++) {
                    PartitionField partitionField = partitionFieldList.get(i);

                    TPartition tPartition = new TPartition();
                    tPartition.setName(partitionField.name());
                    int sourceId = partitionField.sourceId();
                    Types.NestedField nestedField = schema.columns().get(sourceId);
                    tPartition.setType(nestedField.type().typeId().name());
                    tPartition.setComment(nestedField.doc());
                    tPartition.setTransform(partitionField.transform().dedupName());

                    tPartitions[i] = tPartition;
                }

                assignMangedTablePropertiesRequest.setPartitions(tPartitions);
            } else {
                LOG.debug("no partitions!");
            }

            // generate create table properties.
            List<Property> propertyList = new ArrayList<>();
            String primaryKeys = "";
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (PRIMARY_KEYS.equalsIgnoreCase(entry.getKey())) {
                    primaryKeys = entry.getValue();
                } else {
                    Property property = new Property();

                    property.setKey(entry.getKey());
                    property.setValue(entry.getValue());
                    propertyList.add(property);
                }
            }
            Property[] propertiesForDlc = propertyList.toArray(new Property[0]);
            assignMangedTablePropertiesRequest.setProperties(propertiesForDlc);

            if (StringUtils.isNotBlank(primaryKeys)) {
                String[] upsertKeys = primaryKeys.split(PRIMARY_SPLIT_QUOTA);
                assignMangedTablePropertiesRequest.setUpsertKeys(upsertKeys);
            } else {
                LOG.debug("upsertKeys is blank!");
            }
            Map<String, String> propertiesNew = ImmutableMap.of();
            ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
            try {
                AssignMangedTablePropertiesResponse response =
                        client.AssignMangedTableProperties(assignMangedTablePropertiesRequest);
                Property[] propertiesRes = response.getProperties();

                LOG.info("Check dlc managed table flag:{}, decide apply lakefs config or not.", isDlcManagedTable);
                if (isDlcManagedTable) {
                    // lakefs
                    propertiesBuilder.put("lakehouse.storage.type", "lakefs");
                } else {
                    LOG.info("not dlc managed table, skip lakefs config!");
                }

                for (Property property : propertiesRes) {
                    String key = property.getKey();
                    String value = property.getValue();
                    LOG.debug("key:" + key + ", " + value);

                    propertiesBuilder.put(key, value);
                }

                propertiesNew = propertiesBuilder.build();
            } catch (Exception e) {
                LOG.error("request error", e);
            }

            if (propertiesNew.size() == 0) {
                LOG.debug("request super createTable for properties");
                if (!this.tableExists(identifier)) {
                    return super.createTable(identifier, schema, spec, null, properties);
                } else {
                    LOG.info("Table({}) in Database({}) is exists when createTable, ignore!", tableName, databaseName);
                }
            } else {
                LOG.debug("request super createTable for propertiesNew");
                if (!this.tableExists(identifier)) {
                    return super.createTable(identifier, schema, spec, null, propertiesNew);
                } else {
                    LOG.info("Table({}) in Database({}) is exists when createTable, ignore!", tableName, databaseName);
                }
            }
        } else {
            LOG.warn("createTable: identifier is null!");
        }
        return null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("uri", this.conf == null ? "" : this.conf.get(HiveConf.ConfVars.METASTOREURIS.varname, ""))
                .toString();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @VisibleForTesting
    void setListAllTables(boolean listAllTables) {
        this.listAllTables = listAllTables;
    }
}
