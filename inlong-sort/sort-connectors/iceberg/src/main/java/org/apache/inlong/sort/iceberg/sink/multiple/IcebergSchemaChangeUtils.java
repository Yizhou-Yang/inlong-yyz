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

package org.apache.inlong.sort.iceberg.sink.multiple;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.inlong.sort.schema.TableChange;
import org.apache.inlong.sort.iceberg.FlinkTypeToType;
import org.apache.inlong.sort.util.SchemaChangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class IcebergSchemaChangeUtils extends SchemaChangeUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSchemaChangeUtils.class);

    private static final Joiner DOT = Joiner.on(".");

    public static void createTable(Catalog catalog, TableIdentifier tableId, SupportsNamespaces asNamespaceCatalog,
            Schema schema, List<String> primaryKeyList, boolean upsertMode, String sinkPartitionRules) {
        if (!catalog.tableExists(tableId)) {
            if (asNamespaceCatalog != null && !asNamespaceCatalog.namespaceExists(tableId.namespace())) {
                try {
                    asNamespaceCatalog.createNamespace(tableId.namespace());
                    LOGGER.info("Auto create Database({}) in Catalog({}).", tableId.namespace(), catalog.name());
                } catch (AlreadyExistsException e) {
                    LOGGER.warn("Database({}) already exist in Catalog({})!", tableId.namespace(), catalog.name());
                }
            }
            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            properties.put("format-version", "2");
            if (upsertMode) {
                properties.put("write.upsert.enabled", "true");

                for (String primaryKey : primaryKeyList) {
                    properties.put("write.parquet.bloom-filter-enabled.column." + primaryKey, "true");
                }
            } else {
                LOGGER.debug("createTable upsertMode: false, no primaryKey!");
            }
            LOGGER.debug("createTable upsertMode:" + upsertMode + ", primaryKey:" + String.join(",", primaryKeyList));

            properties.put("write.metadata.metrics.default", "full");
            // for hive visible
            properties.put("engine.hive.enabled", "true");
            // fake properties for primary key
            properties.put("primary.keys", String.join(",", primaryKeyList));
            try {
                Table table = null;
                String tableName = tableId.name();
                List<Map<String, String>> list = parseSinkPartitionRules(tableName, sinkPartitionRules);
                LOGGER.info("list size:{}", list.size());
                boolean isCreate = false;
                for (Map<String, String> map : list) {
                    String partitionColumn = map.get("PartitionColumns");
                    String partitionPolicy = map.get("PartitionPolicies");

                    String[] partitionColumnArr = partitionColumn.split(",");
                    String[] partitionPolicyArr = partitionPolicy.split(",");

                    LOGGER.info("createTable partitionColumn:{} partitionPolicy:{}", partitionColumn, partitionPolicy);
                    int partitionColumnLen = partitionColumnArr.length, partitionPolicyLen = partitionPolicyArr.length;
                    if (partitionColumnLen == partitionPolicyLen) {
                        PartitionSpec partitionSpec =
                                parsePartitionPolicy(schema, partitionColumnArr, partitionPolicyArr);
                        if (partitionSpec != null) {
                            table = catalog.createTable(tableId, schema, partitionSpec, null, properties.build());
                            isCreate = true;
                        } else {
                            LOGGER.warn("partitionSpec is null, skip partition!");
                        }
                    } else {
                        LOGGER.warn("PartitionColumn size not equal to PartitionMethod, skip partition!");
                    }
                }

                if (!isCreate) {
                    table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), null,
                            properties.build());
                } else {
                    LOGGER.debug("Already create table!");
                }

                if (table == null) {
                    LOGGER.warn(
                            "Auto create Table({}) in Database({}) in Catalog({}) failed, maybe something wrong or table exists!",
                            tableId.name(), tableId.namespace(), catalog.name());
                } else {
                    LOGGER.info("Auto create Table({}) in Database({}) in Catalog({}) success!",
                            tableId.name(), tableId.namespace(), catalog.name());
                }
            } catch (AlreadyExistsException e) {
                LOGGER.warn("Table({}) already exist in Database({}) in Catalog({})!",
                        tableId.name(), tableId.namespace(), catalog.name());
            }
        }
    }

    private static List<Map<String, String>> parseSinkPartitionRules(String tableName, String sinkPartitionRules) {
        List<Map<String, String>> list = new ArrayList<>();

        LOGGER.info("tableName:{} sinkPartitionRules:{}", tableName, sinkPartitionRules);
        if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(sinkPartitionRules)) {
            String[] partitionNameRuleArr = sinkPartitionRules.split("\\|");
            for (String str : partitionNameRuleArr) {
                LOGGER.info("str:{}", str);
                if (StringUtils.isNotBlank(str) && str.contains(":")) {
                    String[] arrays = str.split(":");
                    if (arrays.length == 3) {
                        String partitionTableNames = arrays[0];
                        String partitionColumns = arrays[1];
                        String partitionPolicies = arrays[2];

                        if (StringUtils.isNotBlank(partitionTableNames) && StringUtils.isNotBlank(partitionColumns)
                                && StringUtils.isNotBlank(partitionPolicies)) {
                            boolean isMatch = Pattern.matches(partitionTableNames, tableName);
                            if (isMatch) {
                                Map<String, String> map = new HashMap<>();

                                map.put("PartitionColumns", partitionColumns);
                                map.put("PartitionPolicies", partitionPolicies);
                                list.add(map);
                                return list;
                            } else {
                                LOGGER.debug("partitionTableNames:{} tableName:{} not match!", partitionTableNames,
                                        tableName);
                            }
                        } else {
                            LOGGER.info("partitionTableNames or partitionColumns or partitionPolicies is blank!");
                        }
                    } else {
                        LOGGER.info("arrays size:{} is not equal to 3!", arrays.length);
                    }
                } else {
                    LOGGER.info("str is blank or not contains ':'!");
                }
            }
        } else {
            LOGGER.debug("sinkPartitionRules or tableName is blank!");
        }
        return list;
    }

    private static PartitionSpec parsePartitionPolicy(Schema schema, String[] partitionColumnArr,
            String[] partitionPolicyArr) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

        boolean flag = false;
        PartitionSpec partitionSpec = null;
        for (int i = 0; i < partitionColumnArr.length; i++) {
            String partitionColumn = partitionColumnArr[i];
            String partitionPolicy = partitionPolicyArr[i];

            LOGGER.info("parsePartitionPolicy partitionColumn:{} partitionPolicy:{}", partitionColumn, partitionPolicy);
            if (StringUtils.isNotBlank(partitionColumn) && StringUtils.isNotBlank(partitionPolicy)) {
                if (partitionPolicy.contains("identity")) {
                    builder = builder.identity(partitionColumn);
                    flag = true;
                } else if (Pattern.matches("bucket\\[.*]", partitionPolicy)) {
                    int leftIndex = partitionPolicy.lastIndexOf("[");
                    int rightIndex = partitionPolicy.lastIndexOf("]");
                    String size = partitionPolicy.substring(leftIndex + 1, rightIndex);
                    if (StringUtils.isNotBlank(size) && StringUtils.isNumeric(size)) {
                        int sizeInt = Integer.parseInt(size);
                        builder = builder.bucket(partitionColumn, sizeInt);
                        flag = true;
                    } else {
                        LOGGER.info("Bucket size is blank or not numeric!");
                    }
                } else if (Pattern.matches("truncate\\[.*]", partitionPolicy)) {
                    int leftIndex = partitionPolicy.lastIndexOf("[");
                    int rightIndex = partitionPolicy.lastIndexOf("]");
                    String size = partitionPolicy.substring(leftIndex + 1, rightIndex);
                    if (StringUtils.isNotBlank(size) && StringUtils.isNumeric(size)) {
                        int sizeInt = Integer.parseInt(size);
                        builder = builder.truncate(partitionColumn, sizeInt);
                        flag = true;
                    } else {
                        LOGGER.info("Bucket size is blank or not numeric!");
                    }
                } else if (partitionPolicy.contains("year")) {
                    builder = builder.year(partitionColumn);
                    flag = true;
                } else if (partitionPolicy.contains("month")) {
                    builder = builder.month(partitionColumn);
                    flag = true;
                } else if (partitionPolicy.contains("day")) {
                    builder = builder.day(partitionColumn);
                    flag = true;
                } else if (partitionPolicy.contains("hour")) {
                    builder = builder.hour(partitionColumn);
                    flag = true;
                }
            } else {
                LOGGER.warn("PartitionColumn is blank or PartitionMethod is blank, skip partition!");
            }
        }
        if (flag) {
            partitionSpec = builder.build();
        } else {
            LOGGER.warn("no partition policy!");
        }
        return partitionSpec;
    }

    public static void applySchemaChanges(UpdateSchema pendingUpdate, List<TableChange> tableChanges) {
        try {
            for (TableChange change : tableChanges) {
                if (change instanceof TableChange.AddColumn) {
                    apply(pendingUpdate, (TableChange.AddColumn) change);
                } else {
                    throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
                }
            }
            pendingUpdate.commit();
        } catch (Exception e) {
            if (e.getMessage().contains("Cannot add column, name already exists")) {
                // try catch exception for replay ddl binlog
                LOGGER.warn("ddl exec exception", e);
            } else {
                throw e;
            }
        }
    }

    public static void apply(UpdateSchema pendingUpdate, TableChange.AddColumn add) {
        Type type = add.dataType().accept(new FlinkTypeToType(RowType.of(add.dataType())));
        pendingUpdate.addColumn(parentName(add.fieldNames()), leafName(add.fieldNames()), type, add.comment());

        if (add.position() instanceof TableChange.After) {
            TableChange.After after = (TableChange.After) add.position();
            String referenceField = peerName(add.fieldNames(), after.column());
            pendingUpdate.moveAfter(DOT.join(add.fieldNames()), referenceField);

        } else if (add.position() instanceof TableChange.First) {
            pendingUpdate.moveFirst(DOT.join(add.fieldNames()));

        } else {
            Preconditions.checkArgument(add.position() == null,
                    "Cannot add '%s' at unknown position: %s", DOT.join(add.fieldNames()), add.position());
        }
    }

    public static String leafName(String[] fieldNames) {
        Preconditions.checkArgument(fieldNames.length > 0, "Invalid field name: at least one name is required");
        return fieldNames[fieldNames.length - 1];
    }

    public static String peerName(String[] fieldNames, String fieldName) {
        if (fieldNames.length > 1) {
            String[] peerNames = Arrays.copyOf(fieldNames, fieldNames.length);
            peerNames[fieldNames.length - 1] = fieldName;
            return DOT.join(peerNames);
        }
        return fieldName;
    }

    public static String parentName(String[] fieldNames) {
        if (fieldNames.length > 1) {
            return DOT.join(Arrays.copyOfRange(fieldNames, 0, fieldNames.length - 1));
        }
        return null;
    }
}
