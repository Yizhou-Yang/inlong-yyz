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

package org.apache.inlong.sort.hive;

import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_DATABASE_PATTERN;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_ENABLE;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_FORMAT;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_TABLE_PATTERN;
import static org.apache.inlong.sort.hive.HiveOptions.SINK_PARTITION_NAME;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.filesystem.RowDataPartitionComputer;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.hadoop.mapred.JobConf;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.sink.PartitionPolicy;

/**
 * A {@link RowDataPartitionComputer} that converts Flink objects to Hive objects before computing
 * the partition value strings.
 */
public class HiveRowDataPartitionComputer extends RowDataPartitionComputer {

    private final DataFormatConverters.DataFormatConverter[] partitionConverters;
    private final HiveObjectConversion[] hiveObjectConversions;

    private transient JsonDynamicSchemaFormat jsonFormat;
    private final boolean sinkMultipleEnable;
    private final String sinkMultipleFormat;
    private final String databasePattern;
    private final String tablePattern;
    private HiveShim hiveShim;

    private final String sinkPartitionName;

    private final PartitionPolicy partitionPolicy;

    public HiveRowDataPartitionComputer(
            JobConf jobConf,
            HiveShim hiveShim,
            String defaultPartValue,
            String[] columnNames,
            DataType[] columnTypes,
            String[] partitionColumns,
            PartitionPolicy partitionPolicy) {
        super(defaultPartValue, columnNames, columnTypes, partitionColumns);
        this.hiveShim = hiveShim;
        this.partitionConverters =
                Arrays.stream(partitionTypes)
                        .map(TypeConversions::fromLogicalToDataType)
                        .map(DataFormatConverters::getConverterForDataType)
                        .toArray(DataFormatConverters.DataFormatConverter[]::new);
        this.hiveObjectConversions = new HiveObjectConversion[partitionIndexes.length];
        for (int i = 0; i < hiveObjectConversions.length; i++) {
            DataType partColType = columnTypes[partitionIndexes[i]];
            ObjectInspector objectInspector = HiveInspectors.getObjectInspector(partColType);
            hiveObjectConversions[i] =
                    HiveInspectors.getConversion(
                            objectInspector, partColType.getLogicalType(), hiveShim);
        }
        this.sinkPartitionName = jobConf.get(SINK_PARTITION_NAME.key(), SINK_PARTITION_NAME.defaultValue());
        this.sinkMultipleEnable = Boolean.parseBoolean(jobConf.get(SINK_MULTIPLE_ENABLE.key(), "false"));
        this.sinkMultipleFormat = jobConf.get(SINK_MULTIPLE_FORMAT.key());
        this.databasePattern = jobConf.get(SINK_MULTIPLE_DATABASE_PATTERN.key());
        this.tablePattern = jobConf.get(SINK_MULTIPLE_TABLE_PATTERN.key());
        this.partitionPolicy = partitionPolicy;
    }

    @Override
    public LinkedHashMap<String, String> generatePartValues(RowData in) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

        if (sinkMultipleEnable) {
            GenericRowData rowData = (GenericRowData) in;
            JsonNode rootNode;
            try {
                if (jsonFormat == null) {
                    jsonFormat = (JsonDynamicSchemaFormat) DynamicSchemaFormatFactory.getFormat(sinkMultipleFormat);
                }
                rootNode = jsonFormat.deserialize((byte[]) rowData.getField(0));

                String databaseName = jsonFormat.parse(rootNode, databasePattern);
                String tableName = jsonFormat.parse(rootNode, tablePattern);
                List<Map<String, Object>> physicalDataList = HiveTableUtil.jsonNode2Map(
                        jsonFormat.getPhysicalData(rootNode));

                List<String> pkListStr = jsonFormat.extractPrimaryKeyNames(rootNode);
                RowType schema = jsonFormat.extractSchema(rootNode, pkListStr);

                Map<String, Object> rawData = physicalDataList.get(0);
                ObjectIdentifier identifier = ObjectIdentifier.of("default_catalog", databaseName, tableName);
                HiveWriterFactory hiveWriterFactory = HiveTableUtil.getWriterFactory(hiveShim, identifier);
                if (hiveWriterFactory == null) {
                    HiveTableUtil.createTable(databaseName, tableName, schema, partitionPolicy);
                    hiveWriterFactory = HiveTableUtil.getWriterFactory(hiveShim, identifier);
                }

                String[] partitionColumns = hiveWriterFactory.getPartitionColumns();
                String[] columnNames = hiveWriterFactory.getAllColumns();
                DataType[] allTypes = hiveWriterFactory.getAllTypes();
                List<String> columnList = Arrays.asList(columnNames);
                int[] partitionIndexes = Arrays.stream(partitionColumns).mapToInt(columnList::indexOf).toArray();

                boolean replaceLineBreak = hiveWriterFactory.getStorageDescriptor().getInputFormat()
                        .contains("TextInputFormat");
                GenericRowData genericRowData = HiveTableUtil.getRowData(rawData, columnNames, allTypes,
                        replaceLineBreak);

                Object field;
                for (int i = 0; i < partitionIndexes.length; i++) {
                    int fieldIndex = partitionIndexes[i];
                    field = genericRowData.getField(fieldIndex);
                    DataType type = allTypes[fieldIndex];
                    ObjectInspector objectInspector = HiveInspectors.getObjectInspector(type);
                    HiveObjectConversion hiveConversion = HiveInspectors.getConversion(objectInspector,
                            type.getLogicalType(), hiveShim);
                    String partitionValue = field != null ? String.valueOf(hiveConversion.toHiveObject(field)) : null;
                    if (partitionValue == null) {
                        field = HiveTableUtil.getDefaultPartitionValue(rawData, schema, partitionPolicy);
                        partitionValue = field != null ? String.valueOf(hiveConversion.toHiveObject(field)) : null;
                    }
                    if (StringUtils.isEmpty(partitionValue)) {
                        partitionValue = defaultPartValue;
                    }
                    partSpec.put(partitionColumns[i], partitionValue);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            for (int i = 0; i < partitionIndexes.length; i++) {
                Object field = partitionConverters[i].toExternal(in, partitionIndexes[i]);
                String partitionValue = field != null ? hiveObjectConversions[i].toHiveObject(field).toString() : null;
                if (StringUtils.isEmpty(partitionValue)) {
                    partitionValue = defaultPartValue;
                }
                partSpec.put(partitionColumns[i], partitionValue);
            }
        }
        return partSpec;
    }
}
