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

package org.apache.inlong.sort.tchousex.flink.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.dirty.utils.DirtySinkFactoryUtils;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.apache.inlong.sort.tchousex.flink.option.TCHouseXOptions;
import org.apache.inlong.sort.tchousex.flink.option.TCHouseXSinkOptions;
import org.apache.inlong.sort.util.SchemaChangeUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.inlong.sort.base.Constants.DIRTY_PREFIX;
import static org.apache.inlong.sort.base.Constants.INLONG_AUDIT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;
import static org.apache.inlong.sort.base.Constants.SINK_AUTO_CREATE_TABLE_WHEN_SNAPSHOT;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_DATABASE_PATTERN;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_ENABLE;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_FORMAT;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_SCHEMA_UPDATE_POLICY;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_TABLE_PATTERN;
import static org.apache.inlong.sort.base.Constants.SINK_SCHEMA_CHANGE_ENABLE;
import static org.apache.inlong.sort.base.Constants.SINK_SCHEMA_CHANGE_POLICIES;
import static org.apache.inlong.sort.base.Constants.SINK_UID;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXOptions.HOSTNAME;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXOptions.PASSWORD;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXOptions.PORT;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXOptions.TABLE_IDENTIFIER;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXOptions.USERNAME;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXSinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXSinkOptions.SINK_BUFFER_FLUSH_MAX_BYTES;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXSinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.inlong.sort.tchousex.flink.option.TCHouseXSinkOptions.SINK_MAX_RETRIES;

public final class TCHouseXDynamicTableFactory implements DynamicTableSinkFactory {

    private static final Map<SchemaChangeType, List<SchemaChangePolicy>> SUPPORTS_POLICY_MAP = new HashMap<>();
    static {
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.CREATE_TABLE,
                Arrays.asList(SchemaChangePolicy.ENABLE, SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.DROP_TABLE,
                Arrays.asList(SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.RENAME_TABLE,
                Arrays.asList(SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.TRUNCATE_TABLE,
                Arrays.asList(SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.ADD_COLUMN,
                Arrays.asList(SchemaChangePolicy.ENABLE, SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.DROP_COLUMN,
                Arrays.asList(SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.RENAME_COLUMN,
                Arrays.asList(SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.CHANGE_COLUMN_TYPE,
                Arrays.asList(SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
        SUPPORTS_POLICY_MAP.put(SchemaChangeType.ALTER,
                Arrays.asList(SchemaChangePolicy.ENABLE, SchemaChangePolicy.IGNORE, SchemaChangePolicy.LOG,
                        SchemaChangePolicy.ERROR));
    }

    @Override
    public String factoryIdentifier() {
        return "tchouse-x";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TABLE_IDENTIFIER);
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_BUFFER_FLUSH_MAX_BYTES);
        options.add(SINK_MULTIPLE_FORMAT);
        options.add(SINK_MULTIPLE_DATABASE_PATTERN);
        options.add(SINK_MULTIPLE_TABLE_PATTERN);
        options.add(SINK_MULTIPLE_ENABLE);
        options.add(SINK_MULTIPLE_SCHEMA_UPDATE_POLICY);
        options.add(INLONG_METRIC);
        options.add(INLONG_AUDIT);
        options.add(FactoryUtil.SINK_PARALLELISM);
        options.add(SINK_SCHEMA_CHANGE_ENABLE);
        options.add(SINK_SCHEMA_CHANGE_POLICIES);
        options.add(SINK_AUTO_CREATE_TABLE_WHEN_SNAPSHOT);
        options.add(SINK_UID);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validateExcept(DIRTY_PREFIX);
        ReadableConfig readableConfig = helper.getOptions();
        TCHouseXOptions tCHouseXOptions = createTCHouseXOptions(readableConfig);
        TCHouseXSinkOptions tCHouseXSinkOptions = createTCHouseXSinkOptions(readableConfig);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        String databasePattern = readableConfig.get(SINK_MULTIPLE_DATABASE_PATTERN);
        String tablePattern = readableConfig.get(SINK_MULTIPLE_TABLE_PATTERN);
        boolean multipleSink = readableConfig.get(SINK_MULTIPLE_ENABLE);
        SchemaUpdateExceptionPolicy schemaUpdatePolicy = readableConfig.get(SINK_MULTIPLE_SCHEMA_UPDATE_POLICY);
        String sinkMultipleFormat = readableConfig.get(SINK_MULTIPLE_FORMAT);
        boolean enableSchemaChange = readableConfig.get(SINK_SCHEMA_CHANGE_ENABLE);
        String schemaChangePolicies = readableConfig.get(SINK_SCHEMA_CHANGE_POLICIES);
        validateSinkMultiple(physicalSchema.toPhysicalRowDataType(), multipleSink, sinkMultipleFormat,
                databasePattern, tablePattern, enableSchemaChange, schemaChangePolicies);
        String inlongMetric = readableConfig.get(INLONG_METRIC);
        String auditHostAndPorts = readableConfig.get(INLONG_AUDIT);
        Integer parallelism = readableConfig.get(FactoryUtil.SINK_PARALLELISM);
        // Build the dirty data side-output
        final DirtyOptions dirtyOptions = DirtyOptions.fromConfig(helper.getOptions());
        final DirtySink<Object> dirtySink = DirtySinkFactoryUtils.createDirtySink(context, dirtyOptions);
        String uid = readableConfig.get(SINK_UID);
        // create and return dynamic table sink
        return new TCHouseXDynamicTableSink(
                tCHouseXOptions,
                tCHouseXSinkOptions,
                physicalSchema,
                multipleSink,
                sinkMultipleFormat,
                databasePattern,
                tablePattern,
                schemaUpdatePolicy,
                inlongMetric,
                auditHostAndPorts,
                parallelism,
                dirtyOptions,
                dirtySink,
                enableSchemaChange,
                schemaChangePolicies,
                uid);
    }

    private TCHouseXSinkOptions createTCHouseXSinkOptions(ReadableConfig readableConfig) {
        Integer sinkBatchSize = readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS);
        Integer sinkMaxRetries = readableConfig.get(SINK_MAX_RETRIES);
        Long sinkBatchIntervalMs = readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis();
        Long sinkBatchBytes = readableConfig.get(SINK_BUFFER_FLUSH_MAX_BYTES);
        return TCHouseXSinkOptions.builder().setSinkBatchSize(sinkBatchSize).setSinkMaxRetries(sinkMaxRetries)
                .setSinkBatchIntervalMs(sinkBatchIntervalMs).setSinkBatchBytes(sinkBatchBytes).build();
    }

    private TCHouseXOptions createTCHouseXOptions(ReadableConfig readableConfig) {
        String host = readableConfig.get(HOSTNAME);
        int port = readableConfig.get(PORT);
        String username = readableConfig.get(USERNAME);
        String password = readableConfig.get(PASSWORD);
        String tableIdentifier = readableConfig.get(TABLE_IDENTIFIER);
        return TCHouseXOptions.builder().setHostName(host).setPort(port).setUsername(username).setPassword(password)
                .setTableIdentifier(tableIdentifier).build();
    }

    private void validateSinkMultiple(DataType physicalDataType, boolean multipleSink, String sinkMultipleFormat,
            String databasePattern, String tablePattern, boolean enableSchemaChange, String schemaChangePolicies) {
        if (multipleSink) {
            if (StringUtils.isBlank(databasePattern)) {
                throw new ValidationException(
                        "The option 'sink.multiple.database-pattern'"
                                + " is not allowed blank when the option 'sink.multiple.enable' is 'true'");
            }
            if (StringUtils.isBlank(tablePattern)) {
                throw new ValidationException(
                        "The option 'sink.multiple.table-pattern' "
                                + "is not allowed blank when the option 'sink.multiple.enable' is 'true'");
            }
            if (StringUtils.isBlank(sinkMultipleFormat)) {
                throw new ValidationException(
                        "The option 'sink.multiple.format' "
                                + "is not allowed blank when the option 'sink.multiple.enable' is 'true'");
            }
            DynamicSchemaFormatFactory.getFormat(sinkMultipleFormat);
            Set<String> supportFormats = DynamicSchemaFormatFactory.SUPPORT_FORMATS.keySet();
            if (!supportFormats.contains(sinkMultipleFormat)) {
                throw new ValidationException(String.format(
                        "Unsupported value '%s' for '%s'. "
                                + "Supported values are %s.",
                        sinkMultipleFormat, SINK_MULTIPLE_FORMAT.key(), supportFormats));
            }
            if (physicalDataType.getLogicalType() instanceof VarBinaryType) {
                throw new ValidationException(
                        "Only supports 'BYTES' or 'VARBINARY(n)' of PhysicalDataType "
                                + "when the option 'sink.multiple.enable' is 'true'");
            }
            if (enableSchemaChange) {
                Map<SchemaChangeType, SchemaChangePolicy> policyMap = SchemaChangeUtils
                        .deserialize(schemaChangePolicies);
                for (Entry<SchemaChangeType, SchemaChangePolicy> kv : policyMap.entrySet()) {
                    List<SchemaChangePolicy> policies = SUPPORTS_POLICY_MAP.get(kv.getKey());
                    if (policies == null) {
                        throw new ValidationException(
                                String.format("Unsupported type of schema-change: %s", kv.getKey()));
                    }
                    if (!policies.contains(kv.getValue())) {
                        throw new ValidationException(
                                String.format("Unsupported policy of schema-change: %s", kv.getValue()));
                    }
                }
            }
        }
    }
}