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

package org.apache.inlong.sort.doris.schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.shaded.org.apache.commons.codec.binary.Base64;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.metric.sub.SinkTableMetricData;
import org.apache.inlong.sort.base.schema.SchemaChangeHandleException;
import org.apache.inlong.sort.base.schema.SchemaChangeHelper;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.doris.http.HttpGetEntity;
import org.apache.inlong.sort.doris.model.DorisResponse;
import org.apache.inlong.sort.doris.model.DorisVersion;
import org.apache.inlong.sort.doris.model.TableSchema;
import org.apache.inlong.sort.doris.util.DorisParseUtils;
import org.apache.inlong.sort.protocol.ddl.Column;
import org.apache.inlong.sort.protocol.ddl.enums.PositionType;
import org.apache.inlong.sort.protocol.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.protocol.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Schema change helper
 */
public class DorisSchemaChangeHelper extends SchemaChangeHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisSchemaChangeHelper.class);

    private static final String CHECK_LIGHT_SCHEMA_CHANGE_API = "http://%s/api/enable_light_schema_change/%s/%s";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private static final String GET_VERSION_API = "http://%s/api/fe_version_info";
    private static final String GET_SCHEMA_API = "http://%s/api/%s/%s/_schema";
    private static final Integer DORIS_HTTP_CALL_SUCCESS = 0;
    private static final String CONTENT_TYPE_JSON = "application/json";
    private final DorisOptions options;
    private final int maxRetries;
    private final OperationHelper operationHelper;
    private static final String DEFAULT_DATABASE = "information_schema";

    private DorisSchemaChangeHelper(JsonDynamicSchemaFormat dynamicSchemaFormat, DorisOptions options,
            boolean schemaChange,
            Map<SchemaChangeType, SchemaChangePolicy> policyMap, String databasePattern, String tablePattern,
            int maxRetries, SchemaUpdateExceptionPolicy exceptionPolicy,
            SinkTableMetricData metricData, DirtySinkHelper<Object> dirtySinkHelper) {
        super(dynamicSchemaFormat, schemaChange, policyMap, databasePattern, null,
                tablePattern, exceptionPolicy, metricData, dirtySinkHelper);
        this.options = Preconditions.checkNotNull(options, "doris options is null");
        this.maxRetries = maxRetries;
        operationHelper = OperationHelper.of(dynamicSchemaFormat, checkSupportDecimalV3());
    }

    private boolean checkSupportDecimalV3() {
        DorisVersion dorisVersion = getDorisVersion();
        if (dorisVersion == null) {
            return false;
        }
        return dorisVersion.supportDecimalV3();
    }

    public static DorisSchemaChangeHelper of(JsonDynamicSchemaFormat dynamicSchemaFormat, DorisOptions options,
            boolean schemaChange, Map<SchemaChangeType, SchemaChangePolicy> policyMap, String databasePattern,
            String tablePattern, int maxRetries, SchemaUpdateExceptionPolicy exceptionPolicy,
            SinkTableMetricData metricData, DirtySinkHelper<Object> dirtySinkHelper) {
        return new DorisSchemaChangeHelper(dynamicSchemaFormat, options, schemaChange, policyMap, databasePattern,
                tablePattern, maxRetries, exceptionPolicy, metricData, dirtySinkHelper);
    }

    @Override
    public void doAlterOperation(String database, String table, byte[] originData, String originSchema, JsonNode data,
            Map<SchemaChangeType, List<AlterColumn>> typeMap) {
        StringJoiner joiner = new StringJoiner(",");
        RowType rowType = null;
        try {
            List<String> primaryKeys = dynamicSchemaFormat.extractPrimaryKeyNames(data);
            rowType = dynamicSchemaFormat.extractSchema(data, primaryKeys);
        } catch (Exception e) {
            LOGGER.warn("Extract schema from data failed, {}", new String(originData), e);
        }
        for (Entry<SchemaChangeType, List<AlterColumn>> kv : typeMap.entrySet()) {
            SchemaChangePolicy policy = policyMap.get(kv.getKey());
            doSchemaChangeBase(kv.getKey(), policy, originSchema);
            if (policy == SchemaChangePolicy.ENABLE) {
                String alterStatement = null;
                try {
                    switch (kv.getKey()) {
                        case ADD_COLUMN:
                            alterStatement = doAddColumn(database, table, kv.getValue(), kv.getKey(),
                                    originSchema, rowType);
                            break;
                        case DROP_COLUMN:
                            alterStatement = doDropColumn(database, table, kv.getValue(), kv.getKey(), originSchema);
                            break;
                        case RENAME_COLUMN:
                            alterStatement = doRenameColumn(database, table, kv.getValue(), kv.getKey(), originSchema);
                            break;
                        case CHANGE_COLUMN_TYPE:
                            alterStatement =
                                    doChangeColumnType(database, table, kv.getValue(), kv.getKey(), originSchema);
                            break;
                        default:
                    }
                } catch (Exception e) {
                    if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                        throw new SchemaChangeHandleException(
                                String.format("Build alter statement failed, origin schema: %s", originSchema), e);
                    }
                    LOGGER.warn("Build alter statement failed, origin schema: {}", originSchema, e);
                }
                if (alterStatement != null) {
                    joiner.add(alterStatement);
                }
            }
        }
        String statement = joiner.toString();
        if (statement.length() != 0) {
            try {
                String alterStatementCommon = operationHelper.buildAlterStatementCommon(database, table);
                statement = alterStatementCommon + statement;
                // The checkLightSchemaChange is removed because most scenarios support it
                boolean result = executeStatement(database, statement);
                if (!result) {
                    LOGGER.error("Alter table failed,statement: {}", statement);
                    throw new SchemaChangeHandleException(String.format("Add column failed,statement: %s", statement));
                }
                LOGGER.info("Alter table success,statement: {}", statement);
                reportWriteMetric(originData, database, null, table);
            } catch (Exception e) {
                if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Alter table failed, origin schema: %s", originSchema), e);
                }
                handleDirtyData(data, originData, database, null, table, DirtyType.HANDLE_ALTER_TABLE_ERROR, e);
            }
        }
    }

    @Override
    public String doDropColumn(String database, String table, List<AlterColumn> alterColumns,
            SchemaChangeType type, String originSchema) {
        return operationHelper.buildDropColumnStatement(alterColumns);
    }

    public String doAddColumn(String database, String table, List<AlterColumn> alterColumns,
            SchemaChangeType type, String originSchema, RowType rowType) {
        Preconditions.checkState(alterColumns != null
                && !alterColumns.isEmpty(), "Alter columns is empty");
        Map<String, String> positionMap = new HashMap<>();
        List<RowField> fields = alterColumns.stream().map(s -> {
            Column column = s.getNewColumn();
            Preconditions.checkNotNull(column, "New column is null");
            Preconditions.checkState(column.getName() != null && !column.getName().trim().isEmpty(),
                    "The column name is blank");
            parsePosition(column, positionMap);
            return convert2RowField(column);
        }).collect(Collectors.toList());
        return operationHelper.buildAddColumnStatement(fields, positionMap, rowType);
    }

    @Override
    public String doAddColumn(String database, String table, List<AlterColumn> alterColumns,
            SchemaChangeType type, String originSchema) {
        Preconditions.checkState(alterColumns != null
                && !alterColumns.isEmpty(), "Alter columns is empty");
        Map<String, String> positionMap = new HashMap<>();
        List<RowField> fields = alterColumns.stream().map(s -> {
            Column column = s.getNewColumn();
            Preconditions.checkNotNull(column, "New column is null");
            Preconditions.checkState(column.getName() != null && !column.getName().trim().isEmpty(),
                    "The column name is blank");
            parsePosition(column, positionMap);
            return convert2RowField(column);
        }).collect(Collectors.toList());
        return operationHelper.buildAddColumnStatement(fields, positionMap, null);
    }

    private void parsePosition(Column column, Map<String, String> positionMap) {
        if (column.getPosition() != null && column.getPosition().getPositionType() != null) {
            if (column.getPosition().getPositionType() == PositionType.FIRST) {
                positionMap.put(column.getName(), null);
            } else if (column.getPosition().getPositionType() == PositionType.AFTER) {
                Preconditions.checkState(column.getPosition().getColumnName() != null
                        && !column.getPosition().getColumnName().trim().isEmpty(),
                        "The column name of Position is empty");
                positionMap.put(column.getName(), column.getPosition().getColumnName());
            }
        }
    }

    @Override
    public void doCreateTable(byte[] originData, String database, String table, SchemaChangeType type,
            String originSchema, JsonNode data, CreateTableOperation operation) {
        SchemaChangePolicy policy = policyMap.get(type);
        if (policy == SchemaChangePolicy.ENABLE) {
            try {
                List<String> primaryKeys = dynamicSchemaFormat.extractPrimaryKeyNames(data);
                Preconditions.checkState(operation.getColumns() != null && !operation.getColumns().isEmpty(),
                        String.format("The columns of table: %s.%s is empty", database, table));
                List<RowField> fields = new ArrayList<>();
                for (Column column : operation.getColumns()) {
                    fields.add(convert2RowField(column));
                }
                String comment = StringUtils.isBlank(operation.getComment()) ? Constants.CREATE_TABLE_COMMENT
                        : operation.getComment() + " " + Constants.CREATE_TABLE_COMMENT;
                RowType rowType = null;
                try {
                    rowType = dynamicSchemaFormat.extractSchema(data, primaryKeys);
                } catch (Exception e) {
                    LOGGER.warn("Extract schema from data failed, {}", new String(originData), e);
                }
                String stmt = operationHelper.buildCreateTableStatement(database, table,
                        primaryKeys, new RowType(fields), comment, rowType);
                boolean result = executeStatement(database, stmt);
                if (!result) {
                    LOGGER.error("Create table failed,statement: {}", stmt);
                    throw new IOException(String.format("Create table failed,statement: %s", stmt));
                }
                reportWriteMetric(originData, database, null, table);
                return;
            } catch (Exception e) {
                if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Drop column failed, origin schema: %s", originSchema), e);
                }
                handleDirtyData(data, originData, database, null, table, DirtyType.CREATE_TABLE_ERROR, e);
                return;
            }
        }
        doSchemaChangeBase(type, policy, originSchema);
    }

    private Map<String, Object> buildRequestParam(String column, boolean dropColumn) {
        Map<String, Object> params = new HashMap<>();
        params.put("isDropColumn", dropColumn);
        params.put("columnName", column);
        return params;
    }

    private String authHeader() {
        return "Basic " + new String(Base64.encodeBase64((options.getUsername() + ":"
                + options.getPassword()).getBytes(StandardCharsets.UTF_8)));
    }

    private boolean executeStatement(String database, String stmt) throws IOException {
        Map<String, String> param = new HashMap<>();
        param.put("stmt", stmt);
        List<String> fenodes = Arrays.asList(options.getFenodes().split(","));
        List<String> uris = fenodes.stream()
                .map(fenode -> String.format(SCHEMA_CHANGE_API, fenode, database))
                .collect(Collectors.toList());

        for (String requestUrl : uris) {
            HttpPost httpPost = new HttpPost(requestUrl);
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
            httpPost.setHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_JSON);
            httpPost.setEntity(new StringEntity(JsonDynamicSchemaFormat.OBJECT_MAPPER.writeValueAsString(param)));
            // if any fenode succeeds, return true, else keep trying
            if (sendRequest(httpPost)) {
                return true;
            }
        }
        return false;
    }

    private TableSchema getSchema(String database, String table) {
        String url = String.format(GET_SCHEMA_API, options.getFenodes(), database, table);
        HttpGetEntity httpGet = new HttpGetEntity(url);
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        return sendRequest(httpGet, r -> {
            if (r == null) {
                return null;
            }
            try {
                DorisResponse<Object> response = JsonDynamicSchemaFormat.OBJECT_MAPPER
                        .readValue(r, new TypeReference<DorisResponse<Object>>() {
                        });
                Integer code = response.getCode();
                boolean result = DORIS_HTTP_CALL_SUCCESS.equals(code);
                if (!result) {
                    LOGGER.warn("Get schema of {}.{} failed: {}", database, table, r);
                    return null;
                }
                Object data = response.getData();
                if (data == null) {
                    return null;
                }
                if (DorisParseUtils.parseUnkownTableError(data.toString())) {
                    return null;
                }
                return JsonDynamicSchemaFormat.OBJECT_MAPPER.convertValue(data, new TypeReference<TableSchema>() {
                });
            } catch (JsonProcessingException e) {
                LOGGER.warn("Parse the schema of {}.{} failed, response: {}", database, table, r);
            }
            return null;
        });
    }

    private DorisVersion getDorisVersion() {
        String url = String.format(GET_VERSION_API, options.getFenodes());
        HttpGetEntity httpGet = new HttpGetEntity(url);
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        return sendRequest(httpGet, r -> {
            if (r == null) {
                return null;
            }
            try {
                DorisResponse<Object> response = JsonDynamicSchemaFormat.OBJECT_MAPPER
                        .readValue(r, new TypeReference<DorisResponse<Object>>() {
                        });
                LOGGER.info("Get doris version response: {}", r);
                Integer code = response.getCode();
                boolean result = DORIS_HTTP_CALL_SUCCESS.equals(code);
                if (!result) {
                    LOGGER.warn("Get doris version failed: {}", r);
                    return null;
                }
                Object data = response.getData();
                if (data == null) {
                    return null;
                }
                return JsonDynamicSchemaFormat.OBJECT_MAPPER.convertValue(data, new TypeReference<DorisVersion>() {
                });
            } catch (JsonProcessingException e) {
                LOGGER.warn("Parse the doris version failed, response: {}", r);
            }
            return null;
        });
    }

    private boolean checkLightSchemaChange(String database, String table, String column, boolean dropColumn)
            throws IOException {
        String url = String.format(CHECK_LIGHT_SCHEMA_CHANGE_API, options.getFenodes(), database, table);
        Map<String, Object> param = buildRequestParam(column, dropColumn);
        HttpGetEntity httpGet = new HttpGetEntity(url);
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpGet.setEntity(new StringEntity(JsonDynamicSchemaFormat.OBJECT_MAPPER.writeValueAsString(param)));
        boolean success = sendRequest(httpGet);
        if (!success) {
            LOGGER.warn("schema change can not do table {}.{}", database, table);
        }
        return success;
    }

    private boolean sendRequest(HttpUriRequest request) {
        return sendRequest(request, r -> {
            if (r == null) {
                return false;
            }
            try {
                DorisResponse<Object> response = JsonDynamicSchemaFormat.OBJECT_MAPPER.readValue(r,
                        new TypeReference<DorisResponse<Object>>() {
                        });
                Integer code = response.getCode();
                boolean result = DORIS_HTTP_CALL_SUCCESS.equals(code);
                if (!result) {
                    LOGGER.warn("The return code of Doris shows it failed: {}", r);
                }
                if (response.getData() != null
                        && DorisParseUtils.parseAlreadyExistsError(response.getData().toString())) {
                    return true;
                }
                return result;
            } catch (JsonProcessingException e) {
                LOGGER.warn("Parse the response failed: {}", r, e);
            }
            return false;
        });
    }

    private <T> T sendRequest(HttpUriRequest request, Function<String, T> responseHandler) {
        Preconditions.checkNotNull(responseHandler, "responseHandler is null");
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            for (int i = 0; i <= maxRetries; i++) {
                try {
                    CloseableHttpResponse response = httpclient.execute(request);
                    final int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == HttpStatus.SC_OK && response.getEntity() != null) {
                        String loadResult = EntityUtils.toString(response.getEntity());
                        return responseHandler.apply(loadResult);
                    }
                } catch (Exception e) {
                    if (i >= maxRetries) {
                        LOGGER.error("send http requests error", e);
                        throw new IOException(e);
                    }
                    try {
                        Thread.sleep(1000L * i);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to send http request,interrupted while doing another attempt", e);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("send request error", e);
            throw new SchemaChangeHandleException("send request error", e);
        }
        return responseHandler.apply(null);
    }

    /**
     * Apply schema change
     *
     * @param tableIdentifier The identifier of table, format by 'database.table'
     * @param data The origin data
     */
    public boolean applySchemaChange(String tableIdentifier, JsonNode data) {
        boolean result = false;
        String[] tableIdentififerArr = tableIdentifier.split("\\.");
        String database = tableIdentififerArr[0], table = tableIdentififerArr[1];
        try {
            TableSchema tableSchema = getSchema(database, table);
            List<String> pkNames = dynamicSchemaFormat.extractPrimaryKeyNames(data);
            RowType rowType = dynamicSchemaFormat.extractSchema(data, pkNames);
            if (tableSchema == null) {
                if (checkSchemaChangeTypeEnable(SchemaChangeType.CREATE_TABLE)) {
                    String stmt = operationHelper.buildCreateTableStatement(database, table, pkNames,
                            rowType, Constants.CREATE_TABLE_COMMENT, null);
                    try {
                        result = executeStatement(database, stmt);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Create table auto failed", e);
                    }
                    if (result) {
                        return true;
                    }
                    LOGGER.warn("Create table auto failed maybe it is already created,statement: {}", stmt);
                    tableSchema = getSchema(database, table);
                } else {
                    return false;
                }
            }
            if (tableSchema != null) {
                if (!checkSchemaChangeTypeEnable(SchemaChangeType.ADD_COLUMN)) {
                    return false;
                }
                List<RowField> addFiels = operationHelper.extractAddFields(tableSchema, rowType);
                if (addFiels.isEmpty()) {
                    return true;
                }
                String stmt = operationHelper.buildAddColumnStatement(addFiels, new HashMap<>(), null);
                if (stmt != null) {
                    stmt = operationHelper.buildAlterStatementCommon(database, table) + stmt;
                    try {
                        result = executeStatement(database, stmt);
                    } catch (IOException e) {
                        throw new IllegalArgumentException(
                                String.format("Add column auto failed, statement: %s", stmt), e);
                    }
                    if (!result) {
                        throw new SchemaChangeHandleException(
                                String.format("Add column auto failed, statement: %s", stmt));
                    }
                } else {
                    LOGGER.warn("The schema of {}.{} maybe has already same as the schema of data", database, table);
                }
            }
        } catch (Throwable e) {
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                throw new SchemaChangeHandleException(
                        String.format("Apply schema-change failed for %s.%s", database, table), e);
            }
            LOGGER.warn("Apply schema-change failed for {}.{}", database, table, e);
        }
        return result;
    }

    public boolean createDatabaseAuto(String tableIdentifier, JsonNode data) {
        if (!checkSchemaChangeTypeEnable(SchemaChangeType.CREATE_TABLE)) {
            return false;
        }
        boolean result = false;
        String database = tableIdentifier.split("\\.")[0];
        try {
            String stmt = operationHelper.buildCreateDatabaseStatement(database);
            result = executeStatement(DEFAULT_DATABASE, stmt);
            if (!result) {
                throw new SchemaChangeHandleException(
                        String.format("Create database auto failed, statement: %s", stmt));
            }
        } catch (Throwable e) {
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                throw new SchemaChangeHandleException(
                        String.format("Create database: %s auto failed", database), e);
            }
            LOGGER.warn("Create database: {} auto failed", database, e);
        }
        return result;
    }
}
