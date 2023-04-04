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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.shaded.org.apache.commons.codec.binary.Base64;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
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
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.schema.SchemaChangeHandleException;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.ddl.operations.AlterOperation;
import org.apache.inlong.sort.ddl.operations.CreateTableOperation;
import org.apache.inlong.sort.ddl.operations.Operation;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.apache.inlong.sort.util.SchemaChangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema change helper
 */
public class SchemaChangeHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChangeHelper.class);

    private static final String CHECK_LIGHT_SCHEMA_CHANGE_API = "http://%s/api/enable_light_schema_change/%s/%s";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private static final String DORIS_HTTP_CALL_SUCCESS = "0";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private final boolean schemaChange;
    private final Map<SchemaChangeType, SchemaChangePolicy> policyMap;
    private final DorisOptions options;
    private final JsonDynamicSchemaFormat dynamicSchemaFormat;
    private final String databasePattern;
    private final String tablePattern;
    private final int maxRetries;
    private final OperationHelper operationHelper;
    private final SchemaUpdateExceptionPolicy exceptionPolicy;

    private SchemaChangeHelper(JsonDynamicSchemaFormat dynamicSchemaFormat, DorisOptions options, boolean schemaChange,
            Map<SchemaChangeType, SchemaChangePolicy> policyMap, String databasePattern, String tablePattern,
            int maxRetries, SchemaUpdateExceptionPolicy exceptionPolicy) {
        this.dynamicSchemaFormat = Preconditions.checkNotNull(dynamicSchemaFormat, "dynamicSchemaFormat is null");
        this.options = Preconditions.checkNotNull(options, "doris options is null");
        this.schemaChange = schemaChange;
        this.policyMap = policyMap;
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
        this.maxRetries = maxRetries;
        this.exceptionPolicy = exceptionPolicy;
        operationHelper = OperationHelper.of(dynamicSchemaFormat);
    }

    public static SchemaChangeHelper of(JsonDynamicSchemaFormat dynamicSchemaFormat, DorisOptions options,
            boolean schemaChange, Map<SchemaChangeType, SchemaChangePolicy> policyMap, String databasePattern,
            String tablePattern, int maxRetries, SchemaUpdateExceptionPolicy exceptionPolicy) {
        return new SchemaChangeHelper(dynamicSchemaFormat, options, schemaChange, policyMap, databasePattern,
                tablePattern, maxRetries, exceptionPolicy);
    }

    /**
     * Process schema change for Doris
     *
     * @param data The origin data
     * @throws Exception The exception may throws when executing
     */
    public void process(JsonNode data) {
        if (!schemaChange) {
            return;
        }
        Operation operation;
        try {
            JsonNode operationNode = Preconditions.checkNotNull(data.get("operation"),
                    "Operation node is null");
            operation = Preconditions.checkNotNull(
                    dynamicSchemaFormat.objectMapper.convertValue(operationNode, new TypeReference<Operation>() {
                    }), "Operation is null");
        } catch (Exception e) {
            if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                throw new SchemaChangeHandleException("Extract Operation from origin data failed", e);
            }
            LOGGER.warn("Extract Operation from origin data failed", e);
            return;
        }
        String originSchema = dynamicSchemaFormat.extractDDL(data);
        SchemaChangeType type = SchemaChangeUtils.extractSchemaChangeType(operation);
        if (type == null) {
            LOGGER.warn("Unsupported for schema-change: {}", originSchema);
            return;
        }
        switch (type) {
            case ADD_COLUMN:
                doAddColumn(type, originSchema, data, (AlterOperation) operation);
                break;
            case DROP_COLUMN:
                doDropColumn(type, originSchema, data, (AlterOperation) operation);
                break;
            case RENAME_COLUMN:
                doRenameColumn(type, originSchema);
                break;
            case CHANGE_COLUMN_TYPE:
                doChangeColumnType(type, originSchema);
                break;
            case CREATE_TABLE:
                doCreateTable(type, originSchema, data, (CreateTableOperation) operation);
                break;
            case DROP_TABLE:
                doDropTable(type, originSchema);
                break;
            case RENAME_TABLE:
                doRenameTable(type, originSchema);
                break;
            case TRUNCATE_TABLE:
                doTruncateTable(type, originSchema);
                break;
            default:
                LOGGER.warn("Unsupported for {}: {}", type, originSchema);
        }
    }

    private void doChangeColumnType(SchemaChangeType type, String originSchema) {
        SchemaChangePolicy policy = policyMap.get(type);
        if (policy == SchemaChangePolicy.ENABLE) {
            LOGGER.warn("Unsupported for {}: {}", type, originSchema);
            return;
        }
        doSchemaChangeBase(policy, originSchema);
    }

    private void doRenameColumn(SchemaChangeType type, String originSchema) {
        SchemaChangePolicy policy = policyMap.get(type);
        if (policy == SchemaChangePolicy.ENABLE) {
            LOGGER.warn("Unsupported for {}: {}", type, originSchema);
            return;
        }
        doSchemaChangeBase(policy, originSchema);
    }

    private void doDropColumn(SchemaChangeType type, String originSchema, JsonNode data, AlterOperation operation) {
        SchemaChangePolicy policy = policyMap.get(type);
        if (policy == SchemaChangePolicy.ENABLE) {
            try {
                String database = dynamicSchemaFormat.parse(data, databasePattern);
                String table = dynamicSchemaFormat.parse(data, tablePattern);
                String stmt = operationHelper.buildDropColumnStatement(database, table, operation);
                if (checkLightSchemaChange(database, table,
                        operation.getAlterColumns().get(0).getNewColumn().getName(), true)) {
                    boolean result = executeStatement(database, stmt);
                    if (!result) {
                        LOGGER.error("Drop column failed,statement: {}", stmt);
                        throw new IOException(String.format("Drop column failed,statement: %s", stmt));
                    }
                    return;
                }
                LOGGER.warn("Only support drop column when enable light-schema-change for Doris");
                return;
            } catch (Exception e) {
                if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Drop column failed, origin schema: %s", originSchema), e);
                }
                return;
            }
        }
        doSchemaChangeBase(policy, originSchema);
    }

    private void doAddColumn(SchemaChangeType type, String originSchema, JsonNode data, AlterOperation operation) {
        SchemaChangePolicy policy = policyMap.get(type);
        if (policy == SchemaChangePolicy.ENABLE) {
            try {
                String database = dynamicSchemaFormat.parse(data, databasePattern);
                String table = dynamicSchemaFormat.parse(data, tablePattern);
                String stmt = operationHelper.buildAddColumnStatement(database, table, operation);
                if (checkLightSchemaChange(database, table,
                        operation.getAlterColumns().get(0).getNewColumn().getName(), false)) {
                    boolean result = executeStatement(database, stmt);
                    if (!result) {
                        LOGGER.error("Add column failed,statement: {}", stmt);
                        throw new SchemaChangeHandleException(String.format("Add column failed,statement: %s", stmt));
                    }
                    return;
                }
                LOGGER.warn("Only support add column when enable light-schema-change for Doris");
                return;
            } catch (Exception e) {
                if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Add column failed, origin schema: %s", originSchema), e);
                }
                return;
            }
        }
        doSchemaChangeBase(policy, originSchema);
    }

    private void doTruncateTable(SchemaChangeType type, String originSchema) {
        SchemaChangePolicy policy = policyMap.get(SchemaChangeType.TRUNCATE_TABLE);
        if (policy == SchemaChangePolicy.ENABLE) {
            LOGGER.warn("Unsupported for {}: {}", type, originSchema);
            return;
        }
        doSchemaChangeBase(policy, originSchema);
    }

    private void doRenameTable(SchemaChangeType type, String originSchema) {
        SchemaChangePolicy policy = policyMap.get(SchemaChangeType.RENAME_TABLE);
        if (policy == SchemaChangePolicy.ENABLE) {
            LOGGER.warn("Unsupported for {}: {}", type, originSchema);
            return;
        }
        doSchemaChangeBase(policy, originSchema);
    }

    private void doDropTable(SchemaChangeType type, String originSchema) {
        SchemaChangePolicy policy = policyMap.get(SchemaChangeType.DROP_TABLE);
        if (policy == SchemaChangePolicy.ENABLE) {
            LOGGER.warn("Unsupported for {}: {}", type, originSchema);
            return;
        }
        doSchemaChangeBase(policy, originSchema);
    }

    private void doCreateTable(SchemaChangeType type, String originSchema, JsonNode data,
            CreateTableOperation operation) {
        SchemaChangePolicy policy = policyMap.get(type);
        if (policy == SchemaChangePolicy.ENABLE) {
            try {
                String database = dynamicSchemaFormat.parse(data, databasePattern);
                String table = dynamicSchemaFormat.parse(data, tablePattern);
                List<String> primaryKeys = dynamicSchemaFormat.extractPrimaryKeyNames(data);
                String stmt = operationHelper.buildCreateTableStatement(database, table, primaryKeys, operation);
                // it is only used for test
                stmt = stmt + " PROPERTIES (\n"
                        + "\"replication_num\" = \"1\" \n"
                        + ")";
                boolean result = executeStatement(database, stmt);
                if (!result) {
                    LOGGER.error("Create table failed,statement: {}", stmt);
                    throw new IOException(String.format("Create table failed,statement: %s", stmt));
                }
                return;
            } catch (Exception e) {
                if (exceptionPolicy == SchemaUpdateExceptionPolicy.THROW_WITH_STOP) {
                    throw new SchemaChangeHandleException(
                            String.format("Drop column failed, origin schema: %s", originSchema), e);
                }
                return;
            }
        }
        doSchemaChangeBase(policy, originSchema);
    }

    private void doSchemaChangeBase(SchemaChangePolicy policy, String schema) {
        if (policy == null) {
            return;
        }
        switch (policy) {
            case LOG:
                LOGGER.warn("Unsupported for {}: {}", policy, schema);
                break;
            case ERROR:
                throw new SchemaChangeHandleException(String.format("Unsupported for %s: %s", policy, schema));
            default:
        }
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
        String requestUrl = String.format(SCHEMA_CHANGE_API, options.getFenodes(), database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_JSON);
        httpPost.setEntity(new StringEntity(dynamicSchemaFormat.objectMapper.writeValueAsString(param)));
        return sendRequest(httpPost);
    }

    private boolean checkLightSchemaChange(String database, String table, String column, boolean dropColumn)
            throws IOException {
        return true;
//        String url = String.format(CHECK_LIGHT_SCHEMA_CHANGE_API, options.getFenodes(), database, table);
//        Map<String, Object> param = buildRequestParam(column, dropColumn);
//        HttpGetEntity httpGet = new HttpGetEntity(url);
//        httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
//        httpGet.setEntity(new StringEntity(dynamicSchemaFormat.objectMapper.writeValueAsString(param)));
//        boolean success = sendRequest(httpGet);
//        if (!success) {
//            LOGGER.warn("schema change can not do table {}.{}", database, table);
//        }
//        return success;
    }

    @SuppressWarnings("unchecked")
    private boolean sendRequest(HttpUriRequest request) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            for (int i = 0; i <= maxRetries; i++) {
                try {
                    CloseableHttpResponse response = httpclient.execute(request);
                    final int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == HttpStatus.SC_OK && response.getEntity() != null) {
                        String loadResult = EntityUtils.toString(response.getEntity());
                        Map<String, Object> responseMap = dynamicSchemaFormat.objectMapper
                                .readValue(loadResult, Map.class);
                        String code = responseMap.getOrDefault("code", "-1").toString();
                        if (DORIS_HTTP_CALL_SUCCESS.equals(code)) {
                            return true;
                        }
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
        return false;
    }
}
