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

package org.apache.inlong.sort.protocol.node.extract;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.Metadata;
import org.apache.inlong.sort.protocol.constant.OceanBaseConstant;
import org.apache.inlong.sort.protocol.constant.OceanBaseConstant.ScanStartUpMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * oceanBase extract node for extract data from oceanBase
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("oceanBaseExtract")
@JsonInclude(Include.NON_NULL)
@Data
public class OceanBaseExtractNode extends ExtractNode implements Metadata, InlongMetric, Serializable {

    @Nullable
    @JsonProperty("primaryKey")
    private String primaryKey;
    @JsonProperty("tenantName")
    private String tenantName;
    @JsonProperty("hostname")
    private String hostname;
    @JsonProperty("port")
    private Integer port;
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    @Nullable
    @JsonProperty("databaseName")
    private String databaseName;
    @Nullable
    @JsonProperty("tableName")
    private String tableName;
    @Nullable
    @JsonProperty("tableList")
    private String tableList;
    @Nullable
    @JsonProperty("scanStartupMode")
    private ScanStartUpMode scanStartupMode;

    @JsonCreator
    public OceanBaseExtractNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermark_field") WatermarkField watermarkField,
            @JsonProperty("properties") Map<String, String> properties,
            @Nullable @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("tenantName") String tenantName,
            @JsonProperty("hostname") String hostname,
            @JsonProperty("port") Integer port,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableList") String tableList,
            @Nullable @JsonProperty("scanStartupMode") ScanStartUpMode scanStartupMode) {
        super(id, name, fields, watermarkField, properties);
        this.primaryKey = primaryKey;
        this.hostname = Preconditions.checkNotNull(hostname, "hostname is null");
        this.port = Preconditions.checkNotNull(port, "port is null");
        this.tenantName = Preconditions.checkNotNull(tenantName, "tenantName is null");
        this.username = Preconditions.checkNotNull(username, "username is null");
        this.password = Preconditions.checkNotNull(password, "password is null");
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableList = tableList;
        this.scanStartupMode = scanStartupMode;
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put(OceanBaseConstant.CONNECTOR, OceanBaseConstant.OCEANBASE_CDC);
        options.put(OceanBaseConstant.HOSTNAME, hostname);
        options.put(OceanBaseConstant.PORT, Preconditions.checkNotNull(port, "databaseName is null").toString());
        options.put(OceanBaseConstant.USERNAME, username);
        options.put(OceanBaseConstant.PASSWORD, password);
        if (scanStartupMode == null) {
            scanStartupMode = OceanBaseConstant.ScanStartUpMode.forName(OceanBaseConstant.SCAN_STARTUP_MODE);
        }
        if (ScanStartUpMode.INITIAL == scanStartupMode) {
            options.put(OceanBaseConstant.DATABASE_NAME,
                    Preconditions.checkNotNull(databaseName, "databaseName is null"));
            options.put(OceanBaseConstant.TABLE_NAME, Preconditions.checkNotNull(tableName, "tableName is null"));
        } else {
            options.put(OceanBaseConstant.TABLE_LIST, Preconditions.checkNotNull(tableList, "tableList is null"));
        }
        if (scanStartupMode != null) {
            options.put(OceanBaseConstant.SCAN_STARTUP_MODE, scanStartupMode.getValue());
        }
        return options;
    }

    /**
     * Is virtual.
     * By default, the planner assumes that a metadata column can be used for both reading and writing.
     * However, in many cases an external system provides more read-only metadata fields than writable fields.
     * Therefore, it is possible to exclude metadata columns from persisting using the VIRTUAL keyword.
     *
     * @param metaField The meta field
     * @return true if it is virtual else false
     */
    @Override
    public boolean isVirtual(MetaField metaField) {
        return true;
    }

    /**
     * Supported meta field set
     *
     * @return The set of supported meta field
     */
    @Override
    public Set<MetaField> supportedMetaFields() {
        return EnumSet.of(MetaField.PROCESS_TIME, MetaField.TENANT_NAME,
                MetaField.DATABASE_NAME, MetaField.TABLE_NAME, MetaField.OP_TS);
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

    @Override
    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String getMetadataKey(MetaField metaField) {
        String metadataKey;
        switch (metaField) {
            case TABLE_NAME:
                metadataKey = "table_name";
                break;
            case DATABASE_NAME:
                metadataKey = "database_name";
                break;
            case OP_TS:
                metadataKey = "op_ts";
                break;
            case TENANT_NAME:
                metadataKey = "tenant_name";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for %s: %s",
                        this.getClass().getSimpleName(), metaField));
        }
        return metadataKey;
    }
}
