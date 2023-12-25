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

package org.apache.inlong.manager.pojo.source.oceanBase;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * OceanBase source info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OceanBaseDTO {

    @ApiModelProperty("Hostname of the OceanBase server")
    private String hostname;

    @ApiModelProperty("Port of the OceanBase server")
    private Integer port;

    @ApiModelProperty("Username of the OceanBase server")
    private String username;

    @ApiModelProperty("Password of the OceanBase server")
    private String password;

    @ApiModelProperty("Database name")
    private String databaseName;

    @ApiModelProperty("Table name")
    private String tableName;

    @ApiModelProperty("Tenant name")
    private String tenantName;

    @ApiModelProperty("table list")
    private String tableList;

    @ApiModelProperty("Scan startup mode")
    private String scanStartupMode;

    @ApiModelProperty("Primary key must be shared by all tables")
    private String primaryKey;

    @ApiModelProperty("Properties for Oracle")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static OceanBaseDTO getFromRequest(OceanBaseRequest request) {
        return OceanBaseDTO.builder()
                .databaseName(request.getDatabaseName())
                .hostname(request.getHostname())
                .port(request.getPort())
                .username(request.getUsername())
                .password(request.getPassword())
                .tableList(request.getTableList())
                .tableName(request.getTableName())
                .tenantName(request.getTenantName())
                .primaryKey(request.getPrimaryKey())
                .scanStartupMode(request.getScanStartupMode())
                .properties(request.getProperties())
                .build();
    }

    public static OceanBaseDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, OceanBaseDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("parse extParams of OceanBaseSource failure: %s", e.getMessage()));
        }
    }

}
