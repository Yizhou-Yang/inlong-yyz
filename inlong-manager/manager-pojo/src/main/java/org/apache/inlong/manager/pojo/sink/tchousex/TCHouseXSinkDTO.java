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

package org.apache.inlong.manager.pojo.sink.tchousex;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.AESUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import javax.validation.constraints.NotNull;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Sink info of TCHouse-X
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TCHouseXSinkDTO {

    @ApiModelProperty("TCHouse-X hostname")
    private String hostname;

    @ApiModelProperty("TCHouse-X port")
    private Integer port;

    @ApiModelProperty("Username for TCHouse-X accessing")
    private String username;

    @ApiModelProperty("Password for TCHouse-X accessing")
    private String password;

    @ApiModelProperty("TCHouse-X table name, such as: db.tbl")
    private String tableIdentifier;

    @ApiModelProperty("The primary key of sink table")
    private String primaryKey;

    @ApiModelProperty("Password encrypt version")
    private Integer encryptVersion;

    @ApiModelProperty("Properties for doris")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static TCHouseXSinkDTO getFromRequest(TCHouseXSinkRequest request) throws Exception {
        Integer encryptVersion = AESUtils.getCurrentVersion(null);
        String passwd = null;
        if (StringUtils.isNotEmpty(request.getPassword())) {
            passwd = AESUtils.encryptToString(request.getPassword().getBytes(StandardCharsets.UTF_8),
                    encryptVersion);
        }
        return TCHouseXSinkDTO.builder()
                .hostname(request.getHostname())
                .port(request.getPort())
                .username(request.getUsername())
                .password(passwd)
                .tableIdentifier(request.getTableIdentifier())
                .encryptVersion(encryptVersion)
                .properties(request.getProperties())
                .build();
    }

    public static TCHouseXSinkDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, TCHouseXSinkDTO.class).decryptPassword();
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    String.format("parse extParams of TCHouse-X SinkDTO failure: %s", e.getMessage()));
        }
    }

    private TCHouseXSinkDTO decryptPassword() throws Exception {
        if (StringUtils.isNotEmpty(this.password)) {
            byte[] passwordBytes = AESUtils.decryptAsString(this.password, this.encryptVersion);
            this.password = new String(passwordBytes, StandardCharsets.UTF_8);
        }
        return this;
    }

}
