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

package org.apache.inlong.manager.pojo.cluster.agent;

import javax.validation.constraints.NotBlank;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Inlong cluster node request
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Cluster node request")
public class DeleteAgentClusterNodeRequest {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "ID of the parent cluster")
    private Integer parentId;

    @ApiModelProperty(value = "Cluster type, including AGENT, DATAPROXY, etc.")
    @NotBlank(message = "type cannot be blank")
    private String type;

    @ApiModelProperty(value = "Cluster IP")
    @NotBlank(message = "ip cannot be blank")
    private String ip;

    @ApiModelProperty(value = "Extended params")
    private String extParams;

    @ApiModelProperty(value = "Cluster status")
    private Integer status;

    @ApiModelProperty(value = "Version number")
    private Integer version;

}
