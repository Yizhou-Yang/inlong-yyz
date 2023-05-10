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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.List;

/**
 * Inlong cluster node request for Agent
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = ClusterType.AGENT)
@ApiModel("Inlong cluster node request for Agent")
public class AgentClusterNodeRequest extends ClusterNodeRequest {

    @ApiModelProperty(value = "Agent group name")
    private String agentGroup;

    @NotBlank(message = "cluster name cannot be blank")
    @ApiModelProperty(value = "Cluster name")
    @Pattern(regexp = "^[a-z0-9_.-]{1,128}$", message = "only supports lowercase letters, numbers, '.', '-', or '_'")
    private String name;

    @NotBlank(message = "clusterTags cannot be blank")
    @ApiModelProperty(value = "Cluster tags, separated by commas")
    private String clusterTags;

    private List<DeleteAgentClusterNodeRequest> deleteAgentClusterNodeRequests;

    public AgentClusterNodeRequest() {
        this.setType(ClusterType.AGENT);
    }

}
