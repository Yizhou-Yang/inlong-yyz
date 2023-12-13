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

package org.apache.inlong.sort.iceberg.provider;

import com.google.common.base.Preconditions;
import com.qcloud.dlc.auth.AbstractDLCCredentialProvider;
import com.qcloud.dlc.auth.IDLCCredentialProvider;
import com.tencent.cloud.wedata.credential.WedataCredential;
import com.tencentcloudapi.common.Credential;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.inlong.sort.iceberg.utils.DLCUtils;

@Slf4j
public class WedataDLCCredentialProvider extends AbstractDLCCredentialProvider implements IDLCCredentialProvider {

    private String appId;
    private String secretId;
    private String secretKey;
    private Map<String, String> options;

    public WedataDLCCredentialProvider() {
    }

    @Override
    public void initialize(Configuration conf) {
        String secretId = conf.get("qcloud.dlc.secret-id");
        String executorSecretId = conf.get("executor.secret.id");
        String secretKey = conf.get("qcloud.dlc.secret-key");
        String executorSecretKey = conf.get("executor.secret.key");

        Preconditions.checkArgument(secretId != null, "qcloud.dlc.secret-id must be set.");
        Preconditions.checkArgument(executorSecretId != null, "executor.secret.id must be set.");
        Preconditions.checkArgument(secretKey != null, "qcloud.dlc.secret-key must be set.");
        Preconditions.checkArgument(executorSecretKey != null, "executor.secret.key must be set.");

        Map<String, String> options = DLCUtils.getTmpTokenOptions(conf);
        this.options = options;

    }

    public String getAppId() {
        return this.appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getSecretId() {
        return this.secretId;
    }

    public void setSecretId(String secretId) {
        this.secretId = secretId;
    }

    public String getSecretKey() {
        return this.secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public Credential getCredential() {
        return new WedataCredential(this.options);
    }
}
