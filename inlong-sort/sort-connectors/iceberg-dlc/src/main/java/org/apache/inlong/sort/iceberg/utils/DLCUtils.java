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

package org.apache.inlong.sort.iceberg.utils;

import com.tencent.cloud.wedata.credential.utils.Constants;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class DLCUtils {

    // dlc的临时密钥参数
    public static final String REGION = "service.region";
    public static final String USER_APPID = "user.appid";
    public static final String JWT_SECRET = "service.jwt.secret";
    public static final String EXECUTOR_SECRET_KEY = "executor.secret.key";
    public static final String EXECUTOR_SECRET_ID = "executor.secret.id";
    public static final String GATEWAY_URL = "gateway.url";
    public static final String OWNER_UIN = "owner.uin";
    public static final String OPERATOR_UIN = "operator.uin";

    public static Map<String, String> getTmpTokenOptions(Configuration conf) {
        Map<String, String> options = new HashMap<>();
        options.put(Constants.GATEWAY_URL, conf.get(GATEWAY_URL));
        options.put(Constants.JWT_SECRET, conf.get(JWT_SECRET));
        options.put(Constants.TENANT_ID, conf.get(USER_APPID));
        options.put(Constants.USER_ID, conf.get(OPERATOR_UIN));
        options.put(Constants.OWNER_USER_ID, conf.get(OWNER_UIN));
        options.put(Constants.REGION, conf.get(REGION));
        options.put(Constants.SECRET_KEY, conf.get(EXECUTOR_SECRET_KEY));
        options.put(Constants.SECRET_ID, conf.get(EXECUTOR_SECRET_ID));
        return options;
    }

    public static Map<String, String> getTmpTokenOptions(Map<String, String> conf) {
        Map<String, String> options = new HashMap<>();
        options.put(Constants.GATEWAY_URL, conf.get(GATEWAY_URL));
        options.put(Constants.JWT_SECRET, conf.get(JWT_SECRET));
        options.put(Constants.TENANT_ID, conf.get(USER_APPID));
        options.put(Constants.USER_ID, conf.get(OPERATOR_UIN));
        options.put(Constants.OWNER_USER_ID, conf.get(OWNER_UIN));
        options.put(Constants.REGION, conf.get(REGION));
        options.put(Constants.SECRET_KEY, conf.get(EXECUTOR_SECRET_KEY));
        options.put(Constants.SECRET_ID, conf.get(EXECUTOR_SECRET_ID));
        return options;
    }
}
