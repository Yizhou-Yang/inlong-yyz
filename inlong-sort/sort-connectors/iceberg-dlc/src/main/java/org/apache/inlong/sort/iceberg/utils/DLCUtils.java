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
