package org.apache.inlong.sort.iceberg.provider;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
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

    public WedataDLCCredentialProvider() {
    }

    @Override
    public void initialize(Configuration conf) {
        String secretId = conf.get("qcloud.dlc.secret-id");
        String secretKey = conf.get("qcloud.dlc.secret-key");
        Preconditions.checkArgument(secretId != null, "qcloud.dlc.secret-id must be set.");
        Preconditions.checkArgument(secretKey != null, "qcloud.dlc.secret-key must be set.");
        this.setSecretId(secretId);
        this.setSecretKey(secretKey);

        Map<String, String> options = DLCUtils.getTmpTokenOptions(conf);
        log.info("emhui WedataDLCCredentialProvider is [{}], options is [{}]", conf, options);
        WedataCredential wedataCredential = new WedataCredential(options);

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
//        return new WedataCredential(this.options);
        return new Credential(this.getSecretId(), this.getSecretKey());
    }
}

