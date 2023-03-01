package org.apache.inlong.sort.iceberg.provider;

import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.InstanceProfileCredentials;
import com.tencent.cloud.wedata.credential.WedataCredential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.auth.AbstractCOSCredentialProvider;
import org.apache.hadoop.fs.auth.DlcServiceClient;
import org.apache.hadoop.fs.auth.model.DescribeLakeFsAccessRequest;
import org.apache.hadoop.fs.auth.model.DescribeLakeFsAccessResponse;
import org.apache.hadoop.fs.auth.model.LakeFileSystemToken;
import org.apache.inlong.sort.iceberg.utils.DLCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeDataFsCosCredentialsProvider extends AbstractCOSCredentialProvider {
    private static final Logger LOG = LoggerFactory.getLogger(WeDataFsCosCredentialsProvider.class);

    public static final String END_POINT = "service.endpoint";
    public static final String END_POINT_DEFAULT = "dlc.tencentcloudapi.com";
    public static final String SECRET_ID = "service.secret.id";
    public static final String SECRET_KEY = "service.secret.key";
    public static final String SECRET_TOKEN = "service.secret.token";
    public static final String REGION = "service.region";
    public static final String USER_APPID = "user.appid";
    public static final String FETCH_RETRY = "service.fetch.retry";
    public static final String TOKEN_AVAILABLE_PERIOD = "token.available.period";
    public static final String REQUEST_IDENTITY_TOKEN = "request.identity.token";

    private static final long DEFAULT_REFRESH_INTERVAL_MILLISECONDS = 30 * 1000;    // millisecond
    private static final int DEFAULT_MAX_FETCH_RETRY_TIMES = 3;

    public static DlcServiceClient serviceClient;

    private long lastFailedRefreshTimeInMilliseconds = 0;

    private final ReentrantLock lock = new ReentrantLock();

    private InstanceProfileCredentials credentials;

    public WeDataFsCosCredentialsProvider(URI uri, Configuration conf) {
        super(uri, conf);
    }

    @Override
    public COSCredentials getCredentials() {
        LOG.info("emhui WeDataFsCosCredentialsProvider getCredentials");
        if (this.credentials == null || this.credentials.isExpired()) {
            try {
                this.lock.lock();
                if (this.credentials == null || this.credentials.isExpired()) {
                    credentials = fetch(getUri().toString());
                }
            } finally {
                this.lock.unlock();
            }
        } else if (this.credentials.willSoonExpire() && this.shouldRefresh()) {
            try {
                this.lock.lock();
                credentials = fetch(getUri().toString());
            } finally {
                this.lock.unlock();
            }
        }

        return credentials;
    }

    @Override
    public void refresh() {
        this.getCredentials();
    }

    private InstanceProfileCredentials fetch(String path) {
        String endPoint = getConf().get(END_POINT, END_POINT_DEFAULT);
        String region = getConf().get(REGION, "");
        String userAppId = getConf().get(USER_APPID, "");

        // 请求临时密钥参数
        Map<String, String> options = DLCUtils.getTmpTokenOptions(getConf());
        LOG.info("emhui fetch is [{}], options is [{}]", getConf(), options);
        WedataCredential wedataCredential = new WedataCredential(options);
        LOG.info("init dlc service client: {}, {}, {}", endPoint, serviceClient, region);
        InstanceProfileCredentials userCred = new InstanceProfileCredentials(userAppId,
                wedataCredential.getSecretId(),
                wedataCredential.getSecretKey(),
                wedataCredential.getToken(),
                wedataCredential.getExpiredTime());

        String bkt = getBucket();
        checkConfigItem(blankString(bkt), "request path invalid, %s", path);
        if (!checkHostingBucket(bkt)) {
            LOG.debug("get cred for non dlc host bucket, {}", path);
            return userCred;
        }

        serviceClient = DlcServiceClient.buildServiceClient(endPoint, region, wedataCredential);

        DescribeLakeFsAccessRequest request = new DescribeLakeFsAccessRequest();
        request.setFsPath(path);
        long period = getConf().getLong(TOKEN_AVAILABLE_PERIOD, 43200);
        String identity = getConf().get(REQUEST_IDENTITY_TOKEN, "");
        request.setAvailablePeriod(period);
        request.setIdentity(identity);

        int retryTimes = getConf().getInt(FETCH_RETRY, DEFAULT_MAX_FETCH_RETRY_TIMES);
        for (int i = 1; i <= retryTimes; i++) {
            try {
                DescribeLakeFsAccessResponse response = serviceClient.DescribeLakeFsAccess(request);
                LakeFileSystemToken token = response.getAccessToken();
                if (blankString(token.getAppId())) {
                    return new InstanceProfileCredentials(
                            URLDecoder.decode(token.getSecretId(), "utf-8"),
                            URLDecoder.decode(token.getSecretKey(), "utf-8"),
                            URLDecoder.decode(token.getToken(), "utf-8"),
                            token.getExpiredTime());
                }
                return new InstanceProfileCredentials(
                        token.getAppId(),
                        URLDecoder.decode(token.getSecretId(), "utf-8"),
                        URLDecoder.decode(token.getSecretKey(), "utf-8"),
                        URLDecoder.decode(token.getToken(), "utf-8"),
                        token.getExpiredTime());
            } catch (TencentCloudSDKException e) {
                this.lastFailedRefreshTimeInMilliseconds = System.currentTimeMillis();
                String errorMsg = String.format("Service fetch service access token failed, retry: %d/%d", i, retryTimes);
                LOG.error(errorMsg, e);
            } catch (UnsupportedEncodingException e) {
                this.lastFailedRefreshTimeInMilliseconds = System.currentTimeMillis();
                String errorMsg = String.format("Url decode service access token failed, retry: %d/%d", i, retryTimes);
                LOG.error(errorMsg, e);
            }
        }

        return null;
    }

    private boolean shouldRefresh() {
        if (null == this.credentials) {
            return true;
        } else {
            return System.currentTimeMillis() - this.lastFailedRefreshTimeInMilliseconds > DEFAULT_REFRESH_INTERVAL_MILLISECONDS;
        }
    }

    public boolean checkHostingBucket(String bucket) {
        // e: 40000006da0c3eb6390e2729ca9a2a283abee25ed153804d75c1306dcd1ebdd6b1eb4167@dlcf5e2-100017516939-1636514832-100017307912-1304028854
        String pattern = ".*?[@dlc].*?-*-\\d{10}-\\d{12}-\\d{10}";
        return Pattern.matches(pattern, bucket);
    }

    private String getBucket() {
        URI uri = getUri();
        return uri.getHost();
    }

    private boolean blankString(String str) {
        return str == null || str.equals("");
    }

    protected void checkConfigItem(boolean cond, String message, Object... args) {
        if (cond) {
            throw new IllegalArgumentException(String.format(message, args));
        }
    }
}
