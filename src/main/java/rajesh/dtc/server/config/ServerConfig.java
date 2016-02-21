package rajesh.dtc.server.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by rajesh on 2/19/16.
 */
public class ServerConfig {

    public ServerConfig(String jsonConfig) {

    }

    public String getConnectionString() {
        return "localhost:2181";
    }

    public String getServerId() {
        return "hostid";
    }

    public int getConnectionTimeout() {
        return 5000;
    }

    public RetryPolicy getRetryPolicy() {
        return new ExponentialBackoffRetry(500, 3);
    }

    public String getMasterLockPath() {
        return "masterlock";
    }

    public String getDTCRoot() {
        return "DTC";
    }

    public String getSlaveRoot() {
        return "SLAVE";
    }

    public String getTaskRoot() {
        return "TASKS";
    }
}
