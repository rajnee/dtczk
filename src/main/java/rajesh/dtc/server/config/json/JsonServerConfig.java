package rajesh.dtc.server.config.json;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import rajesh.dtc.server.config.ServerConfig;

/**
 * Created by rajesh on 2/20/16.
 */
public class JsonServerConfig implements ServerConfig{

    public String connection_string;
    public String server_id;
    public int connection_timeout;
    public RetryParams retry_params;
    public Locations locations;

    @Override
    public String getConnectionString() {
        return connection_string;
    }

    @Override
    public String getServerId() {
        return server_id;
    }

    @Override
    public int getConnectionTimeout() {
        return connection_timeout;
    }

    @Override
    public RetryPolicy getRetryPolicy() {
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(retry_params.sleep_ms, retry_params.max_retries);
        return exponentialBackoffRetry;
    }

    @Override
    public String getMasterLockPath() {
        return locations.master_lock_path;
    }

    @Override
    public String getDTCRoot() {
        return locations.dtc_root;
    }

    @Override
    public String getSlaveRoot() {
        return locations.slave_root;
    }

    @Override
    public String getTaskRoot() {
        return locations.task_root;
    }
}
