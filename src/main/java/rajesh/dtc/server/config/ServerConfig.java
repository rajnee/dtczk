package rajesh.dtc.server.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by rajesh on 2/19/16.
 */
public interface ServerConfig {

    public String getConnectionString();

    public String getServerId();

    public int getConnectionTimeout();

    public RetryPolicy getRetryPolicy();

    public String getMasterLockPath();

    public String getDTCRoot();

    public String getSlaveRoot();

    public String getTaskRoot();
}
