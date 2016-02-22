package rajesh.dtcframework.server.config;

import org.apache.curator.RetryPolicy;

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

    default public  String getTaskRootPath() {
        return "/" + getTaskRoot();
    }

    default public  String getRootPath() {
        return "/" + getDTCRoot();
    }

    default public  String getTaskPath(String taskId) {
        return "/" + getTaskRoot() + "/" + taskId;
    }

    default public String getSlaveRootPath() {
        return "/" + getSlaveRoot();
    }

    default public  String getSlavePath(String slaveId) {
        return getSlaveRootPath() + "/" + slaveId;
    }
}
