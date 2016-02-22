package rajesh.dtcframework.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rajesh.dtcframework.cache.SlaveCache;
import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.Task;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rajesh on 2/18/16.
 */
public abstract class BaseServer {

    private static Logger logger = LoggerFactory.getLogger(BaseServer.class);

    protected ServerConfig serverConfig;
    protected CuratorFramework curatorFramework;
    protected TasksNode tasksNode;

    protected SlaveCache slaveCache;

    public BaseServer(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public final String getServerId() { return serverConfig.getServerId(); }

    protected void establishZookeeperConnection() throws Exception{
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectionTimeoutMs(serverConfig.getConnectionTimeout()).connectString(serverConfig.getConnectionString());
        builder.retryPolicy(serverConfig.getRetryPolicy());
        builder.namespace(serverConfig.getDTCRoot());
        curatorFramework = builder.build();
        curatorFramework.start();
        this.slaveCache = new SlaveCache(curatorFramework, serverConfig);
        this.tasksNode = new TasksNode(serverConfig.getTaskRootPath(), curatorFramework);
        setupRoots();
    }

    private void setupRoots() throws Exception{
        curatorFramework.create().forPath(serverConfig.getRootPath());
        curatorFramework.create().forPath(serverConfig.getSlaveRootPath());
        try {
            curatorFramework.create().forPath(serverConfig.getTaskRootPath());
        } catch (KeeperException.NodeExistsException e) {

        }
    }

    protected abstract void join() throws Exception;

    protected void loop() {
        while(!stopped) {
            try {
                process();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isStopped() {
        return stopped;
    }

    private boolean stopped;
    void stop() {
        stopped = true;
    }

    protected abstract void process() throws Exception;

    protected void start() throws Exception {
        establishZookeeperConnection();
        join();
        loop();
    }

}
