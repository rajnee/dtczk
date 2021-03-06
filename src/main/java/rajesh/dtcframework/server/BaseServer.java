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
    protected SlavesNode slavesNode;

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
        this.tasksNode = new TasksNode(serverConfig.getTaskRootPath(), curatorFramework);
        this.slavesNode = new SlavesNode(serverConfig.getSlaveRootPath(), curatorFramework);
        this.slaveCache = new SlaveCache(slavesNode, serverConfig);
        setupRoots();
    }

    private void setupRoots() throws Exception{
        try {
            curatorFramework.create().forPath(serverConfig.getRootPath());
        } catch (KeeperException.NodeExistsException e) {
        }

        try {
            curatorFramework.create().forPath(serverConfig.getSlaveRootPath());
        } catch (KeeperException.NodeExistsException e) {
        }

        try {
            curatorFramework.create().forPath(serverConfig.getTaskRootPath());
        } catch (KeeperException.NodeExistsException e) {

        }
    }

    protected abstract void join() throws Exception;

    protected void loop() {
        logger.info(getServerId() + " - Beginning processing");
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
    public void stop() {
        stopped = true;
    }

    protected abstract void process() throws Exception;

    protected void start() throws Exception {
        establishZookeeperConnection();
        logger.info(getServerId() + " has successfully established Zookeeper connection");
        join();
        logger.info(getServerId() + " successfully joined distributed task processing");
        loop();
    }

}
