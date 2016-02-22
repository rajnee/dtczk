package rajesh.dtcframework.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
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

    protected SlaveCache slaveCache;

    public BaseServer(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }


    protected void establishZookeeperConnection() throws Exception{
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectionTimeoutMs(serverConfig.getConnectionTimeout()).connectString(serverConfig.getConnectionString());
        builder.retryPolicy(serverConfig.getRetryPolicy());
        builder.namespace(serverConfig.getDTCRoot());
        curatorFramework = builder.build();
        curatorFramework.start();
        this.slaveCache = new SlaveCache(curatorFramework, serverConfig);
        setupRoots();
    }

    private void setupRoots() throws Exception{
        curatorFramework.create().forPath(serverConfig.getRootPath());
        curatorFramework.create().forPath(serverConfig.getSlaveRootPath());
        curatorFramework.create().forPath(serverConfig.getTaskRootPath());
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

    protected List<Task> getTasksForPath(String slaveServerTaskPath) {
        try {
            List<String> taskIds = curatorFramework.getChildren().forPath(slaveServerTaskPath);
            List<Task> tasks = new ArrayList<Task>(taskIds.size());
            for (String s: taskIds) {
                try {
                    SimpleTask st = new SimpleTask(s, slaveServerTaskPath);
                    tasks.add(st);
                } catch (Exception e) {
                    logger.error("error creating simple task {}", s, e);
                }
            }
            return tasks;
        } catch (Exception e) {
            logger.error("error retrieving tasks", e);
            throw new RuntimeException(e);
        }
    }


    protected String getTaskPathForServer(String serverId) {
        return  serverConfig.getTaskRootPath() + "/" + serverId;
    }

    protected class SimpleTask implements Task {
        private final String id;
        private String data;
        private final String slaveServerPath;

        private SimpleTask(String id, String slaveServerPath) throws Exception {
            this.id = id;
            this.slaveServerPath = slaveServerPath;
            getData();
        }


        @Override
        public String getId() {
            return id;
        }

        @Override
        public byte[] getData() throws Exception{
            return curatorFramework.getData().forPath(slaveServerPath + "/" + id);
        }

        @Override
        public String toString() {
            return "[" + id + "," + data + "]";
        }
    }
}
