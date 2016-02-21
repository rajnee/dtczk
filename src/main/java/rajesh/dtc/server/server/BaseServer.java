package rajesh.dtc.server.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rajesh.dtc.server.Slave;
import rajesh.dtc.server.Task;
import rajesh.dtc.server.cache.SlaveCache;
import rajesh.dtc.server.config.ServerConfig;

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


    protected void establishZookeeperConnection() {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectionTimeoutMs(serverConfig.getConnectionTimeout()).connectString(serverConfig.getConnectionString());
        builder.retryPolicy(serverConfig.getRetryPolicy());
        builder.namespace(serverConfig.getDTCRoot());
        curatorFramework = builder.build();
        curatorFramework.start();
        this.slaveCache = new SlaveCache(curatorFramework, serverConfig);
    }

    protected abstract void join() throws Exception;

    protected void loop() {
        while(true) {
            try {
                process();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    protected List<Slave> getSlaves() {
        try {
            return slaveCache.getSlaves();
        } catch (Exception e) {
            logger.error("Error retrieving slaves from slave cache", e);
        }
        return null;
    }

    protected boolean registerSlave(String id, String data) throws Exception {
        curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(serverConfig.getSlaveRoot() + "/" + id, data.getBytes() );
        return true;
    }
    protected abstract void process() throws Exception;

    protected void start() throws Exception {
        establishZookeeperConnection();
        join();
        loop();
    }

    protected List<Task> getTasks() {
        try {
            List<String> taskIds = curatorFramework.getChildren().forPath(getSlaveServerTaskPath());
            List<Task> tasks = new ArrayList<Task>(taskIds.size());
            for (String s: taskIds) {
                try {
                    SimpleTask st = new SimpleTask(s);
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

    private String getSlaveServerTaskPath() {
        return  serverConfig.getTaskRoot() + "/" + serverConfig.getServerId();
    }

    protected void markComplete(Task t) throws Exception {
        curatorFramework.delete().forPath(getSlaveServerTaskPath() + "/" + t.getId());
    }

    protected class SimpleTask implements Task {
        private final String id;
        private String data;

        private SimpleTask(String id) throws Exception {
            this.id = id;
            getData();
        }


        @Override
        public String getId() {
            return id;
        }

        @Override
        public byte[] getData() throws Exception{
            return curatorFramework.getData().forPath(getSlaveServerTaskPath() + "/" + id);
        }
    }
}
