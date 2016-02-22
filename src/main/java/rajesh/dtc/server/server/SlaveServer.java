package rajesh.dtc.server.server;

import org.apache.curator.framework.api.CreateBuilder;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rajesh.dtc.server.Task;
import rajesh.dtc.server.config.ServerConfig;
import rajesh.dtc.server.server.BaseServer;

import java.util.List;

/**
 * Created by rajesh on 2/18/16.
 */
public abstract class SlaveServer extends BaseServer {

    private static Logger logger = LoggerFactory.getLogger(SlaveServer.class);

    public SlaveServer(ServerConfig serverConfig) {
        super(serverConfig);
    }

    protected boolean registerSlave(String id, String data) throws Exception {
        curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(serverConfig.getSlaveRootPath() + "/" + id, data.getBytes() );
        return true;
    }

    @Override
    protected void join() throws Exception {
        registerSlave(serverConfig.getServerId(), "");
        curatorFramework.create().forPath(getSlaveServerTaskPath());
    }

    @Override
    protected void process() throws Exception {
        List<Task> tasks = getTasks();
        for (Task t: tasks) {
            try {
                executeTask(t);
                markComplete(t);
            } catch (Exception e) {
                logger.warn("Error executing task (ignoring):" + t);
            }
        }
    }


    protected void markComplete(Task t) throws Exception {
        curatorFramework.delete().forPath(getSlaveServerTaskPath() + "/" + t.getId());
    }

    protected List<Task> getTasks() throws Exception{
        return getTasksForPath(getSlaveServerTaskPath());
    }

    protected String getSlaveServerTaskPath() {
        return  getTaskPathForServer(serverConfig.getServerId());
    }

    protected abstract void executeTask(Task t) throws Exception;

}
