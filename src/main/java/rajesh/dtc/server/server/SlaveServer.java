package rajesh.dtc.server.server;

import org.apache.zookeeper.CreateMode;
import rajesh.dtc.server.Task;
import rajesh.dtc.server.config.ServerConfig;
import rajesh.dtc.server.server.BaseServer;

import java.util.List;

/**
 * Created by rajesh on 2/18/16.
 */
public abstract class SlaveServer extends BaseServer {

    public SlaveServer(ServerConfig serverConfig) {
        super(serverConfig);
    }

    protected boolean registerSlave(String id, String data) throws Exception {
        curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(serverConfig.getSlaveRoot() + "/" + id, data.getBytes() );
        return true;
    }

    @Override
    protected void join() throws Exception {
        registerSlave(serverConfig.getServerId(), "");
    }

    @Override
    protected void process() throws Exception {
        List<Task> tasks = getTasksForPath(getSlaveServerTaskPath());
        for (Task t: tasks) {
            executeTask(t);
            markComplete(t);
        }
    }


    protected void markComplete(Task t) throws Exception {
        curatorFramework.delete().forPath(getSlaveServerTaskPath() + "/" + t.getId());
    }

    protected String getSlaveServerTaskPath() {
        return  getTaskPathForServer(serverConfig.getServerId());
    }

    protected abstract void executeTask(Task t);

}
