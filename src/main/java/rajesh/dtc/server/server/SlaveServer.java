package rajesh.dtc.server.server;

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

    @Override
    protected void join() throws Exception {
        registerSlave(serverConfig.getServerId(), "");
    }

    @Override
    protected void process() throws Exception {
        List<Task> tasks = getTasks();
        for (Task t: tasks) {
            executeTask(t);
            markComplete(t);
        }
    }

    protected abstract void executeTask(Task t);

}
