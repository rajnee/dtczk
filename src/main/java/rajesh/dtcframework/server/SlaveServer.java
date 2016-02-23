package rajesh.dtcframework.server;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rajesh.dtcframework.Task;
import rajesh.dtcframework.config.ServerConfig;

import java.util.List;

/**
 * Created by rajesh on 2/18/16.
 */
public abstract class SlaveServer extends BaseServer {

    private static Logger logger = LoggerFactory.getLogger(SlaveServer.class);

    public SlaveServer(ServerConfig serverConfig) {
        super(serverConfig);
    }

    @Override
    protected void join() throws Exception {
        slavesNode.join(getServerId(), "".getBytes());
        tasksNode.joinTask(getServerId());
    }

    @Override
    protected void process() throws Exception {
        List<Task> tasks = null;

        tasks = getTasks();
        if (tasks != null) {
            for (Task t : tasks) {
                try {
                    executeTask(t);
                    markComplete(t);
                } catch (Exception e) {
                    logger.warn("Error executing task (ignoring):" + t);
                }
            }
        }
    }


    protected void markComplete(Task t) throws Exception {
        tasksNode.markComplete(getServerId(), t);
    }

    private Object tasksLock = new Object();

    private CuratorWatcher watcher = new CuratorWatcher() {
        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {
            synchronized (tasksLock) {
                tasksLock.notifyAll();
            }
        }
    };

    protected List<Task> getTasks() throws Exception{
        List<Task> tasks = null;
        while (tasks == null || tasks.size() == 0) {
            if (isStopped()) break;
            tasks = tasksNode.getTasks(getServerId());
            if (tasks.size() == 0) {
                synchronized (tasksLock) {
                    curatorFramework.checkExists().usingWatcher(watcher).forPath(tasksNode.getSlaveServerTaskPath(getServerId()));
                    tasksLock.wait(1000);
                }
            }
        }
        return tasks;
    }


    public final String getSlaveServerTaskPath() {
        return tasksNode.getSlaveServerTaskPath(getServerId());
    }
    protected abstract void executeTask(Task t) throws Exception;


}
