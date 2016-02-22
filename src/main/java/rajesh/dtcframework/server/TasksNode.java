package rajesh.dtcframework.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rajesh.dtcframework.Task;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rajesh on 2/21/16.
 */
public class TasksNode {

    private String taskRootPath;
    private CuratorFramework curatorFramework;
    private static Logger logger = LoggerFactory.getLogger(TasksNode.class);

    public TasksNode(String taskRootPath, CuratorFramework curatorFramework) throws Exception {
        this.taskRootPath = taskRootPath;
        this.curatorFramework = curatorFramework;
        try {
            curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(taskRootPath);
        } catch (KeeperException.NodeExistsException nee) {

        }
    }

    public void addTask(Task t, String slaveServerId) throws Exception {
        CreateBuilder createBuilder = curatorFramework.create();
        createBuilder.withMode(CreateMode.PERSISTENT);
        createBuilder.forPath(getSlaveServerTaskPath(slaveServerId) + "/" + t.getId(), t.getData());
    }

    public String getTaskPath(String slaveServerId, Task t) {
        return getSlaveServerTaskPath(slaveServerId) + "/" + t.getId();
    }

    public void joinTask(String slaveServerId) throws Exception {
        curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(taskRootPath + "/" + slaveServerId);
    }

    public void markComplete(String slaveServerId, Task t) throws Exception {
        curatorFramework.delete().forPath(getSlaveServerTaskPath(slaveServerId) + "/" + t.getId());
    }

    public String getSlaveServerTaskPath(String slaveServerId) {
        return  taskRootPath + "/" + slaveServerId;
    }


    protected List<Task> getTasks(String slaveServerId) {
        String slaveServerTaskPath = taskRootPath + "/" + slaveServerId;
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
