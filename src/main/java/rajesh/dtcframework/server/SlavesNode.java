package rajesh.dtcframework.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rajesh.dtcframework.Slave;

import java.util.List;
import java.util.function.Function;

/**
 * Created by rajesh on 2/21/16.
 */
public class SlavesNode {

    private String slavesRootPath;
    private CuratorFramework curatorFramework;
    private static Logger logger = LoggerFactory.getLogger(SlavesNode.class);

    private static class XCuratorWatcher implements   CuratorWatcher {
        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {

        }
    };

    public SlavesNode(String slavesRootPath, CuratorFramework curatorFramework) throws Exception {
        this.slavesRootPath = slavesRootPath;
        this.curatorFramework = curatorFramework;
        try {
            curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(slavesRootPath);
        } catch (KeeperException.NodeExistsException nee) {

        }
    }

    public void join(String slaveServerId, byte[] data) throws Exception {
        curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(slavesRootPath + "/" + slaveServerId, data);
    }

    public List<String> getSlaves(Runnable function) throws Exception {
        return curatorFramework.getChildren()
                .usingWatcher(new CuratorWatcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) throws Exception {
                        function.run();
                    }
                })
                .forPath(slavesRootPath);
    }


}
