package rajesh.dtcframework.cache;

import com.google.common.cache.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import rajesh.dtcframework.Slave;
import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.server.SlavesNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by rajesh on 2/20/16.
 */
public class SlaveCache {

    private ServerConfig serverConfig;
    private SlavesNode slavesNode;

    private LoadingCache<String, List<Slave>> slaveCache;
//TODO: when slaves leave, we need to take care of cleaning up Task Nodes for the slave and reassigning

    public SlaveCache(final SlavesNode slavesNode, final ServerConfig serverConfig) {
        this.slavesNode = slavesNode;
        this.serverConfig = serverConfig;

        slaveCache =  CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, List<Slave>>() {
                            public List<Slave> load(String key) throws Exception {
                                return loadSlavesFromZookeeper();
                            }
                        });
    }

    public void refresh() {
        slaveCache.invalidateAll();
    }

    public List<Slave> getSlaves() throws Exception{
        return slaveCache.get("slaves");
    }

    protected List<Slave> loadSlavesFromZookeeper() throws Exception {
        System.out.println("loading slaves from data source");
        List<String> slaveStrings = slavesNode.getSlaves( () -> refresh());
        final List<Slave> slaves = new ArrayList<Slave>();
        for (String s: slaveStrings) {
            slaves.add(new SimpleSlave(s, true));
        }
        return slaves;
    }
}
