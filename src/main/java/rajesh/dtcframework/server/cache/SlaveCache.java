package rajesh.dtcframework.server.cache;

import com.google.common.cache.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import rajesh.dtcframework.server.Slave;
import rajesh.dtcframework.server.config.ServerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by rajesh on 2/20/16.
 */
public class SlaveCache {

    private CuratorFramework curatorFramework;
    private ServerConfig serverConfig;

    private LoadingCache<String, List<Slave>> slaveCache;
//TODO: when slaves leave, we need to take care of cleaning up Task Nodes for the slave and reassigning

    private CuratorWatcher watcher = new CuratorWatcher() {
        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {
            refresh();
        }
    };

    public SlaveCache(final CuratorFramework curatorFramework, final ServerConfig serverConfig) {
        this.curatorFramework = curatorFramework;
        this.serverConfig = serverConfig;

        slaveCache =  CacheBuilder.newBuilder()
                .maximumSize(10000)
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
        System.out.println("loading from data source");
        List<String> slaveStrings = curatorFramework.getChildren().usingWatcher(watcher).forPath(serverConfig.getSlaveRootPath());
        final List<Slave> slaves = new ArrayList<Slave>();
        slaveStrings.forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                SimpleSlave sl = new SimpleSlave(s, true);
                slaves.add(sl);
            }
        });
        return slaves;
    }
}
