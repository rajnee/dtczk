package rajesh.dtc.server.cache;

import com.netflix.curator.test.TestingServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.Before;
import org.junit.Test;
import rajesh.dtc.server.BaseDTCTest;
import rajesh.dtc.server.Slave;
import rajesh.dtc.server.config.ServerConfig;
import rajesh.dtc.server.config.json.JsonServerConfig;
import rajesh.dtc.server.config.json.ServerConfigFactory;

import java.io.File;
import java.net.URL;
import java.util.List;
import static org.junit.Assert.*;

/**
 * Created by rajesh on 2/21/16.
 */
public class SlaveCacheTest extends BaseDTCTest {

    private SlaveCache slaveCache;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        slaveCache = new SlaveCache(curatorFramework, serverConfig);
        setupRoots();
        //Add some slaves
        curatorFramework.create().forPath(serverConfig.getSlavePath("slave1"));
        curatorFramework.create().forPath(serverConfig.getSlavePath("slave2"));
    }

    private void setupRoots() throws Exception{
        curatorFramework.create().forPath("/" + serverConfig.getDTCRoot());
        curatorFramework.create().forPath("/" + serverConfig.getSlaveRoot());
        curatorFramework.create().forPath("/" + serverConfig.getTaskRoot());
    }

    @Override
    protected String getConfigFileName() {
        return "testconfig1.json";
    }

    @Test
    public void testSlaveCache() throws Exception {
        List<Slave> slaves = slaveCache.getSlaves();
        assertEquals(2, slaves.size());

        curatorFramework.create().forPath(serverConfig.getSlavePath("slave3"));

        //Wait for refresh of slaves
        Thread.currentThread().sleep(1000);
        slaves = slaveCache.getSlaves();
        assertEquals(3, slaves.size());

        curatorFramework.delete().forPath(serverConfig.getSlavePath("slave2"));
        Thread.currentThread().sleep(1000);
        slaves = slaveCache.getSlaves();
        assertEquals(2, slaves.size());


    }


}
