package rajesh.dtcframework;

import com.netflix.curator.test.TestingServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.Before;
import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.config.json.JsonServerConfig;
import rajesh.dtcframework.config.json.ServerConfigFactory;
import rajesh.dtcframework.server.SlavesNode;

import java.io.File;
import java.net.URL;

/**
 * Created by rajesh on 2/21/16.
 */
public abstract class BaseDTCTest {

    protected TestingServer testingServer;
    protected ServerConfig serverConfig;
    protected CuratorFramework curatorFramework;
    protected SlavesNode slavesNode;

    protected abstract String getConfigFileName();
    @Before
    public void setup() throws Exception {
        testingServer = new TestingServer();
        URL url = System.class.getResource("/" + getConfigFileName());
        File file = new File(url.getFile());
        JsonServerConfig jsonServerConfig;
        jsonServerConfig = (JsonServerConfig) ServerConfigFactory.fromJson(file);
        jsonServerConfig.connection_string = testingServer.getConnectString();
        serverConfig = jsonServerConfig;
        setupCuratorFramework();
        slavesNode = new SlavesNode(serverConfig.getSlaveRootPath(), curatorFramework);
    }

    public void setupCuratorFramework() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectionTimeoutMs(serverConfig.getConnectionTimeout()).connectString(serverConfig.getConnectionString());
        builder.retryPolicy(serverConfig.getRetryPolicy());
        builder.namespace(serverConfig.getDTCRoot());
        curatorFramework = builder.build();
        curatorFramework.start();
    }

}
