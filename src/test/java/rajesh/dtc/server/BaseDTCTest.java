package rajesh.dtc.server;

import com.netflix.curator.test.TestingServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.Before;
import rajesh.dtc.server.config.ServerConfig;
import rajesh.dtc.server.config.json.JsonServerConfig;
import rajesh.dtc.server.config.json.ServerConfigFactory;

import java.io.File;
import java.net.URL;

/**
 * Created by rajesh on 2/21/16.
 */
public class BaseDTCTest {

    protected TestingServer testingServer;
    protected ServerConfig serverConfig;
    protected CuratorFramework curatorFramework;

    @Before
    public void setup() throws Exception {
        testingServer = new TestingServer();
        URL url = System.class.getResource("/testconfig1.json");
        File file = new File(url.getFile());
        JsonServerConfig jsonServerConfig;
        jsonServerConfig = (JsonServerConfig) ServerConfigFactory.fromJson(file);
        jsonServerConfig.connection_string = testingServer.getConnectString();
        serverConfig = jsonServerConfig;
        setupCuratorFramework();
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
