package rajesh.dtc.server.config.json;

import org.junit.Test;
import rajesh.dtc.server.config.ServerConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import static org.junit.Assert.*;
/**
 * Created by rajesh on 2/20/16.
 */
public class JsonServerConfigTest {

    @Test
    public void testServerConfig() throws FileNotFoundException {
        URL url = System.class.getResource("/master1.json");
        System.out.println("Url for master1 is:" + url);
        File file = new File(url.getFile());
        System.out.println("File is :" + file.getAbsolutePath());
        ServerConfig serverConfig = ServerConfigFactory.fromJson(file);
        assertEquals(serverConfig.getConnectionString(), "localhost:2182,localhost:2183,localhost:2184");
        assertEquals(serverConfig.getConnectionTimeout(), 5000);
        assertEquals(serverConfig.getDTCRoot(), "dtc");
        assertEquals(serverConfig.getMasterLockPath(), "master");
        assertEquals(serverConfig.getServerId(), "master1");
        assertEquals(serverConfig.getSlaveRoot(), "slave");
        assertEquals(serverConfig.getTaskRoot(),"tasks");
    }
}
