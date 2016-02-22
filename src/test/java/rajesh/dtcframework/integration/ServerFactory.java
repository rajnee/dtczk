package rajesh.dtcframework.integration;

import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.config.json.ServerConfigFactory;
import rajesh.dtcframework.server.MasterServer;

import java.io.File;
import java.net.URL;

/**
 * Created by rajesh on 2/21/16.
 */
public class ServerFactory {

    public static ServerConfig getServerConfig(String fileName) throws Exception {
        URL url = System.class.getResource("/integration1/" + fileName);
        File file = new File(url.getFile());
        return ServerConfigFactory.fromJson(file);
    }
}
