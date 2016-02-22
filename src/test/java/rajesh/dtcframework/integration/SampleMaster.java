package rajesh.dtcframework.integration;

import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.server.MasterServer;

/**
 * Created by rajesh on 2/21/16.
 */
public class SampleMaster extends MasterServer {
    @Override
    protected void schedule() {
    }

    public SampleMaster(ServerConfig serverConfig) {
        super(serverConfig);
    }
}
