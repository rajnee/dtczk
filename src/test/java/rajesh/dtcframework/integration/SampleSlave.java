package rajesh.dtcframework.integration;

import rajesh.dtcframework.Task;
import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.server.SlaveServer;

import java.util.concurrent.atomic.AtomicInteger;
import static rajesh.dtcframework.integration.Config.*;
/**
 * Created by rajesh on 2/22/16.
 */
public class SampleSlave extends SlaveServer {

    public SampleSlave(ServerConfig serverConfig) {
        super(serverConfig);
    }

    @Override
    protected void executeTask(Task t) throws Exception {
        System.out.println("Handling task:" + getServerId() + "," + t + ", [" + processedTasksCounter.incrementAndGet() + "]");
    }

    @Override
    protected void start() throws Exception {
        super.start();
    }
}
