package rajesh.dtcframework.integration;

import rajesh.dtcframework.Slave;
import rajesh.dtcframework.Task;
import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.server.MasterServer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static rajesh.dtcframework.integration.Config.*;
/**
 * Created by rajesh on 2/21/16.
 */
public class SampleMaster extends MasterServer {
    AtomicInteger counter = new AtomicInteger();

    Object monitor = new Object();

    /* Very dumb round robin scheduling */
    @Override
    protected void schedule() throws Exception {
        try {
            List<Slave> slaves = getSlaves();
            for (Slave slave : slaves) {
                int tc = tasksNode.getNumAssignedTasks(slave.getId());
                if (tc < 10) {
                    for (int i = tc; i < 10; i++) {
                        int k = counter.incrementAndGet();
                        final String j = "task-" + k;
                        Task t = new Task() {
                            @Override
                            public String getId() {
                                return j;
                            }

                            @Override
                            public byte[] getData() throws Exception {
                                return new byte[0];
                            }
                        };

                        tasksNode.addTask(t, slave.getId());
                        if (k >= TOTAL_RECORDS){
                            System.out.println("Stopping server [" + getServerId() + "]");
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {

        }
        tasksNode.watch(monitor);
        synchronized (monitor) {
            monitor.wait(PAUSE_BETWEEN_TASK_SCHEDULE);
        }
    }

    public SampleMaster(ServerConfig serverConfig) {
        super(serverConfig);
    }

    @Override
    protected void start() throws Exception {
        super.start();
    }
}
