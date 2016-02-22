package rajesh.dtcframework.server;

import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;
import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.BaseDTCTest;
import rajesh.dtcframework.Task;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * Created by rajesh on 2/21/16.
 */
public class SlaveServer1Test extends BaseDTCTest {

    static class XServer extends  SlaveServer {
        CountDownLatch processCompletedLatch, serverReadyLatch;
        int taskCount;
        AtomicInteger tasksProcessed = new AtomicInteger();
        public XServer(ServerConfig serverConfig,
                       CountDownLatch processCompletedLatch,
                       CountDownLatch serverReadyLatch,
                       int taskCount) {
            super(serverConfig);
            this.processCompletedLatch = processCompletedLatch;
            this.serverReadyLatch = serverReadyLatch;
            this.taskCount = taskCount;
        }

        @Override
        protected void join() throws Exception {
            try {
                super.join();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            } finally {
                serverReadyLatch.countDown();
            }
            System.out.println("Slave has successfully joined in the test");
        }

        @Override
        protected void process() throws Exception {
            super.process();
        }

        @Override
        protected void executeTask(Task t) throws Exception{
            System.out.println("Executing Task is:" + t);
            int tc = tasksProcessed.incrementAndGet();
            if ( tc == taskCount) {
                processCompletedLatch.countDown();
            }
        }
    }

    @Override
    protected String getConfigFileName() {
        return "testslaveconfig1.json";
    }

    XServer xServer;

    @Before
    public void setup() throws Exception {
        super.setup();
    }


    private static final int TASKS_COUNT = 1000;

    @Test
    public void testSimple() throws Exception {
        CountDownLatch processCompletedLatch = new CountDownLatch(1);
        CountDownLatch serverReadyLatch = new CountDownLatch(1);

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        xServer = new XServer(serverConfig,
                processCompletedLatch,
                serverReadyLatch,
                TASKS_COUNT
        );

        executorService.submit(() -> {
            try {
                xServer.start();
            } catch (Exception e) {
            }
        });

        serverReadyLatch.await();

        //Add some tasks
        for (int i = 0; i < TASKS_COUNT; i++) {
            curatorFramework.create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(serverConfig.getTaskPath(serverConfig.getServerId(), "task" + i), "100".getBytes());
            Thread.currentThread().sleep(5);
        }


        List<String> children = curatorFramework.getChildren().forPath(serverConfig.getSlaveRootPath());
        assertEquals(1, children.size());

        processCompletedLatch.await();
        children = curatorFramework.getChildren().forPath(serverConfig.getTaskRootPath() + "/" + xServer.getServerId());
        assertEquals(0, children.size());
        xServer.stop();

    }
}
