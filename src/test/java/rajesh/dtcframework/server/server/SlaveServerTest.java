package rajesh.dtcframework.server.server;

import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;
import rajesh.dtcframework.server.BaseDTCTest;
import rajesh.dtcframework.server.Task;
import rajesh.dtcframework.server.config.ServerConfig;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by rajesh on 2/21/16.
 */
public class SlaveServerTest extends BaseDTCTest {

    static class XServer extends  SlaveServer {
        CountDownLatch releaseLatch;
        CountDownLatch serverReadyLatch, tasksReadyLatch, processCompletedLatch;

        AtomicBoolean joinCalled = new AtomicBoolean();

        public XServer(ServerConfig serverConfig,
                       CountDownLatch releaseLatch,
                       CountDownLatch serverReadyLatch,
                       CountDownLatch tasksReadyLatch,
                       CountDownLatch processCompletedLatch) {
            super(serverConfig);
            this.releaseLatch = releaseLatch;
            this.serverReadyLatch = serverReadyLatch;
            this.tasksReadyLatch = tasksReadyLatch;
            this.processCompletedLatch = processCompletedLatch;
        }

        @Override
        protected void join() throws Exception {
            joinCalled.set(true);
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
            serverReadyLatch.countDown();
            System.out.println("XServer process, waiting for tasks to be ready");
            tasksReadyLatch.await();
            System.out.println("XServer process, ready to process");
            super.process();
            System.out.println("XServer process, completed");
            processCompletedLatch.countDown();
            System.out.println("XServer process, awaiting release");
            releaseLatch.await();
        }

        @Override
        protected void executeTask(Task t) throws Exception{
            System.out.println("Executing tasks in slave, doing nothing");
            System.out.println("Task is:" + t.getId() + ", data=" + new String(t.getData()));
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

    @Test
    public void testSimple() throws Exception {
        CountDownLatch releaseLatch = new CountDownLatch(1);
        CountDownLatch serverReadyLatch = new CountDownLatch(2);
        CountDownLatch tasksReadyLatch = new CountDownLatch(1);
        CountDownLatch processCompletedLatch = new CountDownLatch(1);

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        xServer = new XServer(serverConfig,
                releaseLatch,
                serverReadyLatch,
                tasksReadyLatch,
                processCompletedLatch
        );
        executorService.submit(() -> {
            try {
                xServer.start();
            } catch (Exception e) {
            }
        });


        serverReadyLatch.await();
        //Add some tasks
        curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(xServer.getSlaveServerTaskPath() + "/task1", "100".getBytes());
        curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(xServer.getSlaveServerTaskPath() + "/task2", "200".getBytes());
        curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(xServer.getSlaveServerTaskPath() + "/task3", "300".getBytes());

        tasksReadyLatch.countDown();

        List<String> children = curatorFramework.getChildren().forPath(serverConfig.getSlaveRootPath());
        assertEquals(1, children.size());

        processCompletedLatch.await();
        children = curatorFramework.getChildren().forPath(xServer.getSlaveServerTaskPath());
        assertEquals(0, children.size());


        xServer.stop();
        releaseLatch.countDown();
        assertEquals(true, xServer.joinCalled.get());

    }
}
