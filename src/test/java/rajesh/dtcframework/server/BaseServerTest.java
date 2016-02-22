package rajesh.dtcframework.server;

import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.BaseDTCTest;
import rajesh.dtcframework.Task;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;

/**
 * Created by rajesh on 2/21/16.
 */
public class BaseServerTest extends BaseDTCTest {


    static class XServer extends  BaseServer {
        AtomicBoolean joinCalled = new AtomicBoolean();
        AtomicInteger processCalled = new AtomicInteger();
        CountDownLatch latch;
        CountDownLatch serverReadyLatch;
        public XServer(ServerConfig serverConfig, CountDownLatch countDownLatch, CountDownLatch serverReadyLatch) {
            super(serverConfig);
            this.latch = countDownLatch;
            this.serverReadyLatch = serverReadyLatch;
        }

        @Override
        protected void join() throws Exception {
            joinCalled.set(true);
            serverReadyLatch.countDown();
        }

        @Override
        protected void process() throws Exception {
            processCalled.incrementAndGet();
            latch.await();
        }
    }

    XServer xServer;

    @Override
    protected String getConfigFileName() {
        return "testconfig1.json";
    }

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    public void testSimple() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountDownLatch serverReadyLatch = new CountDownLatch(1);

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        xServer = new XServer(serverConfig, countDownLatch, serverReadyLatch);
        executorService.submit(() -> {
            try {
                xServer.start();
            } catch (Exception e) {
                System.out.println("Error starting base server");
                e.printStackTrace();
                return;
            }
        });

        serverReadyLatch.await();
        Stat stat = curatorFramework.checkExists().forPath(serverConfig.getRootPath());
        assertNotNull("Root path is null", stat);

        stat = curatorFramework.checkExists().forPath(serverConfig.getSlaveRootPath());
        assertNotNull("Slave Root path is null", stat);

        stat = curatorFramework.checkExists().forPath(serverConfig.getTaskRootPath());
        assertNotNull("Task Root path is null", stat);

        String slavePath = serverConfig.getTaskRootPath() + "/slave1";
        curatorFramework.create().forPath(slavePath);
        curatorFramework.create().forPath(slavePath + "/task1", "task1".getBytes());
        curatorFramework.create().forPath(slavePath + "/task2", "task1".getBytes());
        curatorFramework.create().forPath(slavePath + "/task3", "task1".getBytes());
        curatorFramework.create().forPath(slavePath + "/task4", "task1".getBytes());


        List<Task> taskList = xServer.tasksNode.getTasks("slave1");
        byte[] data = taskList.get(0).getData();
        assertEquals(new String(data), "task1");
        assertEquals(4, taskList.size());

        countDownLatch.countDown();
        xServer.stop();
        assertEquals(true, xServer.joinCalled.get());

    }
}
