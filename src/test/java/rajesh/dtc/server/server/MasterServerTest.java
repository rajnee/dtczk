package rajesh.dtc.server.server;

import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import rajesh.dtc.server.BaseDTCTest;
import rajesh.dtc.server.Task;
import rajesh.dtc.server.config.ServerConfig;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by rajesh on 2/21/16.
 */
public class MasterServerTest extends BaseDTCTest {

    static class XServer extends  MasterServer {
        AtomicBoolean joinCalled = new AtomicBoolean();
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
            try {
                super.join();
            } catch (Exception e) {
                serverReadyLatch.countDown();
                e.printStackTrace();
                throw e;
            }
            serverReadyLatch.countDown();
            System.out.println("Master server has joined the cluster: master lock = " + isHavingMasterLock());
        }

        @Override
        protected void process() throws Exception {
            super.process();
            System.out.println("Process has been called on Master, returning");
            latch.await();
        }

        @Override
        protected void schedule() {
            System.out.println("Schedule called, doing nothing");
        }
    }

    @Override
    protected String getConfigFileName() {
        return "testconfig1.json";
    }

    XServer xServer;

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
            }
        });

        serverReadyLatch.await();

        String slavePath = serverConfig.getTaskRootPath() + "/slave1";
        curatorFramework.create().forPath(slavePath);

        assertTrue(xServer.isHavingMasterLock());

        countDownLatch.countDown();
        xServer.stop();
        assertEquals(true, xServer.joinCalled.get());

    }
}
