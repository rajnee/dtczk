package rajesh.dtc.server.server;

import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import rajesh.dtc.server.BaseDTCTest;
import rajesh.dtc.server.config.ServerConfig;

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
        public XServer(ServerConfig serverConfig, CountDownLatch countDownLatch) {
            super(serverConfig);
            this.latch = countDownLatch;
        }

        @Override
        protected void join() throws Exception {
            joinCalled.set(true);
        }

        @Override
        protected void process() throws Exception {
            processCalled.incrementAndGet();
            latch.await();
        }
    }

    XServer xServer;

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    public void testSimple() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        xServer = new XServer(serverConfig, countDownLatch);
        executorService.submit(() -> {
            try {
                xServer.start();
            } catch (Exception e) {
            }
        });

        Stat stat = curatorFramework.checkExists().forPath(serverConfig.getRootPath());
        assertNotNull("Root path is null", stat);

        stat = curatorFramework.checkExists().forPath(serverConfig.getSlaveRootPath());
        assertNotNull("Slave Root path is null", stat);

        stat = curatorFramework.checkExists().forPath(serverConfig.getTaskRootPath());
        assertNotNull("Task Root path is null", stat);

        countDownLatch.countDown();
        xServer.stop();
        assertEquals(true, xServer.joinCalled.get());

    }
}
