package rajesh.dtcframework.integration;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rajesh.dtcframework.BaseDTCTest;
import rajesh.dtcframework.config.ServerConfig;
import rajesh.dtcframework.config.json.JsonServerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rajesh on 2/21/16.
 */
public class Integration1Test extends BaseDTCTest {

    List<SampleSlave> sampleSlaves;
    List<SampleMaster> sampleMasters;
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    Logger logger = LoggerFactory.getLogger(Integration1Test.class);

    @Override
    protected String getConfigFileName() {
        return "master1.json";
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        ConsoleAppender consoleAppender = new ConsoleAppender();
//        consoleAppender.setThreshold(Level.WARN);
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        consoleAppender.setLayout(new PatternLayout(PATTERN));
        consoleAppender.setThreshold(Level.ERROR);
        consoleAppender.activateOptions();
        org.apache.log4j.Logger.getRootLogger().addAppender(consoleAppender);
        org.apache.log4j.Logger.getLogger("rajesh").setLevel(Level.DEBUG);
        sampleMasters = new ArrayList<>();
        sampleSlaves = new ArrayList<>();

        //Create the masters
        SampleMaster sampleMaster;
        SampleSlave sampleSlave;

        JsonServerConfig serverConfig;
        serverConfig = ServerFactory.getServerConfig("master1.json");
        serverConfig.connection_string = testingServer.getConnectString();
        sampleMaster = new SampleMaster(serverConfig);
        sampleMasters.add(sampleMaster);

//        serverConfig = ServerFactory.getServerConfig("master2.json");
//        serverConfig.connection_string = testingServer.getConnectString();
//        sampleMaster = new SampleMaster(serverConfig);
//        sampleMasters.add(sampleMaster);

        serverConfig = ServerFactory.getServerConfig("slave1.json");
        serverConfig.connection_string = testingServer.getConnectString();
        sampleSlave = new SampleSlave(serverConfig);
        sampleSlaves.add(sampleSlave);

        serverConfig = ServerFactory.getServerConfig("slave2.json");
        serverConfig.connection_string = testingServer.getConnectString();
        sampleSlave = new SampleSlave(serverConfig);
        sampleSlaves.add(sampleSlave);
    }

    @Test
    public void testRunServers() throws Exception {


        //Start masters
        for (int i = 0; i < sampleMasters.size(); i++) {
            logger.info("Running master");
            SampleMaster sampleMaster = sampleMasters.get(i);
            executorService.submit(() -> {
                try {
                    logger.info("Starting master");
                    sampleMaster.start();
                    logger.info("master done");
                } catch (Exception e) {
                    logger.error("master done with exception", e);
                }
            });
        }

        for (int i = 0; i < sampleSlaves.size(); i++) {
            logger.info("Running slave");
            SampleSlave sampleSlave = sampleSlaves.get(i);
            executorService.submit(() -> {
                try {
                    logger.info("Starting slave");
                    sampleSlave.start();
                    logger.info("slave done");
                } catch (Exception e) {
                    logger.error("slave done with exception", e);
                }
            });
        }

        while(true) {
            synchronized (this) {
                if (Config.processedTasksCounter.get() >= Config.TOTAL_RECORDS) break;
                this.wait(1000);
            }
        }

        for (SampleMaster master: sampleMasters) {
            master.stop();
        }

        for (SampleSlave slave: sampleSlaves) {
            slave.stop();
        }
    }
}
