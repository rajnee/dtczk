package rajesh.dtc.server.server;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import rajesh.dtc.server.Slave;
import rajesh.dtc.server.config.ServerConfig;
import rajesh.dtc.server.server.BaseServer;

import java.util.List;

/**
 * Created by rajesh on 2/18/16.
 */
public abstract class MasterServer extends BaseServer {

    private InterProcessMutex masterLock;

    protected boolean isHavingMasterLock() {
        return masterLock.isAcquiredInThisProcess();
    }

    public MasterServer(ServerConfig serverConfig) {
        super(serverConfig);
    }

    protected boolean becomeMaster() {
        masterLock = new InterProcessMutex(curatorFramework, serverConfig.getMasterLockPath());
        while (!masterLock.isAcquiredInThisProcess()) {
            try {
                //This should block and wait till it can acquire, a backup process will wait
                masterLock.acquire();
            } catch (Exception exception) {
                exception.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    protected void join() {
        becomeMaster();
    }

    @Override
    protected void process() throws Exception {
        if (isHavingMasterLock()) {
            List<Slave> slaveList = getSlaves();
            if ( slaveList != null && slaveList.size() > 0) {
                schedule();
            }
        }
    }

    protected abstract void schedule();
}
