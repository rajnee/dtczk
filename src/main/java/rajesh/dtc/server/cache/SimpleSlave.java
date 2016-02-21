package rajesh.dtc.server.cache;

import rajesh.dtc.server.Slave;
import rajesh.dtc.server.Task;

import java.util.List;

/**
 * Created by rajesh on 2/20/16.
 */
public class SimpleSlave implements Slave {

    private final String id;
    private  final boolean slave;

    public SimpleSlave(String id, boolean slave) {
        this.id = id;
        this.slave = slave;
    }
    @Override
    public boolean assignTask(Task t) {
        return false;
    }

    @Override
    public List<Task> getPending() {
        return null;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isSlave() {
        return slave;
    }
}
