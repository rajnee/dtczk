package rajesh.dtcframework.server;

import java.util.List;

/**
 * Created by rajesh on 2/20/16.
 */
public interface Slave extends Agent {
    boolean assignTask(Task t);
    List<Task> getPending();
}
