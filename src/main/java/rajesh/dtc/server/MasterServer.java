package rajesh.dtc.server;

/**
 * Created by rajesh on 2/18/16.
 */
public class MasterServer extends BaseServer {

    protected boolean becomeMaster() {
        return false;
    }

    @Override
    protected void join() {
        if (becomeMaster()) {

        } else {
            
        }
    }

    @Override
    protected void process() throws Exception {

    }
}
