package rajesh.dtc.server;

/**
 * Created by rajesh on 2/18/16.
 */
public class SlaveServer extends BaseServer {

    protected boolean becomeSlave() {
        return false;
    }

    @Override
    protected void join() {
        if(becomeSlave()) {

        } else {

        }
    }

    @Override
    protected void process() throws Exception {

    }
}
