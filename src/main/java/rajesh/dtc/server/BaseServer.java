package rajesh.dtc.server;

/**
 * Created by rajesh on 2/18/16.
 */
public abstract class BaseServer {

    protected void establishZookeeperConnection() {

    }

    protected abstract void join();

    protected void loop() {
        while(true) {
            try {
                process();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected abstract void process() throws Exception;

    protected void start() throws Exception {
        join();
        loop();
    }
}
