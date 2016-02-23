package rajesh.dtcframework.integration;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rajesh on 2/22/16.
 */
public class Config {

    public static final int TOTAL_RECORDS = 100;
    public static final AtomicInteger processedTasksCounter = new AtomicInteger();
    public static final int PAUSE_BETWEEN_TASK_SCHEDULE = 1000;
}
