package study;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.*;
import study.Util;


public class MysqlSource_get extends AbstractSource {
    private static final Logger log = LoggerFactory.getLogger(MysqlSource_get.class);
    private static final long RETRY_INTERVAL_SECONDS = 10;
    private ConnectData connectData;
    private ExecutorService executorService;
    private final Map<String, JSONObject> deviceCache = new ConcurrentHashMap<>();
    private int pageNumber = 1;
    private boolean hasMoreData = false;
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private volatile boolean running = false;
    private static final long sleepInterval = 2000L;
    private Util util = new Util();

    @Override
    public synchronized void start() {
        super.start();
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this::PullData);

    }
    /**
     * 数据处理功能  LIMIT ? OFFSET ?
     */
    private void PullData() {
        int retry = 0;
        while(!running){
            try (Connection connect = connectData.SqlgetConnection()) {
                String query = "SELECT entity_id, ts, `key`,dbl_v ,long_v FROM " + connectData.getTableName() + " where `key` =37  or `key` = 39 LIMIT ? OFFSET ?";
                try(PreparedStatement preparedStatement = connect.prepareStatement(query)) {
                    preparedStatement.setInt(1, connectData.getPageSize());
                    preparedStatement.setInt(2, (pageNumber - 1) * connectData.getPageSize());
                    try (ResultSet resultSet = preparedStatement.executeQuery()){
                        hasMoreData = false;
                        while (resultSet.next()) {
                            processRow(connect, resultSet);
                        }
                        if(!hasMoreData){
                            log.info("No more data found.");
                            pageNumber = 1;
                        }else {
                            pageNumber++;
                        }
                    }
                    try {
                        TimeUnit.SECONDS.sleep(sleepInterval);
                    } catch (InterruptedException e) {
                        log.warn("Interrupted while sleeping.");
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                retry = 0;
            }
            catch (SQLException e) {
                log.error("SQL Exception occurred while polling data", e);
                retry++;
                if(retry > 3){
                    log.error("Max retry count reached. Stopping polling.");
                    running = false;
                }
                else {
                    try {
                        TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECONDS);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        log.warn("Interrupted while sleeping.");
                    }
                }
            }
        }
    }
    private void processRow(Connection connect, ResultSet resultSet) throws SQLException{
        String entityId = resultSet.getString("entity_id");
        int key = resultSet.getInt("key");
        long ts = resultSet.getLong("ts");
        JSONObject deviceInfo = util.GetdeviceInfo(entityId, connect, deviceCache);
        JSONObject jsonObject = new JSONObject(deviceInfo);
        jsonObject.put("tk_id", entityId);
        jsonObject.put("ts", ts);
        if (key == 37) {
            if (resultSet.getObject("long_v") != null) {
                jsonObject.put("instantFlow", resultSet.getLong("long_v"));
            } else {
                jsonObject.put("instantFlow", resultSet.getDouble("dbl_v"));
            }
        } else if (key == 39) {
            if (resultSet.getObject("dbl_v") != null) {
                jsonObject.put("totalFlow", resultSet.getLong("dbl_v"));
            } else {
                jsonObject.put("totalFlow", resultSet.getDouble("long_v"));
            }
        }
        util.TsChange(ts,jsonObject);
        Event event = new SimpleEvent();
        event.setBody(jsonObject.toString().getBytes(StandardCharsets.UTF_8));
        try {
            queue.put(event);
        } catch (InterruptedException e) {
           log.warn("Interrupted while putting event into queue.");
           Thread.currentThread().interrupt();
        }
    }

    public PollableSource.Status process () throws EventDeliveryException {
        Event event = queue.poll();
        if (event == null) {
            return PollableSource.Status.BACKOFF;
        } else {
            getChannelProcessor().processEvent(event);
            return PollableSource.Status.READY;
        }
    }

    public long getBackOffSleepIncrement ()  {
        return 0L;
    }

    public long getMaxBackOffSleepInterval () {
        return 0L;
    }
    @Override
    public synchronized void stop() {
        super.stop();
        running = false;
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Executor service did not terminate in the specified time.");
                }
            } catch (InterruptedException e) {
                log.warn("Executor service termination interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
    }

}
