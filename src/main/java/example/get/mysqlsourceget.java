package example.get;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.json.JSONObject;

public class mysqlsourceget extends AbstractSource implements Configurable, PollableSource {
    private static final Logger logger = LoggerFactory.getLogger(mysqlsourceget.class);
    private static final int MAX_RETRY_COUNT = 3;
    private static final long RETRY_INTERVAL_SECONDS = 10;
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private String url;
    private String user;
    private String password;
    private long sleepInterval = 2000L;
    private String tableName;
    private int pageSize = 500;
    private int pageNumber = 1;
    private Utilti db = new Utilti();
    private volatile boolean running = true;
    private Map<String, JSONObject> deviceCache = new ConcurrentHashMap<>();
    private ExecutorService executorService;

    @Override
    public void start() {
        super.start();
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this::pollData);
    }

    @Override
    public void stop() {
        super.stop();
        running = false;
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Executor service did not terminate in the specified time.");
                }
            } catch (InterruptedException e) {
                logger.warn("Executor service termination interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void pollData() {
        int retryCount = 0;
        while (running) {
            Connection connect = null;
            try {
                connect = DriverManager.getConnection(url, user, password);
                String query = "SELECT entity_id, ts, `key`FROM " + tableName + " where `key` =37 and ts_kv.entity_id ='0b4abea0-a596-11ef-861b-25d52ec0a193' LIMIT ? OFFSET ?";
                try (PreparedStatement statement = connect.prepareStatement(query)) {
                    statement.setInt(1, pageSize);
                    statement.setInt(2, (pageNumber - 1) * pageSize);
                    try (ResultSet rs = statement.executeQuery()) {
                        boolean hasMoreData = false;
                        while (rs.next()) {
                            hasMoreData = true;
                            String entityId = rs.getString("entity_id");
                            int currentKey = rs.getInt("key");
                            if (currentKey != 37) {
                                break;
                            }
                            JSONObject deviceInfo = db.getOne(entityId, connect, deviceCache);
                            if (deviceInfo == null) {
                                logger.warn("Skipping entity_id due to missing device info: {}", entityId);
                                continue;
                            }
                            // 处理并创建事件
                            JSONObject jsonObject = new JSONObject(deviceInfo.toString());
                            jsonObject.put("tk_id", entityId);
                            jsonObject.put("ts", rs.getLong("ts"));
                            db.addFlowDataToJson(db.getTwo(rs.getLong("ts"), entityId,37, connect), jsonObject, "instantFlow");
                            db.addFlowDataToJson(db.getTwo(rs.getLong("ts"), entityId,39, connect), jsonObject, "totalFlow");
                            db.TsChange(rs.getLong("ts"), jsonObject);
                            Event event = new SimpleEvent();
                            event.setBody(jsonObject.toString().getBytes(StandardCharsets.UTF_8));
                            queue.put(event);
                        }
                        if (!hasMoreData) {
                            logger.info("No more data found.");
                        }
                        if (hasMoreData) {
                            pageNumber++;
                        } else {
                            pageNumber = 1; // 重置页码，等待新数据
                        }
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(sleepInterval);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // 恢复中断状态
                        logger.warn("Polling thread interrupted during sleep", e);
                        break;
                    }
                }
                retryCount = 0; // 成功后重置重试计数器
            } catch (SQLException | InterruptedException e) {
                logger.error("SQL Exception occurred while polling data", e);
                retryCount++;
                if (retryCount > MAX_RETRY_COUNT) {
                    logger.error("Max retry count reached. Stopping polling.");
                    running = false;
                } else {
                    try {
                        TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECONDS);
                    } catch (InterruptedException retryInterruptedException) {
                        Thread.currentThread().interrupt();
                        logger.warn("Retry sleep interrupted", retryInterruptedException);
                    }
                }
            } finally {
                if (connect != null) {
                    try {
                        connect.close();
                    } catch (SQLException e) {
                        logger.error("Failed to close database connection", e);
                    }
                }
            }
        }
    }


    @Override
            public Status process () throws EventDeliveryException {
                Event event = queue.poll();
                if (event == null) {
                    return Status.BACKOFF;
                } else {
                    getChannelProcessor().processEvent(event);
                    return Status.READY;
                }
            }

            @Override
            public long getBackOffSleepIncrement () {
                return 0L;
            }

            @Override
            public long getMaxBackOffSleepInterval () {
                return 0L;
            }

            @Override
            public synchronized void configure (Context context){
                url = context.getString("url");
                user = context.getString("user");
                password = context.getString("password");
                sleepInterval = context.getLong("sleepInterval", 2000L);
                tableName = context.getString("tableName");
                String pageSizeStr = context.getString("pageSize");
                if (pageSizeStr != null) {
                    try {
                        pageSize = Integer.parseInt(pageSizeStr);
                        logger.info("Configured pageSize: {}", pageSize);
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid pageSize configuration, using default: {}", pageSize);
                    }
                } else {
                    logger.info("Using default pageSize: {}", pageSize);
                }
            }
        }
