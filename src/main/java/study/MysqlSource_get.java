package study;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class MysqlSource_get extends AbstractSource implements PollableSource, Configurable {
    private static final Logger log = LoggerFactory.getLogger(MysqlSource_get.class);

    private SourceConfig sourceConfig;
    private ZkProgressManager zkProgressManager;
    private ExecutorService executorService;
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private final Map<String, JSONObject> deviceCache = new ConcurrentHashMap<>();
    private volatile boolean running = false;
    private final Util util = new Util();

    private List<String> allDeviceIds;
    // 每个设备的进度（内存缓存）
    private final Map<String, DeviceProgress> deviceProgressMap = new ConcurrentHashMap<>();

    private static class DeviceProgress {
        long lastTs;
        int lastKey;
        DeviceProgress(long lastTs, int lastKey) {
            this.lastTs = lastTs;
            this.lastKey = lastKey;
        }
    }

    @Override
    public void configure(Context context) {
        sourceConfig = new SourceConfig();
        sourceConfig.configure(context);
    }

    @Override
    public synchronized void start() {
        super.start();
        running = true;
        try {
            zkProgressManager = new ZkProgressManager(
                    sourceConfig.getZkConnect(),
                    sourceConfig.getZkBasePath()
            );

            allDeviceIds = loadAllDeviceIds();
            if (allDeviceIds.isEmpty()) {
                log.warn("No devices found, will not pull any data.");
                return;
            }
            log.info("Loaded {} devices", allDeviceIds.size());

            for (String deviceId : allDeviceIds) {
                ZkProgressManager.Process progress = zkProgressManager.loadProgress(deviceId);
                deviceProgressMap.put(deviceId, new DeviceProgress(progress.getLastTs(), progress.getLastKey()));
                log.debug("Loaded progress for device {}: lastTs={}, lastKey={}", deviceId, progress.getLastTs(), progress.getLastKey());
            }

            executorService = Executors.newSingleThreadExecutor();
            executorService.submit(this::pullDataLoop);
            log.info("MysqlSource_get started");
        } catch (Exception e) {
            log.error("MysqlSource_get failed to start", e);
            running = false;
            throw new RuntimeException(e);
        }
    }
    private List<String> loadAllDeviceIds() {
        List<String> deviceIds = new ArrayList<>();
        String sql = "SELECT DISTINCT entity_id FROM " + sourceConfig.getTableName();
        try (Connection conn = sourceConfig.SqlgetConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                deviceIds.add(rs.getString(1));
            }
            log.info("Loaded {} device IDs", deviceIds.size());
        } catch (SQLException e) {
            log.error("Failed to load device IDs", e);
        }
        return deviceIds;
    }

    private void pullDataLoop() {
        long loopInterval = sourceConfig.getSleepInterval();
        while (running) {
            try {
                for (String deviceId : allDeviceIds) {
                    if (!running) break;
                    try {
                        fetchDataForDevice(deviceId);
                    } catch (Exception e) {
                        log.error("Error fetching data for device {}", deviceId, e);
                    }
                }
                Thread.sleep(loopInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in pullDataLoop", e);
            }
        }
    }

    private void fetchDataForDevice(String deviceId) throws Exception {
        DeviceProgress progress = deviceProgressMap.get(deviceId);
        if (progress == null) return;
        log.info("Fetching data for device: {}", deviceId);

        long lastTs = progress.lastTs;
        int lastKey = progress.lastKey;

        String tableName = sourceConfig.getTableName();
        int pageSize = sourceConfig.getPageSize();

        String sql = "SELECT entity_id, ts, key, dbl_v, long_v FROM " + tableName
                + " WHERE entity_id = CAST(? AS uuid) AND (ts > ? OR (ts = ? AND key > ?)) AND key IN (37,39) "
                + " ORDER BY ts ASC, key ASC LIMIT ?";

        try (Connection conn = sourceConfig.SqlgetConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, deviceId);
            pstmt.setLong(2, lastTs);
            pstmt.setLong(3, lastTs);
            pstmt.setInt(4, lastKey);
            pstmt.setInt(5, pageSize);

            long maxTs = lastTs;
            int LastKey = lastKey;

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    long ts = rs.getLong("ts");
                    int key = rs.getInt("key");
                    if (isNewer(ts, key, maxTs, LastKey)) {
                        maxTs = ts;
                        LastKey = key;
                    }
                    processRow(conn, rs);
                }
            }

            // 如果该设备有新数据，更新内存和ZK
            if (maxTs > lastTs || (maxTs == lastTs && LastKey != lastKey)) {
                progress.lastTs = maxTs;
                progress.lastKey = LastKey;
                // 持久化到 ZK
                zkProgressManager.saveProgress(maxTs, deviceId, LastKey);
                log.debug("Device {} progress updated to lastTs={}, lastKey={}", deviceId, maxTs, LastKey);
            }
        }
    }

    private boolean isNewer(long ts, int key, long maxTs, int maxKey) {
        if (ts > maxTs) return true;
        if (ts < maxTs) return false;
        return key > maxKey;
    }

    private void processRow(Connection conn, ResultSet rs) throws SQLException {
        String entityId = rs.getString("entity_id");
        int key = rs.getInt("key");
        long ts = rs.getLong("ts");

        JSONObject deviceInfo = util.GetdeviceInfo(entityId, conn, deviceCache);
        JSONObject jsonObject = new JSONObject(deviceInfo);
        jsonObject.put("tk_id", entityId);
        jsonObject.put("ts", ts);
        jsonObject.put("key", key);

        if (key == 37) {
            if (rs.getObject("long_v") != null) {
                jsonObject.put("instantFlow", rs.getLong("long_v"));
            } else {
                jsonObject.put("instantFlow", rs.getDouble("dbl_v"));
            }
        } else if (key == 39) {
            if (rs.getObject("dbl_v") != null) {
                jsonObject.put("totalFlow", rs.getLong("dbl_v"));
            } else {
                jsonObject.put("totalFlow", rs.getDouble("long_v"));
            }
        }

        util.TsChange(ts, jsonObject);

        Event event = new SimpleEvent();
        event.setBody(jsonObject.toString().getBytes(StandardCharsets.UTF_8));
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            log.warn("Interrupted while putting event into queue");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Event event = queue.poll();
        if (event == null) {
            return Status.BACKOFF;
        } else {
            getChannelProcessor().processEvent(event);
            return Status.READY;
        }
    }

    @Override
    public long getBackOffSleepIncrement() { return 0; }

    @Override
    public long getMaxBackOffSleepInterval() { return 0; }

    @Override
    public synchronized void stop() {
        running = false;
        if (zkProgressManager != null) {
            try {
                zkProgressManager.close();
            } catch (Exception e) {
                log.error("Error closing ZkProgressManager", e);
            }
        }
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Executor service did not terminate in time");
                }
            } catch (InterruptedException e) {
                log.warn("Executor service termination interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
        super.stop();
        log.info("MysqlSource_get stopped");
    }
}