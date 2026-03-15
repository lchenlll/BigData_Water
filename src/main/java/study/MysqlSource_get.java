package study;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MysqlSource_get extends AbstractSource implements PollableSource, Configurable {
    private static final Logger log = LoggerFactory.getLogger(MysqlSource_get.class);
    private static final long RETRY_INTERVAL_SECONDS = 10;

    private SourceConfig sourceConfig;
    private ZkShardManager zkShardManager;
    private ExecutorService executorService;
    private final Map<String, JSONObject> deviceCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ShardContext> shardContexts = new ConcurrentHashMap<>();
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private volatile boolean running = false;
    private final Util util = new Util();

    @Override
    public void configure(Context context) {
        sourceConfig = new SourceConfig();
        sourceConfig.configure(context);
    }

    private static class ShardContext {
        final int shardId;
        final List<String> deviceIds;
        volatile long lastTs;
        volatile boolean active;

        ShardContext(int shardId, List<String> deviceIds, long lastTs) {
            this.shardId = shardId;
            this.deviceIds = Collections.unmodifiableList(deviceIds);
            this.lastTs = lastTs;
            this.active = true;
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        running = true;
        try {
            zkShardManager = new ZkShardManager(
                    sourceConfig.getZkConnect(),
                    sourceConfig.getZkBasePath(),
                    sourceConfig.getNumShards(),
                    sourceConfig.getHeartBeatInterval(),
                    sourceConfig.getShardTimeout(),
                    new ShardAssignmentCallbackImpl()
            );

            // 重要：初始化设备列表（从数据库查询或硬编码）
            List<String> allDevices = loadAllDeviceIds();
            if (allDevices.isEmpty()) {
                log.warn("No devices found, will not pull any data.");
            } else {
                zkShardManager.initializeShards(allDevices);
            }

            // 2. 认领分片（此时 ZK 中已有设备列表）
            log.info("Before claimShard");
            zkShardManager.claimShard();
            log.info("After claimShard, shardContexts size: {}", shardContexts.size());

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

    private List<String> getAllDeviceIds() {
        List<String> deviceIds = new ArrayList<>();
        String sql = "SELECT DISTINCT id FROM device";
        try (Connection conn = sourceConfig.SqlgetConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                deviceIds.add(rs.getString(1));
            }
        } catch (SQLException e) {
            log.error("Failed to load device IDs", e);
        }
        return deviceIds;
    }

    /**
     * 数据拉取主循环：遍历所有活跃分片，每个分片内遍历设备，增量拉取数据
     */
    private void pullDataLoop() {
        log.info("shardContexts size: {}, devices: {}", shardContexts.size(),
                shardContexts.values().stream().map(ctx -> ctx.deviceIds.size()).collect(Collectors.toList()));
        long loopInterval = sourceConfig.getSleepInterval();
        while (running) {
            try {
                List<ShardContext> snapshot = new ArrayList<>(shardContexts.values());
                if (snapshot.isEmpty()) {
                    Thread.sleep(loopInterval);
                    continue;
                }

                for (ShardContext ctx : snapshot) {
                    if (!ctx.active || !running) continue;
                    try {
                        fetchDataForShard(ctx);
                    } catch (Exception e) {
                        log.error("Error fetching data for shard " + ctx.shardId, e);
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

    /**
     * 为单个分片拉取数据：遍历分片内所有设备，对每个设备执行增量查询
     */
    private void fetchDataForShard(ShardContext ctx) throws Exception {
        String tableName = sourceConfig.getTableName();
        int pageSize = sourceConfig.getPageSize();
        List<String> deviceIds = ctx.deviceIds;
        if (deviceIds.isEmpty()) return;

        for (String deviceId : deviceIds) {
            long deviceLastTs = zkShardManager.getDeviceLastTs(ctx.shardId, deviceId);

            String sql = "SELECT id, entity_id, ts, key, dbl_v, long_v FROM " + tableName
                    + " WHERE entity_id = ? AND ts > ? AND (key =37 or key = 39) ORDER BY ts ASC, id ASC LIMIT ?";

            try (Connection conn = sourceConfig.SqlgetConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setString(1, deviceId);
                pstmt.setLong(2, deviceLastTs);
                pstmt.setInt(3, pageSize);

                long maxTsForDevice = deviceLastTs;

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        long ts = rs.getLong("ts");
                        if (ts > maxTsForDevice) {
                            maxTsForDevice = ts;
                        }
                        processRow(conn, rs, ctx.shardId);
                    }
                }

                // 更新分片全局 lastTs（可选）
                if (maxTsForDevice > ctx.lastTs) {
                    ctx.lastTs = maxTsForDevice;
                }
            }
        }
    }

    /**
     * 处理单行数据，生成事件并更新设备进度
     */
    private void processRow(Connection conn, ResultSet rs, int shardId) throws SQLException {
        long id = rs.getLong("id");
        String entityId = rs.getString("entity_id");
        int key = rs.getInt("key");
        long ts = rs.getLong("ts");

        JSONObject deviceInfo = util.GetdeviceInfo(entityId, conn, deviceCache);
        JSONObject jsonObject = new JSONObject(deviceInfo);
        jsonObject.put("tk_id", entityId);
        jsonObject.put("ts", ts);
        jsonObject.put("key", key);
        jsonObject.put("recordId", id); // 可选，便于下游使用

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

        // 更新设备进度到 ZK 管理器（内存）
        zkShardManager.updateDeviceProgress(shardId, entityId, ts, key, String.valueOf(id));
    }

    // ZK 分片回调实现
    private class ShardAssignmentCallbackImpl implements ZkShardManager.ShardAssignmentCallback {
        @Override
        public void onShardAssigned(int shardId, List<String> deviceIds, long lastTs) {
            log.info("Shard assigned: {}, devices: {}, lastTs: {}", shardId, deviceIds.size(), lastTs);
            shardContexts.put(shardId, new ShardContext(shardId, deviceIds, lastTs));
        }

        @Override
        public void onShardUnassigned(int shardId) {
            log.info("Shard unassigned: {}", shardId);
            ShardContext ctx = shardContexts.get(shardId);
            if (ctx != null) {
                ctx.active = false;
                shardContexts.remove(shardId);
            }
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
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (zkShardManager != null) {
            try {
                zkShardManager.releaseAllShards();
                zkShardManager.close();
            } catch (Exception e) {
                log.error("Error closing ZkShardManager", e);
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