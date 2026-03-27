package study;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * ZK管理器：负责分片分配、心跳检测，设备级进度只存 ts 和 key
 */
public class ZkShardManager {
    private static final Logger log = LoggerFactory.getLogger(ZkShardManager.class);
    private final CuratorFramework zkClient;
    private final String zkBasePath;
    private final String zkServerId;
    private final int numShards;
    private final long heartbeatIntervalMs;
    private final long shardTimeoutMs;
    private final ShardAssignmentCallback callback;
    private final InterProcessMutex globalLock;
    private final ConcurrentHashMap<Integer, ShardInfo> ownerShards = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public interface ShardAssignmentCallback {
        void onShardAssigned(int shardId, List<String> deviceIds, long lastTs);
        void onShardUnassigned(int shardId);
    }

    public ZkShardManager(String zkConnect, String zkBasePath, int numShards,
                          long heartbeatInterval, long shardTimeout,
                          ShardAssignmentCallback callback) throws Exception {
        this.zkBasePath = zkBasePath;
        this.numShards = numShards;
        this.heartbeatIntervalMs = heartbeatInterval * 1000;
        this.shardTimeoutMs = shardTimeout * 1000;
        this.callback = callback;
        this.zkServerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + System.currentTimeMillis();

        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(zkConnect, retryPolicy);
        zkClient.start();
        if (!zkClient.blockUntilConnected(30, TimeUnit.SECONDS)) {
            throw new RuntimeException("ZK连接失败");
        }
        log.info("ZK连接成功, serverId: {}", zkServerId);

        createIfNotExists(zkBasePath, "flume root");
        createIfNotExists(zkBasePath + "/shards", "shards");
        createIfNotExists(zkBasePath + "/nodes", "agents");
        createIfNotExists(zkBasePath + "/lock", "lock");
        createIfNotExists(zkBasePath + "/status", "status");

        for (int i = 0; i < numShards; i++) {
            String shardPath = zkBasePath + "/shards/shard_" + i;
            if (zkClient.checkExists().forPath(shardPath) == null) {
                JSONObject initial = new JSONObject();
                initial.put("shardId", i);
                initial.put("deviceIds", new JSONArray());
                initial.put("lastTs", 0L);
                initial.put("deviceProgress", new JSONObject());
                initial.put("status", "pending");
                initial.put("assignedTo", "");
                initial.put("lastHeartbeat", 0L);
                zkClient.create().creatingParentsIfNeeded()
                        .forPath(shardPath, initial.toJSONString().getBytes());
            }
        }

        globalLock = new InterProcessMutex(zkClient, zkBasePath + "/lock");
        registerAgent();
        startHeartbeatTask();
        startProgressPersistTask();
        log.info("ZkShardManager初始化完成");
    }

    private void createIfNotExists(String path, String data) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
        }
    }

    private void registerAgent() throws Exception {
        String agentPath = zkBasePath + "/nodes/" + zkServerId;
        JSONObject info = new JSONObject();
        info.put("host", zkServerId);
        info.put("startTime", System.currentTimeMillis());
        info.put("status", "running");
        zkClient.create().creatingParentsIfNeeded()
                .withProtection()
                .forPath(agentPath, info.toString().getBytes());
        log.info("注册节点: {}", agentPath);
    }

    public void initializeShards(List<String> allDevices) throws Exception {
        List<List<String>> shardDevices = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) shardDevices.add(new ArrayList<>());
        for (String deviceId : allDevices) {
            int shardId = Math.abs(deviceId.hashCode()) % numShards;
            shardDevices.get(shardId).add(deviceId);
        }
        for (int i = 0; i < numShards; i++) {
            String shardPath = zkBasePath + "/shards/shard_" + i;
            byte[] data = zkClient.getData().forPath(shardPath);
            JSONObject shardInfo = JSON.parseObject(new String(data));
            JSONArray deviceArray = new JSONArray();
            deviceArray.addAll(shardDevices.get(i));
            shardInfo.put("deviceIds", deviceArray);
            // 重置分片状态，保证可认领
            shardInfo.put("status", "pending");
            shardInfo.put("assignedTo", "");
            shardInfo.put("lastHeartbeat", 0L);
            zkClient.setData().forPath(shardPath, shardInfo.toJSONString().getBytes());
            log.info("分片 {} 分配设备: {}, status reset to pending", i, shardDevices.get(i));
        }
    }

    public void claimShard() throws Exception {
        if (globalLock.acquire(5, TimeUnit.SECONDS)) {
            try {
                String shardsPath = zkBasePath + "/shards";
                List<String> shardChildren = zkClient.getChildren().forPath(shardsPath);
                for (String child : shardChildren) {
                    String shardPath = shardsPath + "/" + child;
                    byte[] data = zkClient.getData().forPath(shardPath);
                    JSONObject shardInfo = JSON.parseObject(new String(data));
                    String status = shardInfo.getString("status");
                    String assignedTo = shardInfo.getString("assignedTo");

                    if ("pending".equals(status) ||
                            ("assigned".equals(status) && (assignedTo == null || assignedTo.isEmpty()))) {
                        int shardId = shardInfo.getIntValue("shardId");
                        if (ownerShards.containsKey(shardId)) continue;

                        shardInfo.put("status", "assigned");
                        shardInfo.put("assignedTo", zkServerId);
                        shardInfo.put("lastHeartbeat", System.currentTimeMillis());
                        zkClient.setData().forPath(shardPath, shardInfo.toJSONString().getBytes());

                        JSONArray deviceArray = shardInfo.getJSONArray("deviceIds");
                        List<String> deviceIds = new ArrayList<>();
                        for (int i = 0; i < deviceArray.size(); i++) {
                            deviceIds.add(deviceArray.getString(i));
                        }
                        long lastTs = shardInfo.getLongValue("lastTs");
                        ShardInfo local = new ShardInfo(shardId, deviceIds, lastTs, true);
                        local.deviceProgress = loadDeviceProgress(shardId);
                        ownerShards.put(shardId, local);
                        if (callback != null) {
                            callback.onShardAssigned(shardId, deviceIds, lastTs);
                        }
                        log.info("认领分片 {}，设备数: {}, lastTs: {}", shardId, deviceIds.size(), lastTs);
                    }
                }
            } finally {
                globalLock.release();
            }
        } else {
            log.warn("获取全局锁失败，无法认领分片");
        }
    }

    /**
     * 更新内存中某个设备的最后处理信息 (基于 ts 和 key 的比较)
     */
    public void updateDeviceProgress(int shardId, String deviceId, long ts, int key) {
        ShardInfo shard = ownerShards.get(shardId);
        if (shard != null && shard.active) {
            DeviceProgress dp = shard.deviceProgress.computeIfAbsent(deviceId, k -> new DeviceProgress());
            if (isNewer(ts, key, dp.getLastTs(), dp.getLastKey())) {
                dp.setLastTs(ts);
                dp.setLastKey(key);
            }
            if (ts > shard.lastTs) {
                shard.lastTs = ts;
            }
        }
    }

    /**
     * 比较 (ts, key) 的新旧，新记录 > 旧记录
     */
    private boolean isNewer(long newTs, int newKey, long oldTs, int oldKey) {
        if (newTs > oldTs) return true;
        if (newTs < oldTs) return false;
        return newKey > oldKey; // 假设 key 越大越新
    }

    /**
     * 获取设备进度对象
     */
    public DeviceProgress getDeviceProgress(int shardId, String deviceId) {
        ShardInfo shard = ownerShards.get(shardId);
        if (shard != null) {
            return shard.deviceProgress.get(deviceId);
        }
        return null;
    }

    /**
     * 持久化指定分片的设备进度到 ZK
     */
    public void persistShardProgress(int shardId) {
        ShardInfo shard = ownerShards.get(shardId);
        if (shard == null) return;

        String shardPath = zkBasePath + "/shards/shard_" + shardId;
        try {
            if (globalLock.acquire(5, TimeUnit.SECONDS)) {
                try {
                    byte[] data = zkClient.getData().forPath(shardPath);
                    JSONObject shardInfo = JSON.parseObject(new String(data));

                    JSONObject progressJson = new JSONObject();
                    for (Map.Entry<String, DeviceProgress> entry : shard.deviceProgress.entrySet()) {
                        JSONObject obj = new JSONObject();
                        obj.put("lastTs", entry.getValue().getLastTs());
                        obj.put("lastKey", entry.getValue().getLastKey());
                        progressJson.put(entry.getKey(), obj);
                    }
                    shardInfo.put("deviceProgress", progressJson);
                    shardInfo.put("lastTs", shard.lastTs);
                    shardInfo.put("lastHeartbeat", System.currentTimeMillis());

                    zkClient.setData().forPath(shardPath, shardInfo.toJSONString().getBytes());
                    log.debug("分片 {} 设备进度已持久化", shardId);
                } finally {
                    globalLock.release();
                }
            } else {
                log.warn("获取全局锁失败，无法持久化分片 {} 进度", shardId);
            }
        } catch (Exception e) {
            log.error("持久化分片 {} 进度时发生异常", shardId, e);
        }
    }

    public void updateHeartbeats() throws Exception {
        for (ShardInfo shard : ownerShards.values()) {
            String shardPath = zkBasePath + "/shards/shard_" + shard.shardId;
            byte[] data = zkClient.getData().forPath(shardPath);
            JSONObject shardInfo = JSON.parseObject(new String(data));
            shardInfo.put("lastHeartbeat", System.currentTimeMillis());
            shardInfo.put("assignedTo", zkServerId);
            shardInfo.put("status", "assigned");
            zkClient.setData().forPath(shardPath, shardInfo.toJSONString().getBytes());
            shard.lastHeartbeat = System.currentTimeMillis();
        }
        log.debug("已更新 {} 个分片的心跳", ownerShards.size());
    }

    private void checkAndReclaim() throws Exception {
        if (globalLock.acquire(5, TimeUnit.SECONDS)) {
            try {
                long now = System.currentTimeMillis();
                String shardsPath = zkBasePath + "/shards";
                List<String> shardIds = zkClient.getChildren().forPath(shardsPath);

                updateHeartbeats();

                for (String shardIdStr : shardIds) {
                    String shardPath = shardsPath + "/" + shardIdStr;
                    byte[] data = zkClient.getData().forPath(shardPath);
                    JSONObject shardInfo = JSON.parseObject(new String(data));
                    long lastHeartbeat = shardInfo.getLongValue("lastHeartbeat");
                    String assignedTo = shardInfo.getString("assignedTo");
                    String status = shardInfo.getString("status");

                    if ("assigned".equals(status) && (now - lastHeartbeat) > shardTimeoutMs) {
                        log.info("分片 {} 心跳超时 (最后心跳: {}), 重置为pending", shardIdStr, lastHeartbeat);
                        shardInfo.put("status", "pending");
                        shardInfo.put("assignedTo", "");
                        zkClient.setData().forPath(shardPath, shardInfo.toJSONString().getBytes());

                        if (assignedTo != null && assignedTo.equals(zkServerId)) {
                            int shardId = Integer.parseInt(shardIdStr.replace("shard_", ""));
                            ShardInfo removed = ownerShards.remove(shardId);
                            if (removed != null) {
                                removed.active = false;
                                if (callback != null) {
                                    callback.onShardUnassigned(shardId);
                                }
                                log.info("移除超时分片: {}", shardId);
                            }
                        }
                    }
                }
                claimShard();
            } finally {
                globalLock.release();
            }
        } else {
            log.warn("获取全局锁失败，跳过检查超时");
        }
    }

    private void startHeartbeatTask() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndReclaim();
            } catch (Exception e) {
                log.error("心跳任务执行异常", e);
            }
        }, heartbeatIntervalMs / 2, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        log.info("心跳任务已启动，间隔 {} ms", heartbeatIntervalMs);
    }

    private void startProgressPersistTask() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                for (Integer shardId : ownerShards.keySet()) {
                    persistShardProgress(shardId);
                }
            } catch (Exception e) {
                log.error("持久化进度任务异常", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
        log.info("进度持久化任务已启动，间隔10秒");
    }

    @Deprecated
    public void updateShardLastTs(int shardId, long lastTs) throws Exception {
        String path = zkBasePath + "/shards/shard_" + shardId;
        byte[] data = zkClient.getData().forPath(path);
        JSONObject shardInfo = JSON.parseObject(new String(data));
        shardInfo.put("lastTs", lastTs);
        shardInfo.put("lastHeartbeat", System.currentTimeMillis());
        zkClient.setData().forPath(path, shardInfo.toJSONString().getBytes());

        String tsPath = zkBasePath + "/status/processed_ts";
        if (zkClient.checkExists().forPath(tsPath) == null) {
            zkClient.create().forPath(tsPath, String.valueOf(lastTs).getBytes());
        } else {
            byte[] tsData = zkClient.getData().forPath(tsPath);
            long current = Long.parseLong(new String(tsData));
            if (lastTs > current) {
                zkClient.setData().forPath(tsPath, String.valueOf(lastTs).getBytes());
            }
        }
    }

    public void releaseAllShards() throws Exception {
        for (ShardInfo shard : ownerShards.values()) {
            String shardPath = zkBasePath + "/shards/shard_" + shard.shardId;
            byte[] data = zkClient.getData().forPath(shardPath);
            JSONObject shardInfo = JSON.parseObject(new String(data));
            shardInfo.put("status", "pending");
            shardInfo.put("assignedTo", "");
            zkClient.setData().forPath(shardPath, shardInfo.toJSONString().getBytes());
            log.info("释放分片 {}", shard.shardId);
        }
        ownerShards.clear();
    }

    private Map<String, DeviceProgress> loadDeviceProgress(int shardId) throws Exception {
        String path = zkBasePath + "/shards/shard_" + shardId;
        byte[] data = zkClient.getData().forPath(path);
        JSONObject shardInfo = JSON.parseObject(new String(data));
        JSONObject progressJson = shardInfo.getJSONObject("deviceProgress");
        Map<String, DeviceProgress> map = new ConcurrentHashMap<>();
        if (progressJson != null) {
            for (String deviceId : progressJson.keySet()) {
                JSONObject obj = progressJson.getJSONObject(deviceId);
                long lastTs = obj.getLongValue("lastTs");
                int lastKey = obj.getIntValue("lastKey");
                map.put(deviceId, new DeviceProgress(lastTs, lastKey));
            }
        }
        return map;
    }

    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (zkClient != null) {
            zkClient.close();
        }
    }

    private static class ShardInfo {
        int shardId;
        List<String> deviceIds;
        long lastTs;
        volatile boolean active;
        long lastHeartbeat;
        Map<String, DeviceProgress> deviceProgress;

        ShardInfo(int shardId, List<String> deviceIds, long lastTs, boolean active) {
            this.shardId = shardId;
            this.deviceIds = deviceIds;
            this.lastTs = lastTs;
            this.active = active;
            this.lastHeartbeat = System.currentTimeMillis();
            this.deviceProgress = new ConcurrentHashMap<>();
        }
    }

    @Getter
    @Setter
    public static class DeviceProgress {
        private long lastTs;
        private int lastKey;

        public DeviceProgress() {}

        public DeviceProgress(long lastTs, int lastKey) {
            this.lastTs = lastTs;
            this.lastKey = lastKey;
        }
    }
}