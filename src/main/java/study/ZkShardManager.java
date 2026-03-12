package study;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ZK管理器：负责分片分配、心跳检测
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

    /**
     * 回调接口
     */
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

        // 初始化ZK客户端
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(zkConnect, retryPolicy);
        zkClient.start();
        if (!zkClient.blockUntilConnected(30, TimeUnit.SECONDS)) {
            throw new RuntimeException("ZK连接失败");
        }
        log.info("ZK连接成功, serverId: {}", zkServerId);

        // 创建基础路径
        createIfNotExists(zkBasePath, "flume root");
        createIfNotExists(zkBasePath + "/shards", "shards");
        createIfNotExists(zkBasePath + "/nodes", "agents");
        createIfNotExists(zkBasePath + "/lock", "lock");
        createIfNotExists(zkBasePath + "/status", "status");

        // 初始化分片节点
        for (int i = 0; i < numShards; i++) {
            String shardPath = zkBasePath + "/shards/shard_" + i;
            if (zkClient.checkExists().forPath(shardPath) == null) {
                JSONObject initial = new JSONObject();
                initial.put("shardId", i);
                initial.put("deviceIds", new JSONArray());
                initial.put("lastTs", 0L);
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
        zkClient.create().creatingParentContainersIfNeeded()
                .withProtection()
                .forPath(agentPath, info.toString().getBytes());
        log.info("注册节点: {}", agentPath);
    }

    /**
     * 初始化分片设备列表（全量设备分配）
     */
    public void initializeShards(List<String> allDevices) throws Exception {
        List<List<String>> shardDevices = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            shardDevices.add(new ArrayList<>());
        }
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
            shardInfo.put("deviceIds", deviceArray);   // 统一字段名
            zkClient.setData().forPath(shardPath, shardInfo.toJSONString().getBytes());
            log.info("分片 {} 分配设备: {}", i, shardDevices.get(i));
        }
    }

    /**
     * 认领分片（仅在启动或重平衡时调用，心跳任务也会自动认领）
     */
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

                        // 分配给自己
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
     * 更新本节点所有分片的心跳（无需锁，只更新自己持有的）
     */
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

    /**
     * 检查并重新认领超时分片
     */
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

                        // 如果超时分片原本属于本节点，则从本地移除并回调
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

    /**
     * 启动心跳任务
     */
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

    /**
     * 更新分片处理进度
     */
    public void updateShardLastTs(int shardId, long lastTs) throws Exception {
        String path = zkBasePath + "/shards/shard_" + shardId;
        byte[] data = zkClient.getData().forPath(path);
        JSONObject shardInfo = JSON.parseObject(new String(data));
        shardInfo.put("lastTs", lastTs);
        shardInfo.put("lastHeartbeat", System.currentTimeMillis());
        zkClient.setData().forPath(path, shardInfo.toJSONString().getBytes());

        // 更新全局进度
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

    /**
     * 释放所有分片（停止时调用）
     */
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

    /**
     * 关闭资源
     */
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

    /**
     * 本地分片信息缓存
     */
    private static class ShardInfo {
        int shardId;
        List<String> deviceIds;
        long lastTs;
        volatile boolean active;
        long lastHeartbeat;

        ShardInfo(int shardId, List<String> deviceIds, long lastTs, boolean active) {
            this.shardId = shardId;
            this.deviceIds = deviceIds;
            this.lastTs = lastTs;
            this.active = active;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }
}