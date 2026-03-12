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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class MysqlSource_get extends AbstractSource implements PollableSource {
    private SourceConfig sourceconfig;
    private ZkShardManager zkShardManager;

    private static final Logger log = LoggerFactory.getLogger(MysqlSource_get.class);


    private static final long RETRY_INTERVAL_SECONDS = 10;
    private ExecutorService executorService;
    // 设备缓存
    private final Map<String, JSONObject> deviceCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer,ShardContext> shardContexts = new ConcurrentHashMap<>();
    private int pageNumber = 1;
    private boolean hasMoreData = false;
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private volatile boolean running = false;
    private static final long sleepInterval = 2000L;
    private final Util util = new Util();
    private static class ShardContext{
         final int shardId;
         final List<String> deviceIds;
        volatile long lastTs;
        volatile boolean active;

        private ShardContext(int shardId,List<String> deviceIds,long lastTs) {
            this.shardId = shardId;
            this.active = true;
            this.lastTs = System.currentTimeMillis();
            this.deviceIds = Collections.unmodifiableList(deviceIds);
        }
    }


    @Override
    public synchronized void start() {
        super.start();
        running = true;
        try{
             zkShardManager = new ZkShardManager(sourceconfig.getZkConnect(),
                     sourceconfig.getZkBasePath(),
                     sourceconfig.getNumShards(),
                     sourceconfig.getHeartBeatInterval(),
                     sourceconfig.getShardTimeout(), new ShardAssignmentCallbackImpl()

             );
             zkShardManager.claimShard();
            executorService = Executors.newSingleThreadExecutor();
            log.info("MysqlSource_get started");
            executorService.submit(this::PullData);
        } catch (Exception e) {
            log.info("MysqlSource_get failed to start",e);
            running = false;
            throw new RuntimeException(e);
        }

    }

    private void PullData() {
        long Interval = sourceconfig.getSleepInterval();
        while (running) {
            try{
                ArrayList<ShardContext> shardContexts1 = new ArrayList<>(shardContexts.values());
                if(shardContexts1.isEmpty()){
                    Thread.sleep(sleepInterval);
                    continue;
                }
                for (ShardContext ctx : shardContexts1){
                    if(!ctx.active||!running) continue;{
                        try{
                            fetchDataForShard(ctx);
                        } catch (Exception e) {
                            log.error("Error fetching data for shard " + ctx.shardId, e);
                        }
                    }
                    Thread.sleep(Interval);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }catch (Exception e){
                log.error("Error in PullData",e);
            }
        }

    }

    private void fetchDataForShard(ShardContext ctx) throws SQLException {
        String tableName = sourceconfig.getTableName();
        int pageSize = sourceconfig.getPageSize();
        List<String> deviceIds = ctx.deviceIds;
        if(deviceIds.isEmpty()) return;
        String placeholders = deviceIds.stream().map(id -> "?").collect(Collectors.joining(","));
        String query = "SELECT entity_id, ts, `key`,dbl_v ,long_v FROM " + tableName + "WHERE entity_id IN ("+placeholders+") AND ts > ? AND (`key` =37  or `key` = 39)" + " ORDER BY ts ASC LIMIT ?";
        long lastTs = ctx.lastTs;
        try(Connection connect = sourceconfig.SqlgetConnection();
        PreparedStatement preparedStatement = connect.prepareStatement(query)){
            int index = 1;
            for (String deviceId : deviceIds) {
                preparedStatement.setString(index++, deviceId);
            }
            preparedStatement.setLong(index++, lastTs);
            preparedStatement.setInt(index, pageSize);
            long maxTsInBatch = lastTs;
            boolean hasMoreData = false;
            try(ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()){
                    hasMoreData = true;
                    long ts = resultSet.getLong("ts");
                    if(ts > maxTsInBatch) maxTsInBatch = ts;
                    processRow(connect,resultSet);
                }
            }
            if(hasMoreData&&maxTsInBatch>lastTs){
                ctx.lastTs = maxTsInBatch;
                zkShardManager.updateShardLastTs(ctx.shardId,maxTsInBatch);
                log.debug("Shard {} lastTs updated to {}", ctx.shardId, maxTsInBatch);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //回调实现
    private  class ShardAssignmentCallbackImpl implements ZkShardManager.ShardAssignmentCallback {
        @Override
        public void onShardAssigned(int shardId, List<String> deviceIds, long lastTs) {
            log.info("Shard assigned: {}, devices: {}, lastTs: {}", shardId, deviceIds.size(), lastTs);
            shardContexts.put(shardId, new ShardContext(shardId, deviceIds, lastTs));

        }
        @Override
        public void onShardUnassigned(int shardId) {
            log.info("Shard unassigned: {}", shardId);
            ShardContext ctx = shardContexts.get(shardId);
            if(ctx !=null){
                ctx.active = false;
                shardContexts.remove(shardId);
            }

        }
    }
    /**
     * 数据处理功能  LIMIT ? OFFSET ?
     */
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
    @Override
    public Status process () throws EventDeliveryException {
        Event event = queue.poll();
        if (event == null) {
            return PollableSource.Status.BACKOFF;
        } else {
            getChannelProcessor().processEvent(event);
            return PollableSource.Status.READY;
        }
    }
   @Override
    public long getBackOffSleepIncrement ()  {
        return 0L;
    }

    @Override
    public long getMaxBackOffSleepInterval () {
        return 0L;
    }
    @Override
    public synchronized void stop() {
        running = false;
        if(zkShardManager != null){
            try {
                zkShardManager.releaseAllShards();
                zkShardManager.close();
            } catch (Exception e) {
                log.error("Error releasing shards", e);
            }
        }
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
        super.stop();
        log.info("MysqlSource_get stopped");
    }

}
