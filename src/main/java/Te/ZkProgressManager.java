package Te;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ZkProgressManager {
    private static final Logger log = LoggerFactory.getLogger(ZkProgressManager.class);
    private final CuratorFramework zkClient;
    private final String progressPath;

    public ZkProgressManager(String zkConnect, String zkBasePath) throws Exception {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkConnect)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        boolean connected = zkClient.blockUntilConnected(30, TimeUnit.SECONDS);
        if (!connected) {
            if (zkClient.getZookeeperClient().isConnected()) {
                log.info("ZK full");
            } else {
                throw new RuntimeException("ZK outTime");
            }
        } else {
            log.info("ZK ok");
        }

        createIfNotExists(zkBasePath);
        this.progressPath = zkBasePath + "/progress";
        createIfNotExists(progressPath);
    }

    private void createIfNotExists(String progressPath) throws Exception {
        if(zkClient.checkExists().forPath(progressPath)==null){
            zkClient.create().creatingParentsIfNeeded().forPath(progressPath);
        }
    }
    public void saveProgress(Long LastTs,String deviceId,int LastKey) throws Exception {
       String path = progressPath+"/"+deviceId;
        JSONObject data = new JSONObject();
        data.put("LastTs",LastTs);
        data.put("LastKey",LastKey);
        if(zkClient.checkExists().forPath(path)==null){
            zkClient.create().creatingParentsIfNeeded().forPath(path,data.toJSONString().getBytes());
        }else {
            zkClient.setData().forPath(path,data.toJSONString().getBytes());
        }
        log.info("ok：{}",data);
   }
   /*
     加载进度
     @param deviceId
    * @return
    */
    public Process loadProgress(String deviceId) throws Exception {
        String devicePath = progressPath+"/"+deviceId;
        if(zkClient.checkExists().forPath(devicePath)==null){
            return new Process(1732042108691L,37,"535d0120-a59c-11ef-861b-25d52ec0a193") ;
        }
        byte[] data = zkClient.getData().forPath(devicePath);
        JSONObject jsonObject = JSONObject.parseObject(new String(data));
        return new Process(jsonObject.getLong("LastTs"),jsonObject.getIntValue("LastKey"),deviceId);
    }
    @Getter
    public static class Process{
        private final Long LastTs;
        private final int LastKey;
        private final String deviceId;
        public Process(Long LastTs,int LastKey,String deviceId) {
            this.LastTs = LastTs;
            this.LastKey = LastKey;
            this.deviceId = deviceId;
        }
    }
    public void close() throws Exception {
        zkClient.close();
    }
}
