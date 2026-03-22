package study;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class Util {
     private static final Logger log = LoggerFactory.getLogger(Util.class);
     /**
     * 获取设备信息
     * @param entityId 设备ID
     * @param connect 数据库连接
     * @param deviceCache 设备信息缓存
     * @return 设备信息JSON对象
     */
     public JSONObject GetdeviceInfo(String entityId, Connection connect, Map<String, JSONObject> deviceCache) {
         JSONObject deviceInfoStr = deviceCache.get(entityId);
         if (deviceInfoStr != null) {
             return deviceInfoStr;
         }
         // 修正 SQL，添加空格
         String query = "SELECT d.alias, d.sn, t.title " +
                 "FROM device AS d " +
                 "INNER JOIN tenant AS t ON d.tenant_id = t.id " +
                 "WHERE d.id = ?::uuid";
         try (PreparedStatement statement = connect.prepareStatement(query)) {
             statement.setString(1, entityId);
             try (ResultSet rs = statement.executeQuery()) {
                 if (rs.next()) {
                     deviceInfoStr = new JSONObject();
                     deviceInfoStr.put("alias", rs.getString("alias"));
                     deviceInfoStr.put("sn", rs.getString("sn"));
                     deviceInfoStr.put("title", rs.getString("title"));
                     deviceCache.put(entityId, deviceInfoStr);
                     return deviceInfoStr;
                 }
                 log.warn("Device with id {} not found in database.", entityId);
                 // 可选：缓存空对象，避免重复查询
                 JSONObject empty = new JSONObject();
                 deviceCache.put(entityId, empty);
                 return empty;
             }
         } catch (SQLException e) {
             log.error("Error querying device info for entityId: {}", entityId, e);
             throw new RuntimeException("Fail to get device info for entityId: " + entityId, e);
         }
     }
    public void TsChange(Long ts,JSONObject jsonObject){
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        String dateStr = dateTime.format(dateFormatter);
        String timeStr = dateTime.format(timeFormatter);
        jsonObject.put("collect_date", dateStr);
        jsonObject.put("collect_time", timeStr);
     }

}
