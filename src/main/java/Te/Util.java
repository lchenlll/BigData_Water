package Te;

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
     public JSONObject GetdeviceInfo(String entityId, Connection connect, Map<String, JSONObject> deviceCache) {
         JSONObject deviceInfoStr = deviceCache.get(entityId);
         if (deviceInfoStr != null) {
             return deviceInfoStr;
         }
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
