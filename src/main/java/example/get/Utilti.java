package example.get;

import org.json.JSONObject;
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

public class Utilti {
    private static final Logger logger = LoggerFactory.getLogger(Utilti.class);
    public JSONObject getOne(String entityId, Connection connection, Map<String, JSONObject> deviceCache) throws SQLException {
        JSONObject cachedDevice = deviceCache.get(entityId);
        if (cachedDevice != null) {
            return cachedDevice;
        }

        String query = "SELECT alias, sn FROM device WHERE id = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, entityId);
            try (ResultSet deviceRes = preparedStatement.executeQuery()) {
                if (deviceRes.next()) { // 确保有结果
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("name", deviceRes.getString("alias"));
                    jsonObject.put("number", deviceRes.getString("sn"));
                    deviceCache.put(entityId, jsonObject); // 存入缓存
                    return jsonObject;
                }
            }
        }
        logger.warn("Device with id {} not found in database.", entityId);
        return null;
    }

    public ResultSet getTwo(Long ts, String entityId,int key,Connection connection) throws SQLException {
        String query = "SELECT long_v ,dbl_v ,`key`  FROM ts_kv where entity_id = ? and ts = ? and `key` = ? ";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setString(1,entityId);
        preparedStatement.setLong(2,ts);
        preparedStatement.setInt(3,key);
        return preparedStatement.executeQuery();
    }
    public  void addFlowDataToJson(ResultSet resultSet, JSONObject jsonObject, String key) throws SQLException {
        if (resultSet.next()) {
            double flowValue = resultSet.getDouble("dbl_v");
            if (!resultSet.wasNull()) {
                jsonObject.put(key, flowValue);
            } else {
                long flowValueLong = resultSet.getLong("long_v");
                if (!resultSet.wasNull()) {
                    jsonObject.put(key, flowValueLong);
                }
            }
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