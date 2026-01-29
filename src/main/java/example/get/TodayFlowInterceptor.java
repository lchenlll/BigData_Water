package example.get;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TodayFlowInterceptor implements Interceptor {
    private Map<String, Double> todayFlowMap = new HashMap<>();

    @Override
    public void initialize() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            todayFlowMap.clear();
        }, getInitialDelay(), 24, TimeUnit.HOURS);
    }

    private long getInitialDelay() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextMidnight = now.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        return Duration.between(now, nextMidnight).getSeconds();
    }
    @Override
    public Event intercept(Event event) {
        try {
            String body = new String(event.getBody(), StandardCharsets.UTF_8);
            JSONObject jsonObject = new JSONObject(body);
            String tk_id = jsonObject.getString("tk_id");
            double instantFlow = jsonObject.getDouble("instantFlow");
            String collectDate = jsonObject.getString("collect_date");
            String key = tk_id + "_" + collectDate;
            // 累加今日流量
            todayFlowMap.put(key, todayFlowMap.getOrDefault(key, 0.0) + instantFlow);
            // 将todayFlow添加到事件头中
            event.getHeaders().put("todayFlow", String.valueOf(todayFlowMap.get(key)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // Clean up any resources used by the interceptor
    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TodayFlowInterceptor();
        }

        @Override
        public void configure(Context context) {
            // Read configuration parameters if any
        }
    }
}
