package study;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        try {
            String body = new String(event.getBody(), StandardCharsets.UTF_8);
            JSONObject jsonObject = new JSONObject(body);
            long ts = jsonObject.getLong("ts");
            String entityId = jsonObject.getString("entityId");
            Date date = new Date(ts);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String formattedDste = sdf.format(date);
            event.getHeaders().put("timestamp", formattedDste);
            event.getHeaders().put("tk_id",entityId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for(Event event:list){
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {
    }
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
