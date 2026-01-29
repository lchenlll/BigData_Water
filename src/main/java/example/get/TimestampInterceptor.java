package example.get;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TimestampInterceptor implements Interceptor {
    private String timestampHeader;

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        try {
            String body = new String(event.getBody(), StandardCharsets.UTF_8);
            JSONObject jsonObject = new JSONObject(body);
            long ts = jsonObject.getLong("ts");
            Date date = new Date(ts);
            SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd");
            String formattedDste = sdf.format(date);
            event.getHeaders().put("timestamp", formattedDste);
            String tk_id=jsonObject.getString("tk_id");
            event.getHeaders().put("tk_id",tk_id);
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
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {
            // Read configuration parameters if any
        }
    }
}
