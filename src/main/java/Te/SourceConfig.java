package Te;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;


@Getter
@Setter
public class SourceConfig implements Configurable  {
    private static final Logger log = LoggerFactory.getLogger(SourceConfig.class);
    private String url;
    private String username;
    private String password;
    private long sleepInterval;
    private String  tableName;
    private Integer pageSize = 600;
    private String zkConnect;
    private String zkBasePath ;
    private  int numShards ;
    private long heartBeatInterval ;
    private long ShardTimeout ;

    private static DataSource dataSource;
    @Override
    public void configure(Context context) {
        url = context.getString("url");
        username = context.getString("user");
        password = context.getString("password");
        sleepInterval = context.getLong("sleepInterval");
        tableName = context.getString("tableName");
        String pageSizeBack = context.getString("pageSize");
        zkConnect = context.getString("zkConnect");
        zkBasePath = context.getString("zkBasePath");
        if(pageSizeBack!=null){
            try{
                pageSize = Integer.parseInt(pageSizeBack);
                log.info("Configured pageSize: {}", pageSize);
            }catch (NumberFormatException e){
                log.warn("Invalid pageSize configuration, using default: {}", pageSize);
            }
        }else {
            log.info("Using default pageSize: {}", pageSize);
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setIdleTimeout(30000);
        dataSource = new HikariDataSource(config);

    }
    public Connection SqlgetConnection() throws SQLException {
        try {
            return dataSource.getConnection();
        }catch (SQLException e){
            log.error("no",e);
            throw e;
        }
    }


}
