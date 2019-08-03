package cn.com.zlg.datasource;

import cn.com.zlg.demo.TUser;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.ResultSet;

public class ZlgMySQLDataSource extends RichSourceFunction<TUser> {

    private transient DruidDataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("src/main/resources/application.properties");
        //建立druid线程池
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(parameterTool.get("mysql.driverClassName"));
        dataSource.setUsername(parameterTool.get("mysql.username"));
        dataSource.setPassword(parameterTool.get("mysql.password"));
        dataSource.setUrl(parameterTool.get("mysql.url"));
        dataSource.setInitialSize(Integer.valueOf(parameterTool.get("mysql.initialSize")));
        dataSource.setMinIdle(Integer.valueOf(parameterTool.get("mysql.minIdle")));
        dataSource.setMaxActive(Integer.valueOf(parameterTool.get("mysql.maxActive")));
    }


    @Override
    public void close() throws Exception {
        super.close();
        if (dataSource != null)
            dataSource.close();
    }

    @Override
    public void run(SourceContext<TUser> sourceContext) throws Exception {
        ResultSet result = dataSource.getConnection().prepareStatement("select * from t_user").executeQuery();
        while (result.next()) {
            TUser user = new TUser(
                    result.getLong("id"),
                    result.getString("username").trim(),
                    result.getString("password").trim(),
                    result.getString("email").trim(),
                    Integer.valueOf(result.getInt("role")));
            sourceContext.collect(user);
        }
    }

    @Override
    public void cancel() {

    }
}
