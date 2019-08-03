package cn.com.zlg.datasink;

import cn.com.zlg.demo.TUser;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledPreparedStatement;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class ZlgMySQLSinkFunction extends RichSinkFunction implements SinkFunction {

    private transient DruidDataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("application.properties");
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

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (dataSource != null)
            dataSource.close();
    }

    public void invoke(TUser value, Context context) throws Exception {
        DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) dataSource.getConnection().prepareStatement("insert into T_User(id,username, password,email, role) values(?,?, ?, ?,?)");
        //组装数据，执行插入操作
        ps.setLong(1, value.getId());
        ps.setString(2, value.getUsername());
        ps.setString(3, value.getPassword());
        ps.setString(4, value.getEmail());
        ps.setInt(5, value.getRole());
        ps.executeUpdate();
    }

}
