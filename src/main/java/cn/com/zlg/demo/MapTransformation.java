package cn.com.zlg.demo;

import cn.com.zlg.datasource.ZlgMySQLDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//数据转换的各种操作，有 Map / FlatMap / Filter / KeyBy / Reduce / Fold / Aggregations / Window / WindowAll / Union / Window join / Split / Select / Project 等，操作很多，可以将数据转换计算成你想要的数据。
public class MapTransformation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator op = env.addSource(new ZlgMySQLDataSource()).setParallelism(1).map(new MapFunction<TUser, Object>() {

            //这是最简单的转换之一，其中输入是一个数据流，输出的也是一个数据流
            @Override
            public Object map(TUser tUser) throws Exception {
                TUser user = new TUser();
                user.setId(tUser.getId());
                user.setUsername("hello " + tUser.getUsername());
                user.setPassword(tUser.getPassword());
                user.setEmail(tUser.getEmail());
                user.setRole(tUser.getRole() + 1);
                return user;
            }
        });
        op.print();
        env.execute();
    }

}
