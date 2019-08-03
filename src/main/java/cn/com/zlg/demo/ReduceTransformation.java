package cn.com.zlg.demo;

import cn.com.zlg.datasource.ZlgMySQLDataSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


//Reduce 返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值。常用的方法有 average, sum, min, max, count，使用 reduce 方法都可实现
public class ReduceTransformation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //对 user 的 id 做 KeyBy 操作分区
        SingleOutputStreamOperator op = env.addSource(new ZlgMySQLDataSource()).keyBy((KeySelector<TUser, Integer>) tUser -> tUser.getId().intValue()).reduce((ReduceFunction<TUser>) (tUser, t1) -> {
            TUser user = new TUser();
            user.setId(tUser.getId() + t1.getId());
            user.setUsername(tUser.getUsername() + "/" + t1.getUsername());
            user.setPassword(tUser.getPassword() + t1.getPassword());
            user.setEmail(tUser.getEmail() + "|" + t1.getEmail());
            user.setRole((tUser.getRole() + t1.getRole()) / 2);
            return user;
        });
        op.print();
        //op.addSink(new ZlgMySQLSinkFunction());
        env.execute();
    }

}
