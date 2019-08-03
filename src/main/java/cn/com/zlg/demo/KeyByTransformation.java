package cn.com.zlg.demo;

import cn.com.zlg.datasource.ZlgMySQLDataSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


//KeyBy 在逻辑上是基于 key 对流进行分区。在内部，它使用 hash 函数对流进行分区。它返回 KeyedDataStream 数据流
public class KeyByTransformation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<TUser, Integer> op = env.addSource(new ZlgMySQLDataSource()).setParallelism(1).keyBy(new KeySelector<TUser,Integer>(){

            //对 user 的 id 做 KeyBy 操作分区
            @Override
            public Integer getKey(TUser tUser) throws Exception {
                return tUser.getId().intValue();
            }
        });
        op.print();
        env.execute();
    }

}
