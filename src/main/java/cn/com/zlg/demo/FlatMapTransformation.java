package cn.com.zlg.demo;

import cn.com.zlg.datasource.ZlgMySQLDataSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//FlatMap 采用一条记录并输出零个，一个或多个记录
public class FlatMapTransformation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator op = env.addSource(new ZlgMySQLDataSource()).setParallelism(1).flatMap(new FlatMapFunction<TUser, TUser>() {
            @Override
            public void flatMap(TUser tUser, Collector<TUser> collector) throws Exception {
                if(tUser.getId() % 2 == 0)
                    collector.collect(tUser);
            }
        });
        op.print();
        env.execute();
    }

}
