package cn.com.zlg.demo;

import cn.com.zlg.datasource.ZlgMySQLDataSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//Filter 函数根据条件判断出结果
public class FilterTransformation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator op = env.addSource(new ZlgMySQLDataSource()).setParallelism(1).filter(new FilterFunction<TUser>() {
            @Override
            public boolean filter(TUser tUser) throws Exception {
                if(tUser.getId() % 2 == 0)
                    return false;

                else
                    return true;
            }
        });
        op.print();
        env.execute();
    }

}
