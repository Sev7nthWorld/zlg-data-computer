package cn.com.zlg.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//DataStream API 支持各种聚合，例如 min，max，sum,minBy,maxBy 等。 这些函数可以应用于 KeyedStream 以获得 Aggregations 聚合
//max 和 maxBy 之间的区别在于 max 返回流中的最大值，但 maxBy 返回具有最大值的键， min 和 minBy 同理(例子中没有看到什么效果)
public class AggregationsTransformation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2> stream = env.fromElements(new Tuple2(1,1),new Tuple2(2,1),new Tuple2(2,5),new Tuple2(1,2)).keyBy(0).sum(1);

        stream.print();
        env.execute();
    }

}
