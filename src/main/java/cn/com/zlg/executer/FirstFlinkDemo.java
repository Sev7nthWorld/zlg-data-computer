package cn.com.zlg.executer;

import cn.com.zlg.function.MyFlatMapFunction;
import cn.com.zlg.util.ESBuilder;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.util.Collector;


public class FirstFlinkDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("------------------------" + System.currentTimeMillis());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);

        SingleOutputStreamOperator input = env.readTextFile("C:\\Users\\zhaoxin\\idea-workspace\\ncloan\\zlg-data-computer\\src\\main\\resources\\input.txt").flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] str = s.toLowerCase().split("\\W+");
                for (String ss : str) {
                    collector.collect(new Tuple2<String, Integer>(ss, 1));
                }
            }
        }).filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                if (value.f0.equals("a"))
                    return false;
                else
                    return true;
            }
        }).keyBy(0).flatMap(new MyFlatMapFunction());
        /*.split(new OutputSelector<Tuple2<String, Integer>>() {
            @Override
            public Iterable<String> select(Tuple2<String, Integer> input) {
                List output = new ArrayList();
                if(input.f1 % 2 == 0)
                    output.add("even");
                else
                    output.add("odd");
                return output;
            }
        }).select("even","odd").addSink(ESBuilder.esBuilder().build()).setParallelism(2);*/

        ESBuilder.esBuilder().setBulkFlushMaxActions(1);
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders()
//                }
//        );
 //       esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl());
        ESBuilder.esBuilder().setFailureHandler(new RetryRejectedExecutionFailureHandler());



        input.print();

        env.execute();
    }
}
