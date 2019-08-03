package cn.com.zlg.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class DataSourceFromSocket {
    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        int port = Integer.valueOf(args[1]);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String,Integer>> sum = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);
        sum.print();
        env.execute("Java WordCount from SocketTextStream Example");

    }


    public final static class LineSplitter implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");

            for(String token:tokens){
                if(token.length() > 0){
                    collector.collect(new Tuple2<>(token,1));
                }
            }
        }
    }


}