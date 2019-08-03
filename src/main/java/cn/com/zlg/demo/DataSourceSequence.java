package cn.com.zlg.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

//5、generateSequence(from, to) - 创建一个生成指定区间范围内的数字序列的并行数据流。
public class DataSourceSequence implements Serializable {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream stream = env.generateSequence(1,10);
        stream.print();
        env.execute();

    }



}
