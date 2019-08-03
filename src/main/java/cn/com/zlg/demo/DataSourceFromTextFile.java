package cn.com.zlg.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

//readTextFile(path) - 读取文本文件，即符合 TextInputFormat 规范的文件，并将其作为字符串返回。
public class DataSourceFromTextFile implements Serializable {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream stream = env.readTextFile("/Users/zhaoxin/idea-workspace/bigdata/flink-study/src/main/resources/data.txt");
        stream.print();
        env.execute();

    }
}
