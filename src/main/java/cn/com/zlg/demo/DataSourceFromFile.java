package cn.com.zlg.demo;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.Serializable;

//readFile(fileInputFormat, path) - 根据指定的文件输入格式读取文件（一次）。
public class DataSourceFromFile implements Serializable {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        FileInputFormat format = new FileFormat();
        DataStream stream = env.readFile(format,"/Users/zhaoxin/idea-workspace/bigdata/flink-study/src/main/resources/data.txt");
        stream.print();
        env.execute();

    }

    public static class FileFormat extends FileInputFormat{

        @Override
        public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
            return null;
        }

        @Override
        public void open(InputSplit inputSplit) throws IOException {

        }

        @Override
        public boolean reachedEnd() throws IOException {
            return false;
        }

        @Override
        public Object nextRecord(Object o) throws IOException {
            return null;
        }
    }
}
