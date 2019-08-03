package cn.com.zlg.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

//2、fromCollection(Iterator, Class) - 从一个迭代器中创建数据流。Class 指定了该迭代器返回元素的类型。
public class DataSourceFromIteratorCollection implements Serializable {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Element> list = new ArrayList<Element>();
        list.add(new Element("zx","北京"));
        list.add(new Element("seven","辽宁"));
        Iterator outs = list.iterator();
        DataStream<String> stream = env.fromCollection(outs, String.class);
        stream.print();
        env.execute();

    }

    public static class Element implements Serializable {
        private static final long serializId = 1L;

        private String name;
        private String address;

        Element(String name,String address){
            this.name = name;
            this.address = address;
        }
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }

}
