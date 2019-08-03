package cn.com.zlg.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Date;

//3、fromElements(T …) - 从给定的对象序列中创建数据流。所有对象类型必须相同。
public class DataSourceFromElementsCollection implements Serializable {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Event> stream = env.fromElements(new Event("click", new Date(), 1), new Event("dbclick", new Date(), 2), new Event("hover", new Date(), 3));
        stream.print();
        env.execute();

    }

    public static class Event {
        private String name;
        private Date date;
        private Integer type;

        Event(String name, Date date, Integer type) {
            this.name = name;
            this.date = date;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        public Integer getType() {
            return type;
        }

        public void setType(Integer type) {
            this.type = type;
        }
    }

}
