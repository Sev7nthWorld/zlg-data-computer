package cn.com.zlg.datasink;

import cn.com.zlg.util.ZlgElasticsearchUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;


public class ZlgElasticsearchSinkFunction implements ElasticsearchSinkFunction {

    @Override
    public void process(Object o, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        ZlgElasticsearchUtil esUtil = new ZlgElasticsearchUtil();
        Tuple2 temp = (Tuple2)o;
        try {
            requestIndexer.add(esUtil.addSource("hello",null,temp));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
