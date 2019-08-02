package cn.com.zlg.util;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ESBuilder {
    public static ElasticsearchSink.Builder esBuilder() {
        List<HttpHost> esHttphost = new ArrayList<>();
        esHttphost.add(new HttpHost("127.0.0.1", 9200, "http"));
        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple2<String, Integer>>(
                esHttphost,
                new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {


                    public IndexRequest createIndexRequest(Tuple2<String, Integer> element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element.f0);
                        json.put("number", element.f1.toString());

                        return Requests.indexRequest()
                                .index("index-student")
                                .type("_doc")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple2<String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                });
        return esSinkBuilder;
    }
}
