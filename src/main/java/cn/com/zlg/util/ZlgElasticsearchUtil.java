package cn.com.zlg.util;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ZlgElasticsearchUtil {

    public IndexRequest addSource(String index, String id,Object source) {
        IndexRequest request = Requests.indexRequest().index(index).type("_doc");
        if(id  != null && !id.trim().equals(""))
            request.id(id);
        request.source(JSON.toJSON(source),XContentType.JSON);
        return request;
    }

    /**
     * 获取记录信息
     *
     * @param index
     * @param id
     */
    public IndexRequest searchSource(String index, String id) {
        IndexRequest response = Requests.indexRequest(index).type("_doc").id(id);
        return response;
    }


    /**
     * 更新记录信息
     *
     * @param index
     * @param id
     * @param source
     */
    public IndexRequest updateSource(String index, String id,Object source) {
        IndexRequest response = Requests.indexRequest(index).type("_doc").id(id).opType(DocWriteRequest.OpType.UPDATE).source( JSON.toJSONString(source),XContentType.JSON);
        return response;
    }

    /**
     * 删除记录
     *
     * @param index
     * @param id
     * @throws IOException
     */
    public IndexRequest deleteSource(String index, String id) throws IOException {
        IndexRequest response = Requests.indexRequest(index).type("_doc").id(id).opType(DocWriteRequest.OpType.DELETE);
        return response;
    }

    /**
     * 删除索引
     *
     * @param index
     * @throws IOException
     */
    public IndexRequest deleteIndex(String index) throws IOException {
        IndexRequest response = Requests.indexRequest(index).opType(DocWriteRequest.OpType.DELETE);
        return response;
    }


}
