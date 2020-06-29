package com.zhangbh.esapi;

import com.alibaba.fastjson.JSON;
import com.zhangbh.esapi.entity.User;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;

import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 创建索引
     */
    @Test
    public void testCreateIndex() throws IOException {
        //1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("zhangbh_index");
        //2.执行请求
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 测试获取索引，只能判断存不存在
     */
    @Test
    public void testExistsIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("zhangbh_index");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("zhangbh_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    /**
     * 测试添加文档
     */
    @Test
    public void testAddDocument() throws IOException {
        //创建对象
        User user = new User("张博华", 23);
        //创建请求
        IndexRequest request = new IndexRequest("zhangbh_index");
        //设置规则 put /index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        //将数据放入请求
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求，获取响应的结果
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);

        System.out.println(index.toString());
        System.out.println(index.status());
    }

    /**
     * 获取文档，判断是否存在
     */
    @Test
    public void testIsExists() throws IOException {
        GetRequest request = new GetRequest("zhangbh_index", "1");
        //不获取返回的 _source 的上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");

        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 获取文档的信息
     */
    @Test
    public void testGetInfo() throws IOException {
        GetRequest request = new GetRequest("zhangbh_index", "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //打印文档的内容
        System.out.println(response.getSourceAsString());
        //返回全部内容和命令是一样的
        System.out.println(response);
    }

    /**
     * 更新文档信息
     */
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("zhangbh_index", "1");
        request.timeout("1s");
        User user = new User("张博华Java", 3);
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update);
        System.out.println(update.status());
    }

    /**
     * 删除文档记录
     */
    @Test
    public void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("zhangbh_index", "1");
        request.timeout("1s");

        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());
        System.out.println(delete);
    }

    /**
     * 特殊的，项目一般都会使用批量插入
     */
    @Test
    public void testBatchRequest() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("10s");
        List<User> users = new ArrayList<>();
        users.add(new User("zhangbh01", 3));
        users.add(new User("zhangbh02", 3));
        users.add(new User("zhangbh03", 3));
        users.add(new User("zhangbh04", 3));
        users.add(new User("zhangbh05", 3));
        users.add(new User("liwy", 3));
        users.add(new User("liwy", 3));
        for (int i = 0; i < users.size(); i++) {
            request.add(
                    //批量更新和批量删除，在这里修改对应的请求就可以了
                    new IndexRequest("zhangbh_index")
                            .id("" + (i + 1))
                            .source(JSON.toJSONString(users.get(i)), XContentType.JSON)
            );

            BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
            System.out.println(bulk);
            //是否失败
            System.out.println(bulk.hasFailures());
        }
    }

    /**
     * 查询
     */
    @Test
    public void testSearch() throws IOException {
        SearchRequest request = new SearchRequest("zhangbh_index");
        //构建搜索条件


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询条件，可以使用QueryBuilders工具来实现
        //精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "liwy");
        //MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(termQueryBuilder);

        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(searchSourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
        System.out.println(response.getHits().toString());
        System.out.println("==================");
        for (SearchHit documentFields : response.getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }
    }
}
