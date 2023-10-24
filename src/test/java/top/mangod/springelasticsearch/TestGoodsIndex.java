package top.mangod.springelasticsearch;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
@Slf4j
public class TestGoodsIndex {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    /**
     * 创建索引库和Mapping
     * 建议通过kibana工具创建
     * PUT /goods
     * {}
     */
    @Test
    public void indexCreate() throws Exception {
        IndicesClient indicesClient = restHighLevelClient.indices();
        // 创建索引
        CreateIndexRequest indexRequest = new CreateIndexRequest("goods");
        // 创建表 结构
        String mapping = "{\n" + "    \"properties\": {\n" + "      \"brandName\": {\n" + "        \"type\": \"keyword\"\n" + "      },\n" + "      \"categoryName\": {\n" + "        \"type\": \"keyword\"\n" + "      },\n" + "      \"createTime\": {\n" + "        \"type\": \"date\",\n" + "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" + "      },\n" + "      \"id\": {\n" + "        \"type\": \"keyword\"\n" + "      },\n" + "      \"price\": {\n" + "        \"type\": \"double\"\n" + "      },\n" + "      \"saleNum\": {\n" + "        \"type\": \"integer\"\n" + "      },\n" + "      \"status\": {\n" + "        \"type\": \"integer\"\n" + "      },\n" + "      \"stock\": {\n" + "        \"type\": \"integer\"\n" + "      },\n" + "      \"title\": {\n" + "        \"type\": \"text\",\n" + "        \"analyzer\": \"ik_max_word\",\n" + "        \"search_analyzer\": \"ik_smart\"\n" + "      }\n" + "    }\n" + "  }";
        // 把映射信息添加到request请求里面
        // 第一个参数：表示数据源
        // 第二个参数：表示请求的数据类型
        indexRequest.mapping(mapping, XContentType.JSON);
        // 请求服务器
        CreateIndexResponse response = indicesClient.create(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }

    /**
     * 获取索引结构
     * GET goods/_mapping
     */
    @Test
    public void getMapping() throws Exception {
        IndicesClient indicesClient = restHighLevelClient.indices();

        // 创建get请求
        GetIndexRequest request = new GetIndexRequest("goods");
        // 发送get请求
        GetIndexResponse response = indicesClient.get(request, RequestOptions.DEFAULT);
        // 获取表结构
        Map<String, MappingMetadata> mappings = response.getMappings();
        for (String key : mappings.keySet()) {
            System.out.println("key--" + mappings.get(key).getSourceAsMap());
        }
    }

    /**
     * 删除索引库
     * 一般不能这么执行
     */
    @Test
    public void indexDelete() throws Exception {
        IndicesClient indicesClient = restHighLevelClient.indices();
        // 创建delete请求方式
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("goods2");
        // 发送delete请求
        AcknowledgedResponse response = indicesClient.delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }

    /**
     * 判断索引库是否存在
     */
    @Test
    public void indexExists() throws Exception {
        IndicesClient indicesClient = restHighLevelClient.indices();
        // 创建get请求
        GetIndexRequest request = new GetIndexRequest("goods");
        // 判断索引库是否存在
        boolean result = indicesClient.exists(request, RequestOptions.DEFAULT);
        System.out.println(result);
    }

    /**
     * 增加文档信息
     */
    @Test
    public void addDocument() throws IOException {
        // 创建商品信息
        Goods goods = new Goods();
        goods.setId(4L);
        goods.setTitle("鸿星尔克 男鞋 舒适轻巧 跑鞋");
        goods.setPrice(new BigDecimal("99.99"));
        goods.setStock(9999);
        goods.setSaleNum(101);
        goods.setCategoryName("男鞋");
        goods.setBrandName("鸿星尔克");
        goods.setStatus(1);
        goods.setCreateTime(new Date());

        // 将对象转为json
        String data = JSON.toJSONString(goods);
        // 创建索引请求对象
        IndexRequest indexRequest = new IndexRequest("goods").id(goods.getId() + "").source(data, XContentType.JSON);
        // 执行增加文档
        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        log.info("创建状态：{}", response.status());
    }

    /**
     * 获取文档信息
     */
    @Test
    public void getDocument() throws IOException {
        // 创建获取请求对象
        GetRequest getRequest = new GetRequest("goods", "4");
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> sourceAsMap = response.getSourceAsMap();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 更新文档信息
     */
    @Test
    public void updateDocument() throws IOException {

        // 设置商品更新信息
        Goods goods = new Goods();
        goods.setTitle("鸿星尔克 男鞋 舒适轻巧 跑鞋 护脚");
        goods.setPrice(new BigDecimal("89.99"));

        // 将对象转为json
        String data = JSON.toJSONString(goods);
        // 创建索引请求对象
        UpdateRequest updateRequest = new UpdateRequest("goods", "4");
        // 设置更新文档内容
        updateRequest.doc(data, XContentType.JSON);
        // 执行更新文档
        UpdateResponse response = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        log.info("创建状态：{}", response.status());
    }

    /**
     * 删除文档信息
     */
    @Test
    public void deleteDocument() throws IOException {

        // 创建删除请求对象
        DeleteRequest deleteRequest = new DeleteRequest("goods", "4");
        // 执行删除文档
        DeleteResponse response = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        log.info("删除状态：{}", response.status());
    }

    /**
     * 批量删除
     *
     * @throws IOException
     */
    @Test
    public void bulkDeleteDocument() throws IOException {
        BulkRequest request = new BulkRequest();

        request.add(new DeleteRequest().index("goods").id("2"));
        request.add(new DeleteRequest().index("goods").id("3"));
        request.add(new DeleteRequest().index("goods").id("5"));

        BulkResponse response = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.printf("Took===>%s", response.getTook());
        System.out.printf("Items===>%s", response.getItems().length);
    }

    /**
     * 批量添加文档
     */
    @Test
    public void bulkAddDocument() throws IOException {
        //1.查询所有数据，mysql
//        List<Goods> goodsList = goodsMapper.findAll();
        List<Goods> goodsList = new ArrayList<>();
        Goods goods6 = new Goods();
        goods6.setId(6L);
        goods6.setTitle("李宁 男鞋 舒适轻巧 跑鞋");
        goods6.setPrice(new BigDecimal("199.99"));
        goods6.setStock(99);
        goods6.setSaleNum(201);
        goods6.setCategoryName("男鞋");
        goods6.setBrandName("李宁");
        goods6.setStatus(1);
        goods6.setCreateTime(new Date());

        Goods goods7 = new Goods();
        goods7.setId(7L);
        goods7.setTitle("李宁 女鞋 舒适轻巧 跑鞋");
        goods7.setPrice(new BigDecimal("199.99"));
        goods7.setStock(999);
        goods7.setSaleNum(301);
        goods7.setCategoryName("女鞋");
        goods7.setBrandName("李宁");
        goods7.setStatus(1);
        goods7.setCreateTime(new Date());

        goodsList.add(goods6);
        goodsList.add(goods7);

        //2.bulk导入
        BulkRequest bulkRequest = new BulkRequest();

        //2.1 循环goodsList，创建IndexRequest添加数据
        for (Goods goods : goodsList) {
            //将goods对象转换为json字符串
            String data = JSON.toJSONString(goods);//map --> {}
            IndexRequest indexRequest = new IndexRequest("goods");
            indexRequest.id(goods.getId() + "").source(data, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    /**
     * 匹配查询符合条件的所有数据，并设置分页
     */
    @Test
    public void matchAllQuery() {
        try {
            // 构建查询条件 (查询全部)
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            // 创建查询源构造器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchAllQueryBuilder);
            // 设置分页
            searchSourceBuilder.from(0); // 从第几条开始，不包含它
            searchSourceBuilder.size(3); // 要取多少条数据
            // 设置排序
            searchSourceBuilder.sort("price", SortOrder.ASC);
            // 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段;查询的文档只包含哪些指定的字段
            searchSourceBuilder.fetchSource(new String[]{"id", "title", "categoryName"}, new String[]{});
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 排序查询(sort) 代码同matchAllQuery
     * 匹配查询符合条件的所有数据，并设置分页
     */
    @Test
    public void matchAllQuery2() {
        try {
            // 构建查询条件
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            // 创建查询源构造器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchAllQueryBuilder);
            // 设置分页
            searchSourceBuilder.from(2); // 从第几条开始，不包含它
            searchSourceBuilder.size(3); // 要取多少条数据
            // 设置排序
            searchSourceBuilder.sort("price", SortOrder.ASC);
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 分页查询(page) 代码同matchAllQuery
     * 匹配查询符合条件的所有数据，并设置分页
     */
    @Test
    public void matchAllQuery3() {
        try {
            // 构建查询条件
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            // 创建查询源构造器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchAllQueryBuilder);
            // 设置分页
            searchSourceBuilder.from(2); // 从第几条开始，不包含它
            searchSourceBuilder.size(3); // 要取多少条数据
            // 设置排序
            searchSourceBuilder.sort("price", SortOrder.ASC);
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 匹配查询数据
     */
    @Test
    public void matchQuery() {
        try {
            // 构建查询条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // 查询title为 跑步 的文档，查询不到。
            // 因为："analyzer": "ik_max_word","search_analyzer": "ik_smart"。写入文档时，拆分出了“跑鞋”这个词项，查询的时候拆分出了“跑步”这个词项，两者匹配不上
            searchSourceBuilder.query(QueryBuilders.matchQuery("title", "跑步"));
            // 查询title为 桃和李 的文档，可以查询到。
            // 因为："analyzer": "ik_max_word","search_analyzer": "ik_smart"。写入文档时，拆分出了“李”这个词项，查询的时候也会拆出“李”这个词项，两者能匹配不上
            searchSourceBuilder.query(QueryBuilders.matchQuery("title", "桃和李"));
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 词语匹配查询
     */
    @Test
    public void matchPhraseQuery() {
        try {
            // 构建查询条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // title是 舒适轻巧 时，可以查到，改成 轻巧舒适 时，则查不到，因为短语匹配，会把查询文本看做短语，有顺序要求
            searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("title", "轻巧舒适"));
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 内容在多字段中进行查询
     */
    @Test
    public void matchMultiQuery() {
        try {
            // 构建查询条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // "手机"在title字段,categoryName字段中存在的文档
            searchSourceBuilder.query(
                    QueryBuilders.multiMatchQuery("手机", "title", "categoryName")
                            .minimumShouldMatch("50%") // 设置匹配度
                            .field("title", 10)); // 指定指定设置匹配度
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 词项查询（termQuery），对查询文本不做分词
     */
    @Test
    public void termQuery() {
        try {
            // 构建查询条件（注意：termQuery 支持多种格式查询，如 boolean、int、double、string 等，这里使用的是 string 的查询）
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // 表示检索title字段值为 李宁的跑鞋 的文档，如果是match则可以查到，term查询不到，因为term查询时不对查询文本做分词
            searchSourceBuilder.query(QueryBuilders.termQuery("title", "李宁的跑鞋"));
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info("=======" + goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * terms:多个查询内容在一个字段中进行查询
     */
    @Test
    public void termsQuery() {
        try {
            // 构建查询条件（注意：termsQuery 支持多种格式查询，如 boolean、int、double、string 等，这里使用的是 string 的查询）
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // 表示title字段值为华为,OPPO,TCL的文档都能检索出来
            searchSourceBuilder.query(QueryBuilders.termsQuery("title", "华为", "OPPO", "TCL"));
            // 展示100条,默认只展示10条记录
            searchSourceBuilder.size(100);
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 查询排序
     *
     * @throws IOException
     */
    @Test
    public void searchOrderDoc() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("goods");
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        builder.sort("price", SortOrder.DESC);
        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.printf("TotalHits===>%s \n", hits.getTotalHits());
        System.out.printf("Took===>%s \n", response.getTook());

        hits.forEach(h -> {
            System.out.printf("===>%s \n", h.getSourceAsString());
        });

    }

    /**
     * 通配符查询
     * *：表示多个字符（0个或多个字符）
     * ?：表示单个字符
     */
    @Test
    public void wildcardQuery() {
        try {
            // 构建查询条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // 查询所有以 “鞋” 结尾的商品信息
            searchSourceBuilder.query(QueryBuilders.wildcardQuery("title", "*鞋"));
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 模糊查询，用于不小心打错字的场景下的搜索，中文场景下好像用的很少，尴尬
     */
    @Test
    public void fuzzyQuery() {
        try {
            // 构建查询条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // 所有以 “鞋” 结尾的商品信息
            searchSourceBuilder.query(QueryBuilders.fuzzyQuery("title", "几").fuzziness(Fuzziness.AUTO));
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 查询价格大于等于1000的商品
     */
    @Test
    public void rangeQuery() {
        try {
            // 构建查询条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.rangeQuery("price").gte(1000));
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                log.info(hits.getTotalHits().value + "");
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 查询距离现在 10 年间的商品信息
     * [年(y)、月(M)、星期(w)、天(d)、小时(h)、分钟(m)、秒(s)]
     * 例如：
     * now-1h 查询一小时内范围
     * now-1d 查询一天内时间范围
     * now-1y 查询最近一年内的时间范围
     */
    @Test
    public void dateRangeQuery() {
        try {
            // 构建查询条件
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // includeLower（是否包含下边界）、includeUpper（是否包含上边界）
            searchSourceBuilder.query(QueryBuilders.rangeQuery("createTime")
                    .gte("now-10h")
                    .includeLower(true)
                    .includeUpper(true));
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 根据查询条件滚动查询，scroll方式必须一页一页往下查，不能跳
     * 可以用来解决深度分页查询问题
     */
    @Test
    public void scrollQuery() {

        // 假设用户想获取第5页数据，其中每页1条
        int pageNo = 3;
        int pageSize = 1;

        // 定义请求对象
        SearchRequest searchRequest = new SearchRequest("goods");

        // 构建查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        searchRequest.source(builder.query(QueryBuilders.matchAllQuery()).sort("price", SortOrder.DESC).size(pageSize));
        String scrollId = null;
        // 3、发送请求到ES
        SearchResponse scrollResponse = null;
        // 设置游标id存活时间
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(2));
        // 记录所有游标id
        List<String> scrollIds = new ArrayList<>();
        for (int i = 0; i < pageNo; i++) {
            try {
                // 首次检索
                if (i == 0) {
                    //记录游标id
                    searchRequest.scroll(scroll);
                    // 首次查询需要指定索引名称和查询条件
                    SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                    // 下一次搜索要用到该游标id
                    scrollId = response.getScrollId();
                }
                // 非首次检索
                else {
                    // 不需要在使用其他条件，也不需要指定索引名称，只需要使用执行游标id存活时间和上次游标id即可，毕竟信息都在上次游标id里面呢
                    SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                    searchScrollRequest.scroll(scroll);
                    scrollResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                    // 下一次搜索要用到该游标id
                    scrollId = scrollResponse.getScrollId();
                }
                // 记录所有游标id
                scrollIds.add(scrollId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 查询完毕，清除游标id
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.scrollIds(scrollIds);
        try {
            restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("清除滚动查询游标id失败");
            e.printStackTrace();
        }

        // 4、处理响应结果
        System.out.println("滚动查询返回数据：");
        assert scrollResponse != null;
        SearchHits hits = scrollResponse.getHits();
        for (SearchHit hit : hits) {
            // 将 JSON 转换成对象
            Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
            // 输出查询信息
            log.info(goods.toString());
        }
    }

    /**
     * 组合查询，boolQuery 查询
     * 案例：查询从20231023 ~ 20231025 期间 标题含 李宁 的商品信息
     * [年(y)、月(M)、星期(w)、天(d)、小时(h)、分钟(m)、秒(s)]
     */
    @Test
    public void boolQuery() {
        try {
            // 创建 Bool 查询构建器
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            // 构建查询条件
            boolQueryBuilder
                    .must(QueryBuilders.matchQuery("title", "李宁"))
                    .filter()
                    .add(QueryBuilders.rangeQuery("createTime").format("yyyyMMdd")
                            .gte("20231023").lte("20231025"));
            // 构建查询源构建器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(boolQueryBuilder);
            searchSourceBuilder.size(100);
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 组合查询
     *
     * @throws IOException
     */
    @Test
    public void searchBoolDoc() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("goods");
        SearchSourceBuilder builder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("price", 8799));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("saleNum", "600"));

        builder.query(boolQueryBuilder);

        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.printf("TotalHits===>%s \n", hits.getTotalHits());
        System.out.printf("Took===>%s \n", response.getTook());

        hits.forEach(h -> {
            System.out.printf("===>%s \n", h.getSourceAsString());
        });

    }

    /**
     * queryStringQuery查询
     * 案例：查询出必须包含 华为手机 词语的商品信息，跟 matchPhrase 类似
     */
    @Test
    public void queryStringQuery() {
        try {
            // 创建 queryString 查询构建器
            // 会对华为手机 进行分词, 没有设置检索的field ,默认对mapping中字符串类型的filed进行检索;
            QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("自研的芯片").defaultOperator(Operator.AND);

            // 设置了field,就对指定的字段进行检索
            //QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("华为手机").field("title").field("categoryName").defaultOperator(Operator.AND);

            // 构建查询源构建器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(queryStringQueryBuilder);
            searchSourceBuilder.size(100);
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 查询某个范围
     *
     * @throws IOException
     */
    @Test
    public void searchRangeDoc() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("goods");
        SearchSourceBuilder builder = new SearchSourceBuilder();

        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
        rangeQuery.gte(5000);
        rangeQuery.lte(10000);
        builder.query(rangeQuery);

        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.printf("TotalHits===>%s \n", hits.getTotalHits());
        System.out.printf("Took===>%s \n", response.getTook());

        hits.forEach(h -> {
            System.out.printf("===>%s \n", h.getSourceAsString());
        });

    }

    /**
     * 过滤source获取部分字段内容，查询指定字段
     * 案例：只获取 title、categoryName和price的数据
     */
    @Test
    public void sourceFilter() {
        try {
            //查询条件(词条查询：对应ES query里的match)
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("title", "华为"))
                    .must(QueryBuilders.matchQuery("categoryName", "手机"))
                    .filter(QueryBuilders.rangeQuery("price").gt(5000).lt(10000));

            // 构建查询源构建器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(boolQueryBuilder);

            // 如果查询的属性很少，那就使用includes，而excludes设置为空数组
            // 如果排序的属性很少，那就使用excludes，而includes设置为空数组
            String[] includes = {"title", "categoryName", "price"};
            String[] excludes = {};
            searchSourceBuilder.fetchSource(includes, excludes);

            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 高亮查询，先把对应的文档查出来，然后高亮，然后再修改返回结果
     * 案例：把标题中为 华为手机 的词语高亮显示
     */
    @Test
    public void highlightBuilder() {
        try {
            //查询条件(词条查询：对应ES query里的match)
            MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "华为手机");

            //设置高亮三要素
            // field: 你的高亮字段
            // preTags ：前缀
            // postTags：后缀
            HighlightBuilder highlightBuilder = new HighlightBuilder()
                    .field("title")
                    .preTags("<font color='red'>")
                    .postTags("</font>");

            // 构建查询源构建器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchQueryBuilder);
            searchSourceBuilder.highlighter(highlightBuilder);
            searchSourceBuilder.size(100);
            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 根据状态和数据条数验证是否返回了数据
            if (RestStatus.OK.equals(searchResponse.status()) && searchResponse.getHits().getTotalHits().value > 0) {
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    // 将 JSON 转换成对象
                    Goods goods = JSON.parseObject(hit.getSourceAsString(), Goods.class);

                    // 获取高亮的数据
                    HighlightField highlightField = hit.getHighlightFields().get("title");
                    System.out.println("高亮名称：" + highlightField.getFragments()[0].string());

                    // 替换掉原来的数据
                    Text[] fragments = highlightField.getFragments();
                    if (fragments != null && fragments.length > 0) {
                        StringBuilder title = new StringBuilder();
                        for (Text fragment : fragments) {
                            //System.out.println(fragment);
                            title.append(fragment);
                        }
                        goods.setTitle(title.toString());
                    }
                    // 输出查询信息
                    log.info(goods.toString());
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 聚合查询
     * Metric 指标聚合分析
     * 案例：分别获取最贵的商品 和 获取最便宜的商品
     */
    @Test
    public void metricQuery() {
        try {
            // 构建查询条件
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            // 创建查询源构造器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchAllQueryBuilder);

            // 获取最贵的商品
            AggregationBuilder maxPrice = AggregationBuilders.max("maxPrice").field("price");
            searchSourceBuilder.aggregation(maxPrice);
            // 获取最便宜的商品
            AggregationBuilder minPrice = AggregationBuilders.min("minPrice").field("price");
            searchSourceBuilder.aggregation(minPrice);

            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedMax max = aggregations.get("maxPrice");
            log.info("最贵的价格：" + max.getValue());
            ParsedMin min = aggregations.get("minPrice");
            log.info("最便宜的价格：" + min.getValue());
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 分组聚合查询
     * Bucket 分桶聚合分析
     * 案例：根据品牌进行聚合统计
     */
    @Test
    public void bucketQuery() {
        try {
            // 构建查询条件
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            // 创建查询源构造器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchAllQueryBuilder);

            // 根据商品分类进行分组查询
            TermsAggregationBuilder aggBrandName = AggregationBuilders
                    .terms("brandNameName")
                    .field("brandName");
            searchSourceBuilder.aggregation(aggBrandName);

            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedStringTerms aggBrandName1 = aggregations.get("brandNameName");
            for (Terms.Bucket bucket : aggBrandName1.getBuckets()) {
                // 分组名 ==== 数量
                System.out.println(bucket.getKeyAsString() + "====" + bucket.getDocCount());
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 子聚合聚合查询
     * Bucket 分桶聚合分析
     * 案例：根据商品分类进行分组查询, 并且获取分类商品中的平均价格
     */
    @Test
    public void subBucketQuery() {
        try {
            // 构建查询条件
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            // 创建查询源构造器
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchAllQueryBuilder);

            // 根据商品分类进行分组查询,并且获取分类商品中的平均价格
            TermsAggregationBuilder subAggregation = AggregationBuilders
                    .terms("brandNameName")
                    .field("brandName")
                    .subAggregation(AggregationBuilders.avg("avgPrice").field("price"));
            searchSourceBuilder.aggregation(subAggregation);

            // 创建查询请求对象，将查询对象配置到其中
            SearchRequest searchRequest = new SearchRequest("goods");
            searchRequest.source(searchSourceBuilder);
            // 执行查询，然后处理响应结果
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedStringTerms aggBrandName1 = aggregations.get("brandNameName");
            for (Terms.Bucket bucket : aggBrandName1.getBuckets()) {
                // 获取聚合后的品牌的平均价格,注意返回值不是Aggregation对象,而是指定的ParsedAvg对象
                ParsedAvg avgPrice = bucket.getAggregations().get("avgPrice");
                // 分组名 ==== 平均价格
                System.out.println(bucket.getKeyAsString() + "====" + avgPrice.getValueAsString());
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 子查询下的子查询
     * 根据商品分类聚合，获取每个商品类的平均价格，并且在商品分类聚合之上子聚合每个品牌的平均价格
     */
    @Test
    public void subSubAgg() throws IOException {

        // 构建查询条件
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        // 创建查询源构造器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchAllQueryBuilder);

        // 注意这里聚合写的位置不要写错,很容易搞混,错一个括号就不对了
        TermsAggregationBuilder subAggregation = AggregationBuilders
                .terms("categoryNameAgg").field("categoryName")
                .subAggregation(AggregationBuilders.avg("categoryNameAvgPrice").field("price"))
                .subAggregation(AggregationBuilders.terms("brandNameAgg").field("brandName")
                        .subAggregation(AggregationBuilders.avg("brandNameAvgPrice").field("price")));
        searchSourceBuilder.aggregation(subAggregation);

        // 创建查询请求对象，将查询对象配置到其中
        SearchRequest searchRequest = new SearchRequest("goods");
        searchRequest.source(searchSourceBuilder);
        // 执行查询，然后处理响应结果
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //获取总记录数
        System.out.println("totalHits = " + searchResponse.getHits().getTotalHits().value);
        // 获取聚合信息
        Aggregations aggregations = searchResponse.getAggregations();
        ParsedStringTerms categoryNameAgg = aggregations.get("categoryNameAgg");
        //获取值返回
        for (Terms.Bucket bucket : categoryNameAgg.getBuckets()) {
            // 获取聚合后的分类名称
            String categoryName = bucket.getKeyAsString();
            // 获取聚合命中的文档数量
            long docCount = bucket.getDocCount();
            // 获取聚合后的分类的平均价格,注意返回值不是Aggregation对象,而是指定的ParsedAvg对象
            ParsedAvg avgPrice = bucket.getAggregations().get("categoryNameAvgPrice");

            System.out.println(categoryName + "======平均价:" + avgPrice.getValue() + "======数量:" + docCount);

            ParsedStringTerms brandNameAgg = bucket.getAggregations().get("brandNameAgg");
            for (Terms.Bucket brandeNameAggBucket : brandNameAgg.getBuckets()) {
                // 获取聚合后的品牌名称
                String brandName = brandeNameAggBucket.getKeyAsString();

                // 获取聚合后的品牌的平均价格,注意返回值不是Aggregation对象,而是指定的ParsedAvg对象
                ParsedAvg brandNameAvgPrice = brandeNameAggBucket.getAggregations().get("brandNameAvgPrice");

                System.out.println("     " + brandName + "======" + brandNameAvgPrice.getValue());
            }
        }
    }

    @Test
    public void clusterHealthStatus() throws IOException {

        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse response = restHighLevelClient.cluster().health(request, RequestOptions.DEFAULT);
        ClusterHealthStatus status = response.getStatus();
        System.out.println("集群名称：" + response.getClusterName());
        System.out.println("集群健康状态：" + status.name());
    }

    @Test
    public void printIndexInfo() throws IOException {
        Response response = restHighLevelClient.getLowLevelClient().performRequest(new Request("GET", "/_cat/indices"));

        HttpEntity entity = response.getEntity();
        String responseStr = EntityUtils.toString(entity, StandardCharsets.UTF_8);

        String[] indexInfoArr = responseStr.split("\n");
        for (String indexInfo : indexInfoArr) {

            String[] infoArr = indexInfo.split("\\s+");
            String status = infoArr[0];
            String open = infoArr[1];
            String name = infoArr[2];
            String id = infoArr[3];
            String mainShardNum = infoArr[4];
            String viceShardNum = infoArr[5];
            String docNum = infoArr[6];
            String deletedDocNum = infoArr[7];
            String allShardSize = infoArr[8];
            String mainShardSize = infoArr[9];
            System.out.println("》》》》》》》》索引信息》》》》》》》》");
            System.out.println("名称：" + name);
            System.out.println("id：" + id);
            System.out.println("状态：" + status);
            System.out.println("是否开放：" + open);
            System.out.println("主分片数量：" + mainShardNum);
            System.out.println("副本分片数量：" + viceShardNum);
            System.out.println("Lucene文档数量：" + docNum);
            System.out.println("被删除文档数量：" + deletedDocNum);
            System.out.println("所有分片大小：" + allShardSize);
            System.out.println("主分片大小：" + mainShardSize);
        }
    }

}
