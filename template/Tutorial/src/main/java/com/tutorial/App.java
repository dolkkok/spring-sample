package com.tutorial;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutorial.domain.Comment;
import com.tutorial.domain.Student;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.ArrayListMultimap;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.lucene.docset.AllDocIdSet;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.expression.spel.ast.Indexer;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import org.apache.log4j.Logger;

/// https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.3/query-dsl-filters-caching.html

@Configuration
@ComponentScan({"com.tmon"})
public class App {

    @Autowired
    public Client client;

    protected static Logger logger = Logger.getLogger(App.class);


    public String getIndexName() {
        return "test";
    }

    public String getTypeName() {
        return "data";
    }

    /*
    @Bean
    App app() {
        return new App();
    }
    */

    @Bean
    public Client client() {

        /// easticsearch가 한대인 경우
        Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        /// elasticsearch 여러대인 경우
        /// Client client = new TransportClient()
        ///       .addTransportAddress(new InetSocketTransportAddress("localhost", 9300))
        ///       .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        return client;
    }

    @PreDestroy
    public void onDestroy() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    /// Index 생성
    public void createIndex() throws IOException {
        CreateIndexResponse r = client.admin().indices().prepareCreate(getIndexName()).execute().actionGet();

        if (r.isAcknowledged() == true) {
            System.out.println("Create Index : " + getIndexName());
        }
    }

    public void createIndexSettings() throws IOException {
        XContentBuilder settingBuilder =
            jsonBuilder()
                .startObject()
                    .startObject("index")
                        .startObject("analysis")
                            .startObject("analyzer")
                                .startObject("low_analyzer")
                                    .field("type", "custom")
                                    .array("char_filter", "html_strip")
                                    .array("filter", "lowercase", "asciifolding")
                                    .field("tokenizer", "standard")
                                .endObject()
                                .startObject("upper_analyzer")
                                    .field("type", "custom")
                                    .array("char_filter", "html_strip")
                                    .array("filter", "uppercase", "asciifolding")
                                    .field("tokenizer", "standard")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        XContentBuilder mappingBuilder =
            jsonBuilder()
                .startObject()
                    .startObject(getTypeName())
                        .startObject("properties")
                            .startObject("name")
                                .field("type", "string")
                                .field("analyzer", "low_analyzer")
                            .endObject()
                            .startObject("age")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("memo")
                                .field("type", "string")
                                .field("analyzer", "upper_analyzer")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        System.out.println(settingBuilder.string());
        System.out.println(mappingBuilder.string());
        final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(getIndexName());

        createIndexRequestBuilder.setSettings(settingBuilder);
        createIndexRequestBuilder.addMapping(getTypeName(), mappingBuilder);

        // MAPPING DONE
        CreateIndexResponse r = createIndexRequestBuilder.execute().actionGet();

        if (r.isAcknowledged() == true) {
            System.out.println("Create Index : " + getIndexName());
        }
    }

    public void addMapping() throws IOException {

        XContentBuilder mappingBuilder =
                jsonBuilder()
                    .startObject()
                        .startObject(getTypeName())
                            .startObject("properties")
                                .startObject("studio")
                                    .field("type", "string")
                                    .field("index", "not_analyzed")     /// 입력값이 배열인 경우 반드시 *not_analyzed* 로 매핑되어야 함.
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject();

        System.out.println(mappingBuilder.string());

        PutMappingRequestBuilder putMappingRequestBuilder = client.admin().indices().preparePutMapping(getIndexName())
                .setType(getTypeName()).setSource(mappingBuilder);

        PutMappingResponse putMappingResponse = putMappingRequestBuilder.execute().actionGet();

        if (putMappingResponse.isAcknowledged() == true) {
            System.out.println("Put Mapping : ");
        }
    }

    public void addNestedMapping() throws Exception {
        XContentBuilder mappingBuilder =
            jsonBuilder()
                .startObject()
                    .startObject(getTypeName())
                        .startObject("properties")
                            .startObject("comments")
                                .field("type", "nested")
                                .startObject("properties")
                                    .startObject("name")
                                        .field("type", "string")
                                    .endObject()
                                    .startObject("date")
                                        .field("type", "string")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        System.out.println(mappingBuilder.string());

        PutMappingRequestBuilder putMappingRequestBuilder = client.admin().indices().preparePutMapping(getIndexName())
                .setType(getTypeName()).setSource(mappingBuilder);

        PutMappingResponse putMappingResponse = putMappingRequestBuilder.execute().actionGet();

        if (putMappingResponse.isAcknowledged() == true) {
            System.out.println("Put Mapping : ");
        }
    }

    /// index 생성 여부 확인
    public boolean existIndex() {

        IndicesExistsResponse r = client.admin().indices().prepareExists(getIndexName()).execute().actionGet();

        if (r.isExists() == true) {
            System.out.println("Exist Index : " + getIndexName());
            return true;
        }

        return false;
    }

    /// index 삭제
    public void deleteIndex() {
        DeleteIndexResponse r = client.admin().indices().prepareDelete(getIndexName()).execute().actionGet();

        if (r.isAcknowledged() == true) {
            System.out.println("Delete Index : " + getIndexName());
        }
    }

    /// 데이터 입력
    /// https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.6/generate.html
    public void insertDocument(String docId, String name, String age, String memo, List<String> studio, List<Comment> comments) throws Exception, IOException {
        XContentBuilder builder =
            jsonBuilder()
                .startObject()
                    .field("name", name)
                    .field("age", age)
                    .field("memo", memo)
                    .field("studio", studio)
                    .startArray("comments");

                if (comments != null) {
                    for (Comment c : comments) {
                        builder
                            .startObject()
                                .field("name", c.getName())
                                .field("date", c.getDate())
                            .endObject();
                    }
                }

                builder
                    .endArray()
                .endObject();

        System.out.println(builder.string());


        IndexRequest indexRequest = new IndexRequest(getIndexName(), getTypeName(), docId);
        indexRequest.source(builder);
        IndexResponse r = client.index(indexRequest).actionGet();

        if (r.isCreated() == true) {
            System.out.println("Insert Document : " + name);
        }

        /*
        Map<String, Object> objectHashMap = new HashMap<String, Object>();
        objectHashMap.put("name", name);
        objectHashMap.put("age", age);
        objectHashMap.put("memo", memo);
        objectHashMap.put("city", city);

        Map<String, Object> lessonMap = new HashMap<String, Object>();
        lessonMap.put("place")

        objectHashMap.put("lesson.place", lesson);

        IndexRequest indexRequest = new IndexRequest(getIndexName(), getTypeName(), docId);
        indexRequest.source(objectHashMap);
        IndexResponse r = client.index(indexRequest).actionGet();

        if (r.isCreated() == true) {
            System.out.println("Insert Document : " + name);
        }
        */
    }

    /// https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.6/generate.html
    public void InsertDocumentObject(Student student) throws Exception {

        IndexRequest indexRequest = new IndexRequest(getIndexName(), getTypeName(), student.getDocId());

        ObjectMapper om = new ObjectMapper();
        byte []json = om.writeValueAsBytes(student);
        indexRequest.source(json);
        IndexResponse r = client.index(indexRequest).actionGet();

        if (r.isCreated() == true) {
            System.out.println("Insert Document : " + student.getName());
        }
    }

    /// 데이터 삭제
    public void deleteDocument(String docId) {
        DeleteRequest deleteRequest = new DeleteRequest(getIndexName(), getTypeName(), docId);
        DeleteResponse r = client.delete(deleteRequest).actionGet();

        if (r.isFound() == true) {
            System.out.println("Delete Document : " + docId);
        }
    }

    /// 데이터 갱신
    public void updateDocument(String docId, String name, String age, String memo) {

        UpdateRequest updateRequest = new UpdateRequest(getIndexName(), getTypeName(), docId);

        Map<String, Object> objectHashMap = new HashMap<String, Object>();
        objectHashMap.put("name", name);
        objectHashMap.put("age", age);
        objectHashMap.put("memo", memo);

        updateRequest.doc(objectHashMap);

        UpdateResponse r = client.update(updateRequest).actionGet();

        if (r.isCreated() == true) {
            System.out.println("Update Document : " + docId);
        }
    }

    /// 데이터 조회
    public void searchAllDocument() {

        SearchRequestBuilder builder = client.prepareSearch()
                .setIndices(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery());

        System.out.println(builder.internalBuilder());
        SearchResponse r = builder.execute().actionGet();

        String name, age, memo;

        for (SearchHit hit : r.getHits()) {

            hit.getSource().forEach((key, val) -> {
                System.out.print(key + " : " + val + ", ");
            });

            System.out.println();

            age = (String)hit.getSource().get("age");
            name = (String)hit.getSource().get("name");
            memo = (String)hit.getSource().get("memo");

            System.out.println("name : " + name +
                    ", age : " + age +
                    ", memo : " + memo);
        }
    }

    public void searchAllDocumentWithField() {

        SearchRequestBuilder builder = client.prepareSearch()
                .setIndices(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addFields("name", "age", "memo")
                .setQuery(QueryBuilders.matchAllQuery());

        System.out.println(builder.internalBuilder());
        SearchResponse r = builder.execute().actionGet();
        String name, age, memo;

        for (SearchHit hit : r.getHits()) {
            hit.fields().forEach((key, val) -> {
                System.out.print(key + " : " + val.<String>getValue() + ", ");
            });

            System.out.println();

            age = hit.field("age").<String>getValue();
            name = hit.field("name").<String>getValue();
            memo = hit.field("memo").<String>getValue();

            System.out.println("name : " + name +
                                ", age : " + age +
                                ", memo : " + memo);
        }
    }

    public void searchTermQuery() {

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(termQuery("name", "tomas"))
                .setFrom(1)
                .setSize(2);

        doQuery(builder);
    }


    public void searchBoolQuery() {
        /// 반드시 포함 : name = tomas, age = 10
        /// 반드시 불포함 : age < 30 and age > 40
        /// OR 조건 : memo = 'hello' or memo = 'am'
        QueryBuilder qb = QueryBuilders
                .boolQuery()
                .must(termQuery("name", "tomas"))
                .must(termQuery("age", "10"))
                .must(termQuery("studio", "kbs"))
                .mustNot(rangeQuery("age").from(30).to(40))
                .should(termQuery("memo", "hello"))
                .should(termQuery("memo", "am"));

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public  void searchMatchQuery() {
        QueryBuilder qb = QueryBuilders.matchQuery("name", "tomas");

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void searchRegexQuery() {
        QueryBuilder qb = QueryBuilders.regexpQuery("name", "tomas");

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void multiMatchQuery() {
        QueryBuilder qb = QueryBuilders.multiMatchQuery(
                "hello tomas",     // Text you are looking for
                "name", "memo"           // Fields you query on
        );

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void idsQuery() {
        QueryBuilder qb = QueryBuilders.idsQuery().ids("1", "2", "4");
        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .addSort("age", SortOrder.ASC);
             // .addSort(SortBuilders.fieldSort("name").order(SortOrder.ASC));

        doQuery(builder);
    }

    public void searchIdsQuery() throws  Exception {
        QueryBuilder qb = QueryBuilders.idsQuery().ids("6");
        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .addSort("age", SortOrder.ASC);

        System.out.println(builder.internalBuilder());
        SearchResponse r = builder.get();
        ObjectMapper om = new ObjectMapper();

        for (SearchHit hit : r.getHits()) {
            System.out.println(hit.getSourceAsString());
            String sJson = hit.getSourceAsString();
            Student student = om.readValue(sJson, Student.class);

            System.out.println("name : " + student.getName());
            /*
            hit.getSource().forEach((key, val) -> {
                System.out.print(key + " : " + val + ", ");
            });
            System.out.println();
            */
        }
    }

    public void constantScoreQuery() {
        /// With Queries
        QueryBuilder qb = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("name","james"))
                .boost(2.0f);

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void constantScoreQueryFilter() {
        /// Using with Filters
        QueryBuilder qb = QueryBuilders.constantScoreQuery(FilterBuilders.termFilter("name", "tomas"))
                .boost(2.0f);

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void prefixQuery() {
        QueryBuilder qb = QueryBuilders.prefixQuery("name", "tom");
        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void searchRangeQuery() {
        QueryBuilder qb = QueryBuilders
                        .rangeQuery("age")
                        .from(10)
                        .to(30)
                        .includeLower(true)
                        .includeUpper(false);

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void searchTermsQuery() {
        QueryBuilder qb = QueryBuilders.termsQuery("memo",    // field
                                "hello", "am")               // values
                                .minimumMatch(1);              // How many terms must match

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void searchWildCardQuery() {
        QueryBuilder qb = QueryBuilders.wildcardQuery("name", "t?ma*");

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void searchAndFilter() {
        FilterBuilder fb = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("age").from("10").to("30"),
                FilterBuilders.prefixFilter("name", "tom")
        );

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(fb);

        doQuery(builder);
    }

    public void searchBoolFilter() {
        FilterBuilder fb = FilterBuilders.boolFilter()
                .must(FilterBuilders.termFilter("name", "tomas"))
                .mustNot(FilterBuilders.rangeFilter("age").from("40").to("100"))
                .should(FilterBuilders.termFilter("memo", "am"))
                .should(FilterBuilders.termFilter("memo", "2017"));

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(fb);

        doQuery(builder);
    }

    public void searchOrFilter() {
        FilterBuilder fb = FilterBuilders.orFilter(
                FilterBuilders.termFilter("name", "tomas"),
                FilterBuilders.termFilter("name", "james")
        );

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(fb);

        doQuery(builder);
    }

    public void searchQueryFilterCache() {
        FilterBuilder fb = FilterBuilders.andFilter(
                    FilterBuilders.rangeFilter("age").from("10").to("20"),
                    FilterBuilders.prefixFilter("name", "tom")
                ).cache(true);

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(fb);

        doQuery(builder);
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.7/nested.html
     */
    public void searchNestedQuery() {

        QueryBuilder queryBuilder = QueryBuilders.nestedQuery(
            "comments",
            QueryBuilders.boolQuery()
                .must(matchQuery("comments.name", "alice"))
                .must(rangeQuery("comments.date").gt(2018))
        ).scoreMode("avg"); /// scoreMode : avg (default), max, total, none

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder);

        doQuery(builder);
    }


    public void searchFilteredMatchQueryTermFilter() {

        FilteredQueryBuilder filteredQueryBuilder =
                QueryBuilders.filteredQuery(QueryBuilders.regexpQuery("name", "tomas"),
                        FilterBuilders.termFilter("age","10"));

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(filteredQueryBuilder);

        doQuery(builder);


    }



    public void searchFilteredTermQueryTermFilter() {

        FilteredQueryBuilder filteredQueryBuilder =
                QueryBuilders.filteredQuery(QueryBuilders.termQuery("name", "tomas"),
                                            FilterBuilders.termFilter("age","10"));

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(filteredQueryBuilder);

        doQuery(builder);


    }

    public void doQuery(SearchRequestBuilder builder) {
        System.out.println(builder.internalBuilder());
        SearchResponse r = builder.get();

        for (SearchHit hit : r.getHits()) {
            System.out.println(hit.getSourceAsString());
            /*
            hit.getSource().forEach((key, val) -> {
                System.out.print(key + " : " + val + ", ");
            });
            System.out.println();
            */
        }
    }

    public static void main( String[] args ) {

        AnnotationConfigApplicationContext context = null;

        try {
            context = new AnnotationConfigApplicationContext(App.class);
            App app = context.getBean(App.class);

            logger.info("hello world");


            //app.createIndex();
//            app.createIndexSettings();
//            app.addMapping();
//            app.addNestedMapping();
//            app.existIndex();
//            app.deleteIndex();


            app.insertDocument("1", "Tomas", "10", "Hello Tomas 2017",
                    Arrays.asList("mbc", "sbs", "ebs", "kbs"),
                    Arrays.asList(new Comment("Jonh Smith", "2017"), new Comment("Alice White", "2018")));

            app.insertDocument("2", "James", "20", "Hello James 2018",
                    Arrays.asList("mbc", "sbs"),
                    Arrays.asList(new Comment("Alice White", "2019")));

            app.insertDocument("3", "Lukas", "30", "Hello Lukas 2019", Arrays.asList("sbs", "ebs", "kbs"), null);
            app.insertDocument("4", "Tomas", "40", "Hello Tomas 2020", Arrays.asList("ebs", "kbs"), null);
            app.insertDocument("5", "Tomas", "10", "10I Am Tomas 2020", Arrays.asList("ebs"), null);

            Student student = new Student("6", "Suzi", "25", "Hello Suzi 2017",
                    Arrays.asList("mbc", "sbs", "ebs", "kbs", "jtbc"),
                    Arrays.asList(new Comment("Mina Jung", "2017"), new Comment("Semi Lee", "2018")));

            app.InsertDocumentObject(student);

            app.deleteDocument("3");
            app.updateDocument("2", "James Dean", "20", "Hello James 2018");

            app.searchAllDocument();
            app.searchAllDocumentWithField();
            app.searchTermQuery();
            //app.searchFilteredQuery();
            app.searchBoolQuery();
            app.searchBoolQuery();
            app.searchMatchQuery();
            app.multiMatchQuery();
            app.idsQuery();
            app.searchIdsQuery();
            app.constantScoreQuery();
            app.constantScoreQueryFilter();
            app.prefixQuery();
            app.searchRangeQuery();
            app.searchTermsQuery();
            app.searchWildCardQuery();
            app.searchAndFilter();
            app.searchBoolFilter();
            app.searchOrFilter();
            app.searchQueryFilterCache();
            app.searchNestedQuery();
            //app.searchFilteredQuery();


            app.searchFilteredMatchQueryTermFilter();
            app.searchRegexQuery();
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            if (null != context) {
                context.close();
            }
        }
    }
}
