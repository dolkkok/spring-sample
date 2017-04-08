package com.tutorial;

import com.tutorial.domain.Comment;
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
import org.springframework.context.annotation.*;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.expression.spel.ast.Indexer;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;

/// https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.3/query-dsl-filters-caching.html

@Configuration
@ComponentScan({"com.tmon"})
public class App {

    public String getIndexName() {
        return "test";
    }

    public String getTypeName() {
        return "data";
    }

    @Bean
    App app() {
        return new App();
    }

    public Client createClient() {

        /// easticsearch가 한대인 경우
        Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        /// elasticsearch 여러대인 경우
        /// Client client = new TransportClient()
        ///       .addTransportAddress(new InetSocketTransportAddress("localhost", 9300))
        ///       .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        return client;
    }


    /// Index 생성
    public void createIndex(Client client) throws IOException {
        CreateIndexResponse r = client.admin().indices().prepareCreate(getIndexName()).execute().actionGet();

        if (r.isAcknowledged() == true) {
            System.out.println("Create Index : " + getIndexName());
        }
    }

    public void createIndexSettings(Client client) throws IOException {
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

    public void addMapping(Client client) throws IOException {

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

    public void addNetedMapping(Client client) throws Exception {
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
    public boolean existIndex(Client client) {

        IndicesExistsResponse r = client.admin().indices().prepareExists(getIndexName()).execute().actionGet();

        if (r.isExists() == true) {
            System.out.println("Exist Index : " + getIndexName());
            return true;
        }

        return false;
    }

    /// index 삭제
    public void deleteIndex(Client client) {
        DeleteIndexResponse r = client.admin().indices().prepareDelete(getIndexName()).execute().actionGet();

        if (r.isAcknowledged() == true) {
            System.out.println("Delete Index : " + getIndexName());
        }
    }

    /// 데이터 입력
    public void insertDocument(Client client, String docId, String name, String age, String memo, List<String> studio, List<Comment> comments) throws Exception, IOException {
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

    /// 데이터 삭제
    public void deleteDocument(Client client, String docId) {
        DeleteRequest deleteRequest = new DeleteRequest(getIndexName(), getTypeName(), docId);
        DeleteResponse r = client.delete(deleteRequest).actionGet();

        if (r.isFound() == true) {
            System.out.println("Delete Document : " + docId);
        }
    }

    /// 데이터 갱신
    public void updateDocument(Client client, String docId, String name, String age, String memo) {

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
    public void searchAllDocument(Client client) {

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

    public void searchAllDocumentWithField(Client client) {

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

    public void searchTermQuery(Client client) {

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(termQuery("name", "tomas"))
                .setFrom(1)
                .setSize(2);

        doQuery(builder);
    }


    public void searchBoolQuery(Client client) {
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

    public  void searchMatchQuery(Client client) {
        QueryBuilder qb = QueryBuilders.matchQuery("name", "tomas");

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void multiMatchQuery(Client client) {
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

    public void idsQuery(Client client) {
        QueryBuilder qb = QueryBuilders.idsQuery().ids("1", "2", "4");
        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .addSort("age", SortOrder.ASC);
             // .addSort(SortBuilders.fieldSort("name").order(SortOrder.ASC));

        doQuery(builder);
    }

    public void constantScoreQuery(Client client) {
        /// With Queries
        QueryBuilder qb = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("name","james"))
                .boost(2.0f);

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void constantScoreQueryFilter(Client client) {
        /// Using with Filters
        QueryBuilder qb = QueryBuilders.constantScoreQuery(FilterBuilders.termFilter("name", "tomas"))
                .boost(2.0f);

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void prefixQuery(Client client) {
        QueryBuilder qb = QueryBuilders.prefixQuery("name", "tom");
        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void searchRangeQuery(Client client) {
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

    public void searchTermsQuery(Client client) {
        QueryBuilder qb = QueryBuilders.termsQuery("memo",    // field
                                "hello", "am")               // values
                                .minimumMatch(1);              // How many terms must match

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void searchWildCardQuery(Client client) {
        QueryBuilder qb = QueryBuilders.wildcardQuery("name", "t?ma*");

        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);

        doQuery(builder);
    }

    public void searchAndFilter(Client client) {
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

    public void searchBoolFilter(Client client) {
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

    public void searchOrFilter(Client client) {
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

    public void searchQueryFilterCache(Client client) {
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
     *
     * @param client
     * @apiNote
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.7/nested.html
     */
    public void searchNestedQuery(Client client) {

        QueryBuilder queryBuilder = QueryBuilders.nestedQuery(
            "comments",
            QueryBuilders.boolQuery()
                .must(matchQuery("comments.name", "alice"))
                .must(rangeQuery("comments.date").gt(2018))
        ).scoreMode("avg"); /// scoreMode : avg (default), max, total, none

        /*
        FilteredQueryBuilder filteredQueryBuilder =
                QueryBuilders.filteredQuery(QueryBuilders.termQuery("name", "tomas"),
                                            FilterBuilders.termFilter("name","tomas"));
        */
        SearchRequestBuilder builder = client.prepareSearch(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder);

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
            Client client = app.createClient();

            //app.createIndex(client);
            app.createIndexSettings(client);
            app.addMapping(client);
            app.addNetedMapping(client);
            app.existIndex(client);
            app.deleteIndex(client);

            app.insertDocument(client, "1", "Tomas", "10", "Hello Tomas 2017",
                    Arrays.asList("mbc", "sbs", "ebs", "kbs"),
                    Arrays.asList(new Comment("Jonh Smith", "2017"), new Comment("Alice White", "2018")));

            app.insertDocument(client, "2", "James", "20", "Hello James 2018",
                    Arrays.asList("mbc", "sbs"),
                    Arrays.asList(new Comment("Alice White", "2019")));

            app.insertDocument(client, "3", "Lukas", "30", "Hello Lukas 2019", Arrays.asList("sbs", "ebs", "kbs"), null);
            app.insertDocument(client, "4", "Tomas", "40", "Hello Tomas 2020", Arrays.asList("ebs", "kbs"), null);
            app.insertDocument(client, "5", "Tomas", "10", "10I Am Tomas 2020", Arrays.asList("ebs"), null);
            app.deleteDocument(client, "3");
            app.updateDocument(client, "2", "James Dean", "20", "Hello James 2018");

            app.searchAllDocument(client);
            app.searchAllDocumentWithField(client);
            app.searchTermQuery(client);
//          app.searchFilteredQuery(client);
            app.searchBoolQuery(client);
            app.searchBoolQuery(client);
            app.searchMatchQuery(client);
            app.multiMatchQuery(client);
            app.idsQuery(client);
            app.constantScoreQuery(client);
            app.constantScoreQueryFilter(client);
            app.prefixQuery(client);
            app.searchRangeQuery(client);
            app.searchTermsQuery(client);
            app.searchWildCardQuery(client);
            app.searchAndFilter(client);
            app.searchBoolFilter(client);
            app.searchOrFilter(client);
            app.searchQueryFilterCache(client);
            app.searchNestedQuery(client);

            client.close();

        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            if (null != context) {
                context.close();
            }
        }
    }
}
