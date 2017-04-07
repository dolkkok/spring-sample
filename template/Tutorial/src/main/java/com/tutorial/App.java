package com.tutorial;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.springframework.context.annotation.*;
import org.springframework.expression.spel.ast.Indexer;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.HashMap;
import java.util.Map;

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
    public void createIndex(Client client) {
        CreateIndexResponse r = client.admin().indices().prepareCreate(getIndexName()).execute().actionGet();

        if (r.isAcknowledged() == true) {
            System.out.println("Create Index : " + getIndexName());
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
    public void insertDocument(Client client, String docId, String name, String age, String memo) {
        Map<String, Object> objectHashMap = new HashMap<String, Object>();
        objectHashMap.put("name", name);
        objectHashMap.put("age", age);
        objectHashMap.put("memo", memo);

        IndexRequest indexRequest = new IndexRequest(getIndexName(), getTypeName(), docId);
        indexRequest.source(objectHashMap);
        IndexResponse r = client.index(indexRequest).actionGet();

        if (r.isCreated() == true) {
            System.out.println("Insert Document : " + name);
        }
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

        SearchResponse r = client.prepareSearch()
                .setIndices(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();

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

        SearchResponse r = client.prepareSearch()
                .setIndices(getIndexName())
                .setTypes(getTypeName())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addFields("name", "age", "memo")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();

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

    public static void main( String[] args ) {

        AnnotationConfigApplicationContext context = null;

        try {
            context = new AnnotationConfigApplicationContext(App.class);
            App app = context.getBean(App.class);
            Client client = app.createClient();

            //app.createIndex(client);
            //app.existIndex(client);
            //app.deleteIndex(client);

            app.insertDocument(client, "1", "Tomas", "10", "Hello Tomas 2017");
            app.insertDocument(client, "2", "James", "20", "Hello James 2018");
            app.insertDocument(client, "3", "Lukas", "30", "Hello Lukas 2019");

            app.deleteDocument(client, "3");
            app.updateDocument(client, "2", "James Dean", "20", "Hello James 2018");

            //app.searchAllDocument(client);
            app.searchAllDocumentWithField(client);
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
