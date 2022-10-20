package com.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import com.es.types.Customer;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.util.*;

import static com.es.service.StartConsumer.getConsumer;

@Component
public class ElasticSearchQuery {

    private final String indexName = "kafkadump";

    ElasticSearchConfiguration configuration = new ElasticSearchConfiguration();
    ElasticsearchClient elasticsearchClient = configuration.getElasticsearchClient();
    public void createOrUpdateDocument(Customer object) throws IOException {

        IndexResponse response = elasticsearchClient.index(i -> i
                .index(indexName)
                .id(String.valueOf(object.getId()))
                .document(object)
        );
        if (response.result().name().equals("Created")) {
            System.out.println("Document has been successfully created.");
        } else if (response.result().name().equals("Updated")) {
            System.out.println("Document has been successfully updated.");
        }
        else {
            System.out.println("Error while performing the operation.");
        }
    }
    public void bulkInsert() throws IOException {

        List<Customer> customers = getConsumer();
        for (Customer c:customers) {
            createOrUpdateDocument(c);
        }
    }


//        List<Customer> customers = getConsumer();
//        BulkRequest.Builder br = new BulkRequest.Builder();
//        if(elasticsearchClient==null){
//            System.out.println("elastic search client is null");
//        }
//        for (Customer customer : customers) {
//            br.operations(op -> op
//                    .index(idx -> idx
//                            .index(indexName)
//                            .id(String.valueOf(customer.getId()))
//                            .document(customer)
//                    )
//            );
//        }
//        BulkResponse result = null;
//        try {
//            result = elasticsearchClient.bulk(br.build());
//        } catch (Exception e) {
//            System.out.println(e + "error while adding ");
//        }
//
//        // Log errors, if any
//        if(result==null){
//            System.out.println("result is null");
//        }
//        if (result.errors()) {
//            System.out.println("Bulk had errors");
//            for (BulkResponseItem item : result.items()) {
//                if (item.error() != null) {
//                    System.out.println(item.error().reason());
//                }
//            }
//        }
//        return result;
//    }
}
