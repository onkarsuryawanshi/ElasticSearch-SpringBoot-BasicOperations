package com.es.service;


import com.es.ElasticSearchQuery;
import com.es.config.SystemConfig;
import com.es.types.Customer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.es.config.SystemConfig.topicName;

@Component
public class StartConsumer {
    public void execute() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(StartConsumer::getConsumer);
        ElasticSearchQuery query = new ElasticSearchQuery();
        query.bulkInsert();
        executorService.shutdown();
    }
    public static List<Customer> getConsumer(){
        List<Customer> customerList = new ArrayList<>();
        Properties consumerProps = SystemConfig.getConsumerProps(true, 1000L);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        consumer.assign(Collections.singleton(topicPartition));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("consumed: key = %s, value = %s, partition id= %s, offset = %s%n",
//                        record.key(), record.value(), record.partition(), record.offset());
                String json = record.value();
                ObjectMapper objectMapper = new ObjectMapper();
                Customer customer = null;
                try {
                    customer = objectMapper.readValue(json, Customer.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                customerList.add(customer);
            }
            if (records.isEmpty()) {
                System.out.println("-- terminating consumer --");
                break;
            }

            printOffsets(consumer, topicPartition);
        }
        printOffsets(consumer, topicPartition);

//        System.out.println(customerList.size());
        return customerList;
    }
    private static void printOffsets(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        Map<TopicPartition, OffsetAndMetadata> committed = consumer
                .committed(new HashSet<>(Arrays.asList(topicPartition)));
        OffsetAndMetadata offsetAndMetadata = committed.get(topicPartition);
        long position = consumer.position(topicPartition);
        System.out.printf("Committed: %s, current position %s%n", offsetAndMetadata == null ? null : offsetAndMetadata
                .offset(), position);
    }

}
