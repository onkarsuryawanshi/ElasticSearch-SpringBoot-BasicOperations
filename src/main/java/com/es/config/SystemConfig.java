package com.es.config;

import java.util.Properties;

public class SystemConfig {
    public final static String producerApplicationID = "Customer-ProducerApp";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String topicName = "customer-demo-topic";

    public static Properties getConsumerProps(boolean autoCommit, Long autoCommitMillisInterval) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", "testGroup");
        props.setProperty("enable.auto.commit", Boolean.toString(autoCommit));
        if (autoCommit) {
            props.setProperty("auto.commit.interval.ms", Long.toString(autoCommitMillisInterval));
        }
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
