package com.rtbhouse.kafka.workers.integration.utils;

import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public final class TestProperties {

    private static final String KAFKA_HOSTNAME = "127.0.0.1";
    private static final int KAFKA_PORT = 9192;
    private static final String BOOTSTRAP_SERVERS = KAFKA_HOSTNAME + ":" + KAFKA_PORT;

    private TestProperties() {
    }

    public static Properties serverProperties() {
        Properties serverProperties = new Properties();
        serverProperties.put("broker.id", 1);
        serverProperties.put("port", KAFKA_PORT);
        serverProperties.put("host.name", KAFKA_HOSTNAME);
        serverProperties.put("auto.create.topics.enable", false);
        serverProperties.put("offsets.topic.replication.factor", "1");
        return serverProperties;
    }

    public static <K, V> Properties workersProperties(Class<K> keyDeserializer, Class<V> valueDeserializer, String... topics) {
        Properties workersProperties = new Properties();
        workersProperties.put("consumer.topics", String.join(",", topics));
        workersProperties.put("consumer.kafka.bootstrap.servers", BOOTSTRAP_SERVERS);
        workersProperties.put("consumer.kafka.group.id", "workers" + ThreadLocalRandom.current().nextInt());
        workersProperties.put("consumer.kafka.auto.offset.reset", "earliest");
        workersProperties.put("consumer.kafka.key.deserializer", keyDeserializer);
        workersProperties.put("consumer.kafka.value.deserializer", valueDeserializer);
        return workersProperties;
    }

    public static <K, V> Properties producerProperties(Class<K> keySerializer, Class<V> valueSerializer) {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        producerProperties.put("key.serializer", keySerializer);
        producerProperties.put("value.serializer", valueSerializer);
        return producerProperties;
    }

    public static <K, V> Properties consumerProperties(Class<K> keyDeserializer, Class<V> valueDeserializer) {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumerProperties.put("key.deserializer", keyDeserializer);
        consumerProperties.put("value.deserializer", valueDeserializer);
        consumerProperties.put("group.id", "consumer" + ThreadLocalRandom.current().nextInt());
        consumerProperties.put("auto.offset.reset", "earliest");
        return consumerProperties;
    }

    public static <K, V> Properties adminClientProperties() {
        Properties adminClientProperties = new Properties();
        adminClientProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        return adminClientProperties;
    }

}
