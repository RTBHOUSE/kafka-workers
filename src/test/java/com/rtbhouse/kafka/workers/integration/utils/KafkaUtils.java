package com.rtbhouse.kafka.workers.integration.utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaUtils {

    private static final int MAX_RETRIES_DEFAULT = 10;

    private static final int RETRY_BACKOFF_MS_DEFAULT = 1000;

    public static AdminClient createAdminClient(String bootstrapServers) {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.RETRIES_CONFIG, MAX_RETRIES_DEFAULT,
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS_DEFAULT));
    }

    public static void createTopics(String bootstrapServers,
            int numPartitions, int replicationFactor, String... topics) {
        createTopics(bootstrapServers, numPartitions, replicationFactor, List.of(topics));
    }

    public static void createTopics(String bootstrapServers,
            int numPartitions, int replicationFactor, List<String> topics) {
        try(AdminClient adminClient = createAdminClient(bootstrapServers)) {
            createTopics(adminClient, numPartitions, replicationFactor, topics);
        }
    }

    public static void createTopics(AdminClient adminClient,
            int numPartitions, int replicationFactor, String... topics) {
        createTopics(adminClient, numPartitions, replicationFactor, List.of(topics));
    }

    public static void createTopics(AdminClient adminClient,
            int numPartitions, int replicationFactor, List<String> topics) {

        List<NewTopic> newTopics = topics.stream()
                .map(topicName -> new NewTopic(topicName, numPartitions, (short) replicationFactor))
                .collect(Collectors.toList());
        try {
            adminClient.createTopics(newTopics).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
