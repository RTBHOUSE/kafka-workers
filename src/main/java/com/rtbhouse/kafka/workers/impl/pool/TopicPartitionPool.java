package com.rtbhouse.kafka.workers.impl.pool;

import static java.util.function.Function.identity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;

public class TopicPartitionPool {

    private static final Map<TopicPartition, TopicPartition> INSTANCES = new ConcurrentHashMap<>();

    public static TopicPartition getTopicPartition(String topic, int partition) {
        return INSTANCES.computeIfAbsent(new TopicPartition(topic, partition), identity());
    }
}
