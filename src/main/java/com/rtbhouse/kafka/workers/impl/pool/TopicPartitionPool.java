package com.rtbhouse.kafka.workers.impl.pool;

import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.impl.cache.UniqueInstancesCache;

public class TopicPartitionPool {

    private static final UniqueInstancesCache<TopicPartition> INSTANCES = new UniqueInstancesCache<>(64);

    public static TopicPartition getTopicPartition(String topic, int partition) {
        return INSTANCES.saveUnique(new TopicPartition(topic, partition));
    }
}
