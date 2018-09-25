package com.rtbhouse.kafka.workers.api.partitioner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Default implementation of {@link WorkerPartitioner}. It provides one-to-one mapping from {@link TopicPartition} to
 * {@link WorkerSubpartition}.
 */
public class DefaultPartitioner<K, V> implements WorkerPartitioner<K, V> {

    @Override
    public int subpartition(ConsumerRecord<K, V> consumerRecord) {
        return 0;
    }

    @Override
    public int count() {
        return 1;
    }

}
