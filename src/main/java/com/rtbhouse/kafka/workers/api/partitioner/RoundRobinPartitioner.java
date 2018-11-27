package com.rtbhouse.kafka.workers.api.partitioner;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Round-robin implementation of {@link WorkerPartitioner}. It provides one-to-many mapping from {@link TopicPartition}
 * to {@link WorkerSubpartition}. Because of distributed processing records could be reordered during processing.
 */
public class RoundRobinPartitioner<K, V> implements WorkerPartitioner<K, V> {

    private final Map<TopicPartition, Integer> partitionCounterMap = new HashMap<>();
    private final int subpartitionsCount;

    public RoundRobinPartitioner(int subpartitionsCount) {
        this.subpartitionsCount = subpartitionsCount;
    }

    @Override
    public int subpartition(ConsumerRecord<K, V> consumerRecord) {
        return nextValue(consumerRecord.topic(), consumerRecord.partition()) % subpartitionsCount;
    }

    @Override
    public int count(TopicPartition topicPartition) {
        return subpartitionsCount;
    }

    private int nextValue(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Integer counter = partitionCounterMap.get(topicPartition);
        if (counter == null) {
            counter = 0;
        }
        partitionCounterMap.put(topicPartition, counter + 1);
        return counter;
    }

}
