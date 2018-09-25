package com.rtbhouse.kafka.workers.api.partitioner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * User-defined partitioner used for additional sub-partitioning which could give better distribution of processing.
 * <p>
 * Be careful defining and using partitioner because stream of records from one {@link TopicPartition} could be
 * reordered during processing. From the perspective of {@link TopicPartition} it gives partial order of records,
 * however records with the same {@link WorkerSubpartition} remain ordered to each other. It leads also to a bit more
 * complex offsets committing policy which is provided by {@code KafkaWorkers} to ensure at-least-once delivery.
 */
public interface WorkerPartitioner<K, V> {

    /**
     * Determines the sub-partition id for given {@link ConsumerRecord}.
     *
     * @param consumerRecord
     *            consumer record read by internal {@link KafkaConsumer}
     *
     * @return an integer between 0 and {@link #count()}-1
     */
    int subpartition(ConsumerRecord<K, V> consumerRecord);

    /**
     * Returns the total number of sub-partitions for every topic partition.
     *
     * @return an integer greater than or equal to 1
     */
    int count();

}
