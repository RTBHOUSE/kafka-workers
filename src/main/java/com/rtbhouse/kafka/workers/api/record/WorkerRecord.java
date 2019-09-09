package com.rtbhouse.kafka.workers.api.record;

import static com.rtbhouse.kafka.workers.impl.pool.TopicPartitionPool.getTopicPartition;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;

public class WorkerRecord<K, V> {

    private final ConsumerRecord<K, V> record;
    private final int subpartition;

    public WorkerRecord(ConsumerRecord<K, V> record, int subpartition) {
        this.record = record;
        this.subpartition = subpartition;
    }

    public WorkerSubpartition workerSubpartition() {
        return WorkerSubpartition.getInstance(record.topic(), record.partition(), subpartition);
    }

    public TopicPartition topicPartition() {
        return getTopicPartition(record.topic(), record.partition());
    }

    public String topic() {
        return record.topic();
    }

    public int partition() {
        return record.partition();
    }

    public int subpartition() {
        return subpartition;
    }

    public long offset() {
        return record.offset();
    }

    public long timestamp() {
        return record.timestamp();
    }

    public Headers headers() {
        return record.headers();
    }

    public K key() {
        return record.key();
    }

    public V value() {
        return record.value();
    }

    public int size() {
        return record.serializedKeySize() + record.serializedValueSize();
    }

    @Override
    public String toString() {
        return "WorkerRecord(record = " + record + ", subpartition = " + subpartition + ")";
    }

}
