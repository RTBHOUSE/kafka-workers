package com.rtbhouse.kafka.workers.impl.partitioner;

import static com.rtbhouse.kafka.workers.impl.pool.TopicPartitionPool.getTopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerPartitioner;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.impl.errors.BadSubpartitionException;

public class SubpartitionSupplier<K, V> {

    private final WorkerPartitioner<K, V> partitioner;

    public SubpartitionSupplier(WorkerPartitioner<K, V> partitioner) {
        this.partitioner = partitioner;
    }

    public WorkerSubpartition subpartition(ConsumerRecord<K, V> record) {
        // returns WorkerSubpartition for given ConsumerRecord using WorkerPartitioner to determine sub-partition id
        TopicPartition topicPartition = getTopicPartition(record.topic(), record.partition());
        int subpartition = partitioner.subpartition(record);
        if (subpartition < 0 || subpartition >= partitioner.count(topicPartition)) {
            throw new BadSubpartitionException("Invalid subpartition: " + subpartition);
        }
        return WorkerSubpartition.getInstance(topicPartition, subpartition);
    }

    public List<WorkerSubpartition> subpartitions(TopicPartition topicPartition) {
        return subpartitions(Arrays.asList(topicPartition));
    }

    public List<WorkerSubpartition> subpartitions(Collection<TopicPartition> topicPartitions) {
        // generates all WorkerSubpartition(s) associated with given TopicPartition(s)
        List<WorkerSubpartition> workerSubpartitions = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            for (int subpartition = 0; subpartition < partitioner.count(topicPartition); subpartition++) {
                workerSubpartitions.add(WorkerSubpartition.getInstance(topicPartition, subpartition));
            }
        }
        return workerSubpartitions;
    }

}
