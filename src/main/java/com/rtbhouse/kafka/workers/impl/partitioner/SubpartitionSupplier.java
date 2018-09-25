package com.rtbhouse.kafka.workers.impl.partitioner;

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
        int subpartition = partitioner.subpartition(record);
        if (subpartition < 0 || subpartition >= partitioner.count()) {
            throw new BadSubpartitionException("Invalid subpartition: " + subpartition);
        }
        return new WorkerSubpartition(record.topic(), record.partition(), subpartition);
    }

    public List<WorkerSubpartition> subpartitions(TopicPartition topicPartition) {
        return subpartitions(Arrays.asList(topicPartition));
    }

    public List<WorkerSubpartition> subpartitions(Collection<TopicPartition> topicPartitions) {
        // generates all WorkerSubpartition(s) associated with given TopicPartition(s)
        List<WorkerSubpartition> workerSubpartitions = new ArrayList<>();
        for (TopicPartition partition : topicPartitions) {
            for (int subpartition = 0; subpartition < partitioner.count(); subpartition++) {
                workerSubpartitions.add(new WorkerSubpartition(partition, subpartition));
            }
        }
        return workerSubpartitions;
    }

}
