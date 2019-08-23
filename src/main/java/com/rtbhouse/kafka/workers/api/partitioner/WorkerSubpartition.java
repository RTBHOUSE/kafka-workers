package com.rtbhouse.kafka.workers.api.partitioner;

import static java.util.function.Function.identity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.impl.pool.TopicPartitionPool;

public class WorkerSubpartition {

    private static final Map<WorkerSubpartition, WorkerSubpartition> INSTANCES = new ConcurrentHashMap<>();

    private final TopicPartition topicPartition;
    private final int subpartition;

    private int hash = 0;

    public static WorkerSubpartition getInstance(TopicPartition topicPartition, int subpartition) {
        return INSTANCES.computeIfAbsent(new WorkerSubpartition(topicPartition, subpartition), identity());
    }

    public static WorkerSubpartition getInstance(String topic, int partition, int subpartition) {
        return getInstance(TopicPartitionPool.getTopicPartition(topic, partition), subpartition);
    }

    private WorkerSubpartition(TopicPartition partition, int subpartition) {
        this.topicPartition = partition;
        this.subpartition = subpartition;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public String topic() {
        return topicPartition.topic();
    }

    public int partition() {
        return topicPartition.partition();
    }

    public int subpartition() {
        return subpartition;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topicPartition == null) ? 0 : topicPartition.hashCode());
        result = prime * result + subpartition;
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        WorkerSubpartition other = (WorkerSubpartition) obj;
        if (subpartition != other.subpartition) {
            return false;
        }
        if (topicPartition == null) {
            if (other.topicPartition != null) {
                return false;
            }
        } else if (!topicPartition.equals(other.topicPartition)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return topic() + "-" + partition() + "-" + subpartition();
    }
}
