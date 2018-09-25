package com.rtbhouse.kafka.workers.impl.queues;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.task.TaskManager;

public class QueuesManager<K, V> implements Partitioned {

    private final WorkersConfig config;
    private final WorkersMetrics metrics;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final TaskManager<K, V> taskManager;

    private final Map<WorkerSubpartition, RecordsQueue<K, V>> queues = new ConcurrentHashMap<>();

    public QueuesManager(
            WorkersConfig config,
            WorkersMetrics metrics,
            SubpartitionSupplier<K, V> subpartitionSupplier,
            TaskManager<K, V> taskManager) {
        this.config = config;
        this.metrics = metrics;
        this.subpartitionSupplier = subpartitionSupplier;
        this.taskManager = taskManager;
    }

    @Override
    public void register(Collection<TopicPartition> topicPartitions) {
        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(topicPartitions)) {
            queues.put(subpartition, new RecordsQueue<>());
            metrics.addSizeMetric(WorkersMetrics.QUEUE_SIZE_METRIC, subpartition.toString(), queues.get(subpartition));
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> topicPartitions) {
        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(topicPartitions)) {
            metrics.removeSizeMetric(WorkersMetrics.QUEUE_SIZE_METRIC, subpartition.toString());
            queues.get(subpartition).clear();
        }
    }

    public WorkerRecord<K, V> poll(WorkerSubpartition subpartition) {
        return queues.get(subpartition).poll();
    }

    public WorkerRecord<K, V> peek(WorkerSubpartition subpartition) {
        return queues.get(subpartition).peek();
    }

    public void push(WorkerSubpartition subpartition, WorkerRecord<K, V> record) {
        queues.get(subpartition).add(record);
        taskManager.notifyTask(subpartition);
    }

    public Set<TopicPartition> getPartitionsToPause(Set<TopicPartition> assigned, Set<TopicPartition> paused) {
        final int queueMaxSize = config.getInt(WorkersConfig.QUEUE_MAX_SIZE);
        Set<TopicPartition> partitionsToPause = new HashSet<>();
        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(assigned)) {
            if (queues.get(subpartition).size() >= queueMaxSize && !paused.contains(subpartition.topicPartition())) {
                partitionsToPause.add(subpartition.topicPartition());
            }
        }
        return partitionsToPause;
    }

    public Set<TopicPartition> getPartitionsToResume(Set<TopicPartition> pausedPartitions) {
        final int queueMaxSize = config.getInt(WorkersConfig.QUEUE_MAX_SIZE);
        Set<TopicPartition> partitionsToResume = new HashSet<>();
        for (TopicPartition topicPartition : pausedPartitions) {
            boolean shouldBeResumed = true;
            for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(topicPartition)) {
                if (queues.get(subpartition).size() > queueMaxSize / 2) {
                    shouldBeResumed = false;
                }
            }
            if (shouldBeResumed) {
                partitionsToResume.add(topicPartition);
            }
        }
        return partitionsToResume;
    }

}
