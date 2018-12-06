package com.rtbhouse.kafka.workers.impl.queues;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.task.TaskManager;

public class QueuesManager<K, V> implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(QueuesManager.class);

    private final WorkersConfig config;
    private final WorkersMetrics metrics;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final TaskManager<K, V> taskManager;

    private final Map<WorkerSubpartition, RecordsQueue<K, V>> queues = new ConcurrentHashMap<>();
    private final Map<WorkerSubpartition, Long> sizesInBytes = new ConcurrentHashMap<>();

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
            sizesInBytes.put(subpartition, 0L);
            metrics.addSizeMetric(WorkersMetrics.QUEUE_SIZE_METRIC, subpartition.toString(), queues.get(subpartition));

        }
    }

    @Override
    public void unregister(Collection<TopicPartition> topicPartitions) {
        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(topicPartitions)) {
            metrics.removeSizeMetric(WorkersMetrics.QUEUE_SIZE_METRIC, subpartition.toString());
            queues.get(subpartition).clear();
            sizesInBytes.put(subpartition, 0L);
        }
    }

    public WorkerRecord<K, V> poll(WorkerSubpartition subpartition) {
        WorkerRecord<K, V> record = queues.get(subpartition).poll();
        sizesInBytes.compute(subpartition, (key, value) -> value - record.size());
        return record;
    }

    public WorkerRecord<K, V> peek(WorkerSubpartition subpartition) {
        return queues.get(subpartition).peek();
    }

    public void push(WorkerSubpartition subpartition, WorkerRecord<K, V> record) {
        queues.get(subpartition).add(record);
        sizesInBytes.compute(subpartition, (key, value) -> value + record.size());
        taskManager.notifyTask(subpartition);
    }

    public Set<TopicPartition> getPartitionsToPause(Set<TopicPartition> assigned, Set<TopicPartition> paused) {
        Set<TopicPartition> partitionsToPause = new HashSet<>();
        if (config.getLong(WorkersConfig.QUEUE_TOTAL_MAX_SIZE_BYTES) != null
                && getTotalSizeInBytes() >= config.getLong(WorkersConfig.QUEUE_TOTAL_MAX_SIZE_BYTES)) {
            logger.warn("total size in bytes: {} exceeded", config.getLong(WorkersConfig.QUEUE_TOTAL_MAX_SIZE_BYTES));
            partitionsToPause.addAll(assigned);
            partitionsToPause.removeAll(paused);
            return partitionsToPause;
        }
        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(assigned)) {
            if (sizesInBytes.get(subpartition) >= config.getLong(WorkersConfig.QUEUE_MAX_SIZE_BYTES)
                    && !paused.contains(subpartition.topicPartition())) {
                logger.warn("size in bytes: {} for: {} (events count: {}) exceeded",
                        config.getLong(WorkersConfig.QUEUE_MAX_SIZE_BYTES), subpartition,
                        queues.get(subpartition).size());
                partitionsToPause.add(subpartition.topicPartition());
            }
        }
        return partitionsToPause;
    }

    public Set<TopicPartition> getPartitionsToResume(Set<TopicPartition> pausedPartitions) {
        if (config.getLong(WorkersConfig.QUEUE_TOTAL_MAX_SIZE_BYTES) != null
                && getTotalSizeInBytes() >= config.getLong(WorkersConfig.QUEUE_TOTAL_MAX_SIZE_BYTES)) {
            return new HashSet<>();
        }
        Set<TopicPartition> partitionsToResume = new HashSet<>();
        for (TopicPartition topicPartition : pausedPartitions) {
            boolean shouldBeResumed = true;
            for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(topicPartition)) {
                if (sizesInBytes.get(subpartition) > config.getLong(WorkersConfig.QUEUE_MAX_SIZE_BYTES) / 2) {
                    shouldBeResumed = false;
                }
            }
            if (shouldBeResumed) {
                partitionsToResume.add(topicPartition);
            }
        }
        return partitionsToResume;
    }

    private long getTotalSizeInBytes() {
        return sizesInBytes.values().stream().mapToLong(Long::longValue).sum();
    }

}
