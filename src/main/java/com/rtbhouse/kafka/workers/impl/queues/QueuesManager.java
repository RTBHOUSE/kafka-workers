package com.rtbhouse.kafka.workers.impl.queues;

import static java.util.stream.Collectors.toUnmodifiableSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.record.weigher.RecordWeigher;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.task.TaskManager;

public class QueuesManager<K, V> implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(QueuesManager.class);

    private final WorkersConfig config;

    /**
     * Each individual queue may exceed its limit with some margin. In the worst case scenario a single margin
     * may have a size of consumer.kafka.max.partition.fetch.bytes. That's why we have to pause all partitions
     * when the total limit is exceeded.
     */
    private final long queueTotalSizeBytesToPause;
    private volatile long queueSizeBytesToPause;
    private volatile long queueSizeBytesToResume;

    private final WorkersMetrics metrics;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final TaskManager<K, V> taskManager;
    private final RecordWeigher<K, V> recordWeigher;

    private final Map<WorkerSubpartition, RecordsQueue<K, V>> queues = new ConcurrentHashMap<>();
    private final Map<WorkerSubpartition, Long> sizesInBytes = new ConcurrentHashMap<>();
    private final Set<WorkerSubpartition> registeredSubpartitions = new HashSet<>();

    public QueuesManager(
            WorkersConfig config,
            WorkersMetrics metrics,
            SubpartitionSupplier<K, V> subpartitionSupplier,
            TaskManager<K, V> taskManager,
            RecordWeigher<K, V> recordWeigher) {

        this.config = config;
        this.metrics = metrics;
        this.subpartitionSupplier = subpartitionSupplier;
        this.taskManager = taskManager;
        this.recordWeigher = recordWeigher;
        this.queueTotalSizeBytesToPause = (long)(Runtime.getRuntime().maxMemory() * config.getQueueTotalSizeBytesHeapRatio());
    }

    @Override
    public void register(Collection<TopicPartition> topicPartitions) {
        List<WorkerSubpartition> subpartitions = subpartitionSupplier.subpartitions(topicPartitions);
        registeredSubpartitions.addAll(subpartitions);
        for (WorkerSubpartition subpartition : subpartitions) {
            queues.put(subpartition, new RecordsQueue<>());
            sizesInBytes.put(subpartition, 0L);
            metrics.addSizeMetric(WorkersMetrics.QUEUE_SIZE_METRIC, subpartition.toString(), queues.get(subpartition));
        }
        computeQueueSizeToPauseAndResume();
    }

    private void computeQueueSizeToPauseAndResume() {
        int numQueues = Math.max(1, registeredSubpartitions.size());
        queueSizeBytesToPause = queueTotalSizeBytesToPause / numQueues;
        queueSizeBytesToResume = (long)(config.getDouble(WorkersConfig.QUEUE_RESUME_RATIO) * queueSizeBytesToPause);

        int mega = 1024 * 1024;
        logger.info("queueTotalSizeBytesToPause = {} [{} MiB], queueSizeBytesToPause = {} [{} MiB], queueSizeBytesToResume = {} [{} MiB]",
                queueTotalSizeBytesToPause, queueTotalSizeBytesToPause / mega,
                queueSizeBytesToPause, queueSizeBytesToPause / mega,
                queueSizeBytesToResume, queueSizeBytesToResume / mega);
    }

    @Override
    public void unregister(Collection<TopicPartition> topicPartitions) {
        List<WorkerSubpartition> subpartitions = subpartitionSupplier.subpartitions(topicPartitions);
        registeredSubpartitions.removeAll(subpartitions);
        for (WorkerSubpartition subpartition : subpartitions) {
            metrics.removeMetric(WorkersMetrics.QUEUE_SIZE_METRIC, subpartition.toString());
            // TODO: why subpartitions are not removed here?
            queues.get(subpartition).clear();
            sizesInBytes.put(subpartition, 0L);
        }
        computeQueueSizeToPauseAndResume();
    }

    public WorkerRecord<K, V> poll(WorkerSubpartition subpartition) {
        WorkerRecord<K, V> record = queues.get(subpartition).poll();
        sizesInBytes.compute(subpartition, (key, value) -> value - recordWeigher.weight(record));
        return record;
    }

    public WorkerRecord<K, V> peek(WorkerSubpartition subpartition) {
        return queues.get(subpartition).peek();
    }

    public void push(WorkerSubpartition subpartition, WorkerRecord<K, V> record) {
        queues.get(subpartition).add(record);
        sizesInBytes.compute(subpartition, (key, value) -> value + recordWeigher.weight(record));
        taskManager.notifyTask(subpartition);
    }

    public Set<TopicPartition> getPartitionsToPause(Set<TopicPartition> assigned, Set<TopicPartition> paused) {
        Set<TopicPartition> partitionsToPause = new HashSet<>();
        long totalSizeBytes = getTotalSizeInBytes();
        if (totalSizeBytes >= queueTotalSizeBytesToPause) {
            logger.warn("total size in bytes: {} exceeded (limit: {} {})",
                    totalSizeBytes, queueTotalSizeBytesToPause,
                    diffPctString(totalSizeBytes, queueTotalSizeBytesToPause));
            partitionsToPause.addAll(assigned);
            partitionsToPause.removeAll(paused);
            return partitionsToPause;
        }
        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(assigned)) {
            long queueSizeBytes = sizesInBytes.get(subpartition);
            if (queueSizeBytes >= queueSizeBytesToPause && !paused.contains(subpartition.topicPartition())) {
                logger.warn("size in bytes: {} for: {} (events count: {}) exceeded (limit: {} {})",
                        queueSizeBytes, subpartition, queues.get(subpartition).size(),
                        queueSizeBytesToPause, diffPctString(queueSizeBytes, queueSizeBytesToPause));
                partitionsToPause.add(subpartition.topicPartition());
            }
        }
        return partitionsToPause;
    }

    private String diffPctString(long a, long b) {
        return String.format("%+.2f%%", ((double) a / b - 1.0) * 100.0);
    }

    public Set<TopicPartition> getPartitionsToResume(Set<TopicPartition> pausedPartitions) {
        if (getTotalSizeInBytes() >= queueTotalSizeBytesToPause) {
            return Collections.emptySet();
        }

        return pausedPartitions.stream()
                .filter(this::shouldBeResumed)
                .collect(toUnmodifiableSet());
    }

    private boolean shouldBeResumed(TopicPartition topicPartition) {
        return subpartitionSupplier.subpartitions(topicPartition).stream()
                .allMatch(subpartition -> sizesInBytes.get(subpartition) <= queueSizeBytesToResume);
    }

    private long getTotalSizeInBytes() {
        return sizesInBytes.values().stream().mapToLong(Long::longValue).sum();
    }
}
