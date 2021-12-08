package com.rtbhouse.kafka.workers.impl.task;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskManager<K, V> implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);
    private static final Duration CHECK_TIMED_OUT_RECORDS_EVERY = Duration.ofSeconds(1);

    private final WorkersConfig config;
    private final WorkersMetrics metrics;
    private final WorkerTaskFactory<K, V> taskFactory;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final List<WorkerThread<K, V>> threads;
    private final OffsetsState offsetsState;
    private final Duration consumerProcessingTimeout;
    private final WorkerThreadAssignor<K, V> workerThreadAssignor;

    private final Map<WorkerSubpartition, WorkerTaskImpl<K, V>> partitionToTaskMap = new ConcurrentHashMap<>();

    private final Object rebalanceLock = new Object();

    public TaskManager(
            WorkersConfig config,
            WorkersMetrics metrics,
            WorkerTaskFactory<K, V> taskFactory,
            SubpartitionSupplier<K, V> subpartitionSupplier,
            List<WorkerThread<K, V>> threads,
            OffsetsState offsetsState) {
        this.config = config;
        this.metrics = metrics;
        this.taskFactory = taskFactory;
        this.subpartitionSupplier = subpartitionSupplier;
        this.threads = threads;
        this.offsetsState = offsetsState;
        this.consumerProcessingTimeout = this.config.getConsumerProcessingTimeout();
        this.workerThreadAssignor = new SpreadSubpartitionsWorkerThreadAssignor<>();
    }

    @Override
    public void register(Collection<TopicPartition> topicPartitions) throws InterruptedException {
        stopThreads();

        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(topicPartitions)) {
            WorkerTaskImpl<K, V> task = new WorkerTaskImpl<>(taskFactory.createTask(config), metrics);
            task.init(subpartition, config);
            partitionToTaskMap.put(subpartition, task);
        }

        rebalanceTasks();

        startThreads();
    }

    @Override
    public void unregister(Collection<TopicPartition> topicPartitions) throws InterruptedException {
        stopThreads();

        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(topicPartitions)) {
            WorkerTaskImpl<K, V> task = partitionToTaskMap.get(subpartition);
            task.close();
            partitionToTaskMap.remove(subpartition);
        }

        rebalanceTasks();

        startThreads();
    }

    public void notifyTask(WorkerSubpartition subpartition) {
        partitionToTaskMap.get(subpartition).notifyTask();
    }

    public void notifyTaskManager() {
        synchronized (rebalanceLock) {
            rebalanceLock.notifyAll();
        }
    }

    private void rebalanceTasks() {
        final Map<WorkerThread<K, V>, List<WorkerSubpartition>> threadsAssignments = workerThreadAssignor.assign(
                Collections.unmodifiableCollection(partitionToTaskMap.keySet()), Collections.unmodifiableList(threads));
        for (WorkerThread<K, V> thread : threads) {
            final List<WorkerSubpartition> assignedSubpartitions = threadsAssignments.getOrDefault(thread, List.of());
            for (WorkerSubpartition workerSubpartition : assignedSubpartitions) {
                final WorkerTaskImpl<K, V> task = partitionToTaskMap.get(workerSubpartition);
                thread.addTask(task);
                task.setThread(thread);
                logger.info("task: {} assigned to thread: {}", task.subpartition(), thread.getName());
            }
        }
    }

    private void stopThreads() throws InterruptedException {
        for (WorkerThread<K, V> thread : threads) {
            thread.clearTasks();
        }

        // waits for all threads to stop because only then tasks can be rebalanced safely
        synchronized (rebalanceLock) {
            while (!allThreadsNotRunning()) {
                // timeout here is needed to check periodically whether some consumed records have timed out
                // without it a deadlock can happen (when some processing threads are blocked)
                rebalanceLock.wait(CHECK_TIMED_OUT_RECORDS_EVERY.toMillis());
                offsetsState.timeoutRecordsConsumedBefore(Instant.now().minus(consumerProcessingTimeout));
            }
        }
    }

    private void startThreads() {
        // notifies threads to continue tasks processing
        for (WorkerThread<K, V> thread : threads) {
            thread.notifyThread();
        }
    }

    private boolean allThreadsNotRunning() {
        return threads.stream().allMatch(WorkerThread::isNotRunning);
    }

}
