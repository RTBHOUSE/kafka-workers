package com.rtbhouse.kafka.workers.impl.task;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;

public class TaskManager<K, V> implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private final WorkersConfig config;
    private final WorkersMetrics metrics;
    private final WorkerTaskFactory<K, V> taskFactory;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final List<WorkerThread<K, V>> threads;

    private final Map<WorkerSubpartition, WorkerTaskImpl<K, V>> partitionToTaskMap = new ConcurrentHashMap<>();

    private final Object rebalanceLock = new Object();

    public TaskManager(
            WorkersConfig config,
            WorkersMetrics metrics,
            WorkerTaskFactory<K, V> taskFactory,
            SubpartitionSupplier<K, V> subpartitionSupplier,
            List<WorkerThread<K, V>> threads) {
        this.config = config;
        this.metrics = metrics;
        this.taskFactory = taskFactory;
        this.subpartitionSupplier = subpartitionSupplier;
        this.threads = threads;
    }

    @Override
    public void register(Collection<TopicPartition> topicPartitions) throws InterruptedException {
        stopThreads();

        for (WorkerSubpartition subpartition : subpartitionSupplier.subpartitions(topicPartitions)) {
            WorkerTaskImpl<K, V> task = new WorkerTaskImpl<>(taskFactory.createTask(config, subpartition), metrics);
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

    private void rebalanceTasks() throws InterruptedException {
        int index = 0;
        for (WorkerTaskImpl<K, V> task : partitionToTaskMap.values()) {
            threads.get(index).addTask(task);
            task.setThread(threads.get(index));
            logger.info("task: {} assigned to thread: {}", task.subpartition(), threads.get(index).getName());

            // round-robin assignment
            index = (index + 1) % threads.size();
        }
    }

    private void stopThreads() throws InterruptedException {
        for (WorkerThread<K, V> thread : threads) {
            thread.clearTasks();
        }

        // waits for all threads to stop because only then could do tasks rebalance safely
        synchronized (rebalanceLock) {
            while (!allThreadsStopped()) {
                rebalanceLock.wait();
            }
        }
    }

    private void startThreads() {
        // notifies threads to continue tasks processing
        for (WorkerThread<K, V> thread : threads) {
            thread.notifyThread();
        }
    }

    private boolean allThreadsStopped() {
        return threads.stream().allMatch(WorkerThread::isNotRunning);
    }

}
