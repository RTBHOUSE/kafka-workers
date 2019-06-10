package com.rtbhouse.kafka.workers.impl;

import static com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics.WORKER_THREAD_COUNT_METRIC_NAME;
import static com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics.WORKER_THREAD_METRIC_GROUP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.rtbhouse.kafka.workers.impl.punctuator.PunctuatorThread;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.ShutdownCallback;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerPartitioner;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.impl.consumer.ConsumerThread;
import com.rtbhouse.kafka.workers.impl.errors.BadStatusException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;
import com.rtbhouse.kafka.workers.impl.task.TaskManager;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

public class KafkaWorkersImpl<K, V> implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(KafkaWorkersImpl.class);

    private enum Status {
        CREATED, STARTING, STARTED, SHUTDOWN, CLOSING, CLOSED
    }

    private final WorkersConfig config;
    private final WorkersMetrics metrics;
    private final WorkerTaskFactory<K, V> taskFactory;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final ShutdownCallback callback;

    private TaskManager<K, V> taskManager;
    private QueuesManager<K, V> queueManager;
    private final OffsetsState offsetsState;

    private ExecutorService executorService;
    private final List<WorkerThread<K, V>> workerThreads = new ArrayList<>();
    private PunctuatorThread<K, V> punctuatorThread;
    private ConsumerThread<K, V> consumerThread;

    private ShutdownListenerThread shutdownThread;
    private final Object shutdownLock = new Object();

    private volatile Status status = Status.CREATED;
    private final Object statusLock = new Object();

    private WorkersException exception;

    public KafkaWorkersImpl(
            WorkersConfig config,
            WorkerTaskFactory<K, V> taskFactory,
            WorkerPartitioner<K, V> partitioner,
            ShutdownCallback callback) {
        this.config = config;
        this.metrics = new WorkersMetrics(config);
        this.taskFactory = taskFactory;
        this.subpartitionSupplier = new SubpartitionSupplier<>(partitioner);
        this.callback = callback;
        this.offsetsState = new OffsetsState();
    }

    public void start() {
        setStatus(Status.STARTING);
        logger.info("kafka workers starting");

        taskManager = new TaskManager<>(config, metrics, this, taskFactory, subpartitionSupplier, workerThreads, offsetsState);
        queueManager = new QueuesManager<>(config, metrics, subpartitionSupplier, taskManager);

        final int workerThreadsNum = config.getInt(WorkersConfig.WORKER_THREADS_NUM);
        consumerThread = new ConsumerThread<>(config, metrics, this, queueManager, subpartitionSupplier, offsetsState);
        for (int i = 0; i < workerThreadsNum; i++) {
            workerThreads.add(new WorkerThread<>(i, config, metrics, this,  taskManager, queueManager));
        }
        punctuatorThread = new PunctuatorThread<>(config, metrics, this, workerThreads);

        // number of threads includes:
        // - configurable amount of worker threads
        // - plus one consumer thread
        // - plus one punctuator thread
        final int allThreadsNum = workerThreadsNum + 2;
        executorService = Executors.newFixedThreadPool(allThreadsNum);
        executorService.execute(consumerThread);
        for (WorkerThread<K, V> workerThread : workerThreads) {
            executorService.execute(workerThread);
        }
        executorService.execute(punctuatorThread);

        setStatus(Status.STARTED);
        logger.info("kafka workers started");

        metrics.addSizeMetric(WORKER_THREAD_METRIC_GROUP, WORKER_THREAD_COUNT_METRIC_NAME, workerThreads);

        shutdownThread = new ShutdownListenerThread(this);
        shutdownThread.start();
    }

    public void blockingShutdown() {
        logger.info("kafka workers blocking shutdown called");
        if (tryToSetStatus(Status.SHUTDOWN)) {
            shutdownThread.shutdown();
            synchronized (shutdownLock) {
                while (status != Status.CLOSED) {
                    try {
                        shutdownLock.wait();
                    } catch (InterruptedException e) {
                        logger.error("interrupted", e);
                    }
                }
            }
        }
    }

    public void shutdown(WorkersException exception) {
        logger.info("kafka workers shutdown called");
        if (tryToSetStatus(Status.SHUTDOWN)) {
            this.exception = exception;
            shutdownThread.shutdown();
        }
    }

    public void close() {
        setStatus(Status.CLOSING);
        logger.info("kafka workers closing");

        metrics.removeSizeMetric(WORKER_THREAD_METRIC_GROUP, WORKER_THREAD_COUNT_METRIC_NAME);

        consumerThread.shutdown();
        punctuatorThread.shutdown();
        for (WorkerThread<K, V> workerThread : workerThreads) {
            workerThread.shutdown();
        }

        logger.info("executor service closing");
        executorService.shutdown();
        try {
            while (!executorService.awaitTermination(1L, TimeUnit.MINUTES)) {
                logger.error("still waiting for termination");
                // notifies TaskManager to wake up and check if threads are closed
                taskManager.notifyTaskManager();
            }
        } catch (InterruptedException e) {
            logger.error("interrupted: {}", e);
        }
        logger.info("executor service closed");

        if (callback != null) {
            callback.onShutdown(exception);
        }

        setStatus(Status.CLOSED);
        logger.info("kafka workers closed");

        synchronized (shutdownLock) {
            shutdownLock.notifyAll();
        }
    }

    @Override
    public void register(Collection<TopicPartition> partitions) throws InterruptedException {
        logger.info("partitions registered: {}", partitions);
        consumerThread.register(partitions);
        offsetsState.register(partitions);
        queueManager.register(partitions);
        taskManager.register(partitions);
    }

    @Override
    public void unregister(Collection<TopicPartition> partitions) throws InterruptedException {
        logger.info("partitions unregistered: {}", partitions);
        taskManager.unregister(partitions);
        queueManager.unregister(partitions);
        offsetsState.unregister(partitions);
        consumerThread.unregister(partitions);
    }

    private void setStatus(Status newStatus) {
        if (!tryToSetStatus(newStatus)) {
            throw new BadStatusException("could not set: " + newStatus);
        }
    }

    private boolean tryToSetStatus(Status newStatus) {
        synchronized (statusLock) {
            Status oldStatus = status;
            if (isValidChange(oldStatus, newStatus)) {
                status = newStatus;
                logger.info("status changed, old: {}, new: {}", oldStatus, newStatus);
                return true;
            }
        }
        return false;
    }

    private boolean isValidChange(Status oldStatus, Status newStatus) {
        if (oldStatus != null && oldStatus.ordinal() != newStatus.ordinal() - 1) {
            return false;
        } else if (oldStatus == null && newStatus.ordinal() != 0) {
            return false;
        }
        return true;
    }

}
