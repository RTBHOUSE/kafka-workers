package com.rtbhouse.kafka.workers.impl;

import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CANNOT_STOP_THREADS;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CLOSED_GRACEFULLY;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CLOSED_NOT_GRACEFULLY;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CLOSING;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CLOSING_INTERRUPTED;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CREATED;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.SHUTDOWN;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.STARTED;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.STARTING;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.isTransitionAllowed;
import static com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics.WORKER_THREAD_COUNT_METRIC_NAME;
import static com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics.WORKER_THREAD_METRIC_GROUP;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.KafkaWorkers.Status;
import com.rtbhouse.kafka.workers.api.ShutdownCallback;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerPartitioner;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.impl.consumer.ConsumerThread;
import com.rtbhouse.kafka.workers.impl.errors.BadStatusException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.DefaultOffsetsState;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.punctuator.PunctuatorThread;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;
import com.rtbhouse.kafka.workers.impl.record.weigher.RecordWeigher;
import com.rtbhouse.kafka.workers.impl.task.TaskManager;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

public class KafkaWorkersImpl<K, V> implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(KafkaWorkersImpl.class);

    private final WorkersConfig config;
    private final WorkersMetrics metrics;
    private final WorkerTaskFactory<K, V> taskFactory;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final RecordWeigher<K, V> recordWeigher;
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

    private volatile Status status = CREATED;
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
        this.offsetsState = new DefaultOffsetsState(this.config, this.metrics);;
        this.recordWeigher = new RecordWeigher<>(this.config.getRecordKeyWeigher(), this.config.getRecordValueWeigher());
    }

    public void start() {
        setStatus(STARTING);
        logger.info("kafka workers starting");

        taskManager = new TaskManager<>(config, metrics, taskFactory, subpartitionSupplier, workerThreads);
        queueManager = new QueuesManager<>(config, metrics, subpartitionSupplier, taskManager, recordWeigher);

        final int workerThreadsNum = config.getInt(WorkersConfig.WORKER_THREADS_NUM);
        consumerThread = new ConsumerThread<>(config, metrics, this, queueManager, subpartitionSupplier, offsetsState, recordWeigher);
        for (int i = 0; i < workerThreadsNum; i++) {
            workerThreads.add(new WorkerThread<>(i, config, metrics, this,  taskManager, queueManager, offsetsState));
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

        setStatus(STARTED);
        logger.info("kafka workers started");

        metrics.addSizeMetric(WORKER_THREAD_METRIC_GROUP, WORKER_THREAD_COUNT_METRIC_NAME, workerThreads);

        shutdownThread = new ShutdownListenerThread(this);
        shutdownThread.start();
    }

    public void blockingShutdown() {
        logger.info("kafka workers blocking shutdown called");
        if (tryToSetStatus(SHUTDOWN)) {
            shutdownThread.shutdown();
            synchronized (shutdownLock) {
                while (!status.isTerminal()) {
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
        if (tryToSetStatus(SHUTDOWN)) {
            this.exception = exception;
            shutdownThread.shutdown();
        }
    }

    public void close() {
        setStatus(CLOSING);
        logger.info("kafka workers closing");

        metrics.removeMetric(WORKER_THREAD_METRIC_GROUP, WORKER_THREAD_COUNT_METRIC_NAME);

        // firstly stop threads processing
        consumerThread.shutdown();
        punctuatorThread.shutdown();
        for (WorkerThread<K, V> workerThread : workerThreads) {
            workerThread.shutdown();
        }

        logger.info("executorService.shutdown()");
        executorService.shutdown();

        Duration shutdownTimeout = config.getShutdownTimeout();
        Status terminalStatus;
        try {
            logger.info("executorService.awaitTermination({}s)", shutdownTimeout.toSeconds());
            if (executorService.awaitTermination(shutdownTimeout.toMillis(), MILLISECONDS)) {
                terminalStatus = CLOSED_GRACEFULLY;
                logger.info("executorService terminated successfully with shutdown() method");
            } else {
                logger.warn("executorService not terminated within the given period (using shutdown() method)");
                logger.info("executorService.shutdownNow()");
                executorService.shutdownNow();
                logger.info("executorService.awaitTermination({}s)", shutdownTimeout.toSeconds());
                if (executorService.awaitTermination(shutdownTimeout.toMillis(), MILLISECONDS)) {
                    terminalStatus = CLOSED_NOT_GRACEFULLY;
                    logger.info("executorService terminated successfully with shutdownNow() method");
                } else {
                    terminalStatus = CANNOT_STOP_THREADS;
                    logger.error("executorService not terminated within the given period (using shutdownNow() method)");
                }
            }
        } catch (InterruptedException e) {
            terminalStatus = CLOSING_INTERRUPTED;
            logger.error("interrupted", e);
        }

        // then close and clean up any pending resources
        for (WorkerThread<K, V> workerThread : workerThreads) {
            closeThreadResources(workerThread);
        }
        closeThreadResources(consumerThread);
        closeThreadResources(punctuatorThread);

        if (callback != null) {
            callback.onShutdown(exception);
        }

        setStatus(terminalStatus);
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
        consumerThread.unregister(partitions);
        offsetsState.unregister(partitions);
    }

    private void setStatus(Status newStatus) {
        if (!tryToSetStatus(newStatus)) {
            throw new BadStatusException("could not set: " + newStatus);
        }
    }

    private boolean tryToSetStatus(Status newStatus) {
        synchronized (statusLock) {
            Status oldStatus = status;
            if (isTransitionAllowed(oldStatus, newStatus)) {
                status = newStatus;
                logger.info("status changed, old: {}, new: {}", oldStatus, newStatus);
                return true;
            }
        }
        return false;
    }

    private void closeThreadResources(AbstractWorkersThread thread) {
        try {
            thread.close();
        } catch (Exception e) {
            logger.warn("caught exception while closing resources for thread: {}", thread,  e);
        }
    }

    public Status getStatus() {
        return status;
    }
}
