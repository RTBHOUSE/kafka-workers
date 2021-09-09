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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.annotation.Nullable;

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

    private final TaskManager<K, V> taskManager;
    private final QueuesManager<K, V> queueManager;
    private final OffsetsState offsetsState;

    private ThreadPoolExecutor executor;
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
        this.offsetsState = new DefaultOffsetsState(this.config, this.metrics);
        this.recordWeigher = new RecordWeigher<>(this.config.getRecordKeyWeigher(), this.config.getRecordValueWeigher());
        this.taskManager = new TaskManager<>(config, this.metrics, this.taskFactory, this.subpartitionSupplier,
                this.workerThreads, this.offsetsState);
        this.queueManager = new QueuesManager<>(config, this.metrics, this.subpartitionSupplier, this.taskManager,
                this.recordWeigher);
    }

    public void start() {
        setStatus(STARTING);
        logger.info("kafka workers starting");

        final int workerThreadsNum = config.getInt(WorkersConfig.WORKER_THREADS_NUM);
        consumerThread = new ConsumerThread<>(config, metrics, this, queueManager, subpartitionSupplier, offsetsState, recordWeigher);
        consumerThread.setDaemon(false);
        for (int i = 0; i < workerThreadsNum; i++) {
            workerThreads.add(new WorkerThread<>(i, config, metrics, this,  taskManager, queueManager, offsetsState));
        }
        punctuatorThread = new PunctuatorThread<>(config, metrics, this, workerThreads);
        punctuatorThread.setDaemon(false);

        executor = new ThreadPoolExecutor(workerThreadsNum, workerThreadsNum,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        consumerThread.start();
        for (WorkerThread<K, V> workerThread : workerThreads) {
            executor.execute(workerThread);
        }
        punctuatorThread.start();

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
            waitForShutdown();
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

        consumerThread.shutdown();
        punctuatorThread.shutdown();

        for (WorkerThread<K, V> workerThread : workerThreads) {
            workerThread.shutdown();
        }

        logger.info("executorService.shutdown()");
        executor.shutdown();

        Duration shutdownTimeout = config.getShutdownTimeout();
        Status terminalStatus;
        try {
            logger.info("executorService.awaitTermination({}s)", shutdownTimeout.toSeconds());
            if (executor.awaitTermination(shutdownTimeout.toMillis(), MILLISECONDS)) {
                terminalStatus = CLOSED_GRACEFULLY;
                logger.info("executorService terminated successfully with shutdown() method");
            } else {
                logger.warn("executorService not terminated within the given period (using shutdown() method)");
                logger.info("executorService.shutdownNow()");
                executor.shutdownNow();
                logger.info("executorService.awaitTermination({}s)", shutdownTimeout.toSeconds());
                if (executor.awaitTermination(shutdownTimeout.toMillis(), MILLISECONDS)) {
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

        consumerThread.allowToClose();

        try {
            Duration remainingThreadsWaitForClose = Stream.of(
                    config.getConsumerThreadClosingTimeout(), config.getPunctuatorThreadClosingTimeout())
                    .max(Comparator.naturalOrder())
                    .get()
                    .plus(5, ChronoUnit.SECONDS);

            CompletableFuture.allOf(
                    waitForCloseAsync(consumerThread, config.getConsumerThreadClosingTimeout()),
                    waitForCloseAsync(punctuatorThread, config.getPunctuatorThreadClosingTimeout())
            ).get(remainingThreadsWaitForClose.toMillis(), MILLISECONDS);
        } catch (InterruptedException e) {
            terminalStatus = CLOSING_INTERRUPTED;
            logger.error("interrupted", e);
        } catch (TimeoutException e) {
            terminalStatus = CANNOT_STOP_THREADS;
            logErrorIfThreadIsAlive(consumerThread);
            logErrorIfThreadIsAlive(punctuatorThread);
        } catch (Exception e) {
            logger.error("Waiting for closing threads failed", e);
        }

        if (callback != null) {
            callback.onShutdown(exception);
        }

        setStatus(terminalStatus);
        if (executor.isTerminated()) {
            workerThreads.clear();
        } else {
            logger.error("Couldn't stop [{}] worker thread(s)", executor.getActiveCount());
        }
        logger.info("kafka workers closed with status: {}", status);

        synchronized (shutdownLock) {
            shutdownLock.notifyAll();
        }
    }

    private void logErrorIfThreadIsAlive(Thread thread) {
        if (thread.isAlive()) {
            logger.error("Couldn't stop [{}]", thread.getName());
        }
    }

    private CompletableFuture<Void> waitForCloseAsync(AbstractWorkersThread thread, Duration totalTimeout) {

        Duration singleJoinTimeout = totalTimeout.dividedBy(2);

        return CompletableFuture.runAsync(() -> {
            try {
                thread.join(singleJoinTimeout.toMillis());
            } catch (InterruptedException e) {
                logger.error("interrupted", e);
            }

            if (!thread.isAlive()) {
                return;
            }

            logger.warn("Thread [{}] has not finished in {}s (calling interrupt).", thread.getName(), singleJoinTimeout.toSeconds());
            thread.interrupt();

            try {
                thread.join(singleJoinTimeout.toMillis());
            } catch (InterruptedException e) {
                logger.error("interrupted", e);
            }

            if (!thread.isAlive()) {
                return;
            }

            logger.warn("Thread [{}] is still alive {}s after interruption.", thread.getName(), singleJoinTimeout.toSeconds());

            // last join without timeout (handling TimeoutException outside)
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.error("interrupted", e);
            }
        });
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

    public Status getStatus() {
        return status;
    }

    public Status waitForShutdown() {
        Instant closingStartedAt = null;
        Duration shutdownTotalLimit = config.getShutdownTimeout().multipliedBy(2) // shutdown + shutdownNow
                .plus(config.getConsumerThreadClosingTimeout())
                .plus(config.getPunctuatorThreadClosingTimeout());
        synchronized (shutdownLock) {
            while (!status.isTerminal()
                    && !timedOut(closingStartedAt, shutdownTotalLimit)
                    && shutdownThread.isAlive()) {
                if (status.equals(CLOSING) && closingStartedAt == null) {
                    closingStartedAt = Instant.now();
                }
                try {
                    // timeout is needed to check whether a shutdownThread is still alive
                    shutdownLock.wait(10_000L);
                } catch (InterruptedException e) {
                    logger.error("interrupted", e);
                }
            }
        }

        if (!status.isTerminal()) {
            logger.error("[{}, alive={}] has not set a terminal status [status={}]",
                    shutdownThread.getName(), shutdownThread.isAlive(), status);
        }

        return status;
    }

    private boolean timedOut(@Nullable Instant startedAt, Duration timeout) {
        if (startedAt == null) {
            return false;
        }

        return !Instant.now().isBefore(startedAt.plus(timeout));
    }
}
