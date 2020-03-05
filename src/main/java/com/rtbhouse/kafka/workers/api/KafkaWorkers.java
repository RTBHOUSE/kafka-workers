package com.rtbhouse.kafka.workers.api;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.rtbhouse.kafka.workers.api.partitioner.DefaultPartitioner;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerPartitioner;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;

/**
 * {@code KafkaWorkers} is a client library which unifies records consuming from Kafka and processing them by
 * user-defined {@link WorkerTask}s. It supports at-least-once state and output semantics.
 * <p>
 * It provides:
 * <ol>
 * <li>higher level of distribution because of sub-partitioning defined by {@link WorkerPartitioner},</li>
 * <li>tighter control of offsets commits to Kafka applied by {@link RecordStatusObserver},</li>
 * <li>possibility to pause and resume processing by {@link WorkerTask} for given partition.</li>
 * </ol>
 * <p>
 * Internally one {@code KafkaWorkers} instance launches one consumer thread and configurable count of worker threads.
 * Each thread can execute one or more {@link WorkerTask}s and each {@link WorkerTask} processes {@link WorkerRecord}s
 * from internal queue associated with given {@link WorkerSubpartition}.
 * <p>
 * Usage example:
 * <pre>
 * {@code
 *     Properties properties = new Properties();
 *     properties.setProperty("consumer.topics", "my-topic");
 *     properties.setProperty("consumer.kafka.bootstrap.servers", "localhost:9192");
 *     properties.setProperty("consumer.kafka.group.id", "my-workers");
 *     properties.setProperty("consumer.kafka.key.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
 *     properties.setProperty("consumer.kafka.value.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
 *
 *     KafkaWorkers<String, String> kafkaWorkers = new KafkaWorkers<>(
 *             new WorkersConfig(properties),
 *             new MyWorkerTaskFactory<>(),
 *             new MyWorkerPartitioner<>(),
 *             new MyShutdownCallback());
 *
 *     Runtime.getRuntime().addShutdownHook(new Thread(kafkaWorkers::shutdown));
 *     kafkaWorkers.start();
 * }
 * </pre>
 */
public class KafkaWorkers<K, V> {

    public enum Status {
        CREATED, STARTING, STARTED, SHUTDOWN, CLOSING, CLOSED_GRACEFULLY, CLOSED_NOT_GRACEFULLY, CANNOT_STOP_THREADS, CLOSING_INTERRUPTED;

        private static Map<Status, Set<Status>> ALLOWED_TRANSITIONS = Map.of(
                CREATED, Set.of(STARTING),
                STARTING, Set.of(STARTED),
                STARTED, Set.of(SHUTDOWN),
                SHUTDOWN, Set.of(CLOSING),
                CLOSING, Set.of(CLOSED_GRACEFULLY, CLOSED_NOT_GRACEFULLY, CANNOT_STOP_THREADS, CLOSING_INTERRUPTED)
        );

        private static Set<Status> TERMINAL_STATUSES = EnumSet.complementOf(EnumSet.copyOf(ALLOWED_TRANSITIONS.keySet()));

        public static boolean isTransitionAllowed(Status from, Status to) {
            return Optional.ofNullable(from)
                    .map(ALLOWED_TRANSITIONS::get)
                    .map(allowed -> allowed.contains(to))
                    .orElse(to.equals(CREATED));
        }

        public boolean isTerminal() {
            return TERMINAL_STATUSES.contains(this);
        }
    }

    private final KafkaWorkersImpl<K, V> workers;

    /**
     * Creates a {@code KafkaWorkers} instance with default partitioner.
     *
     * @param config
     *            {@code KafkaWorkers} configuration
     * @param factory
     *            factory for creating {@link WorkerTask}s
     */
    public KafkaWorkers(WorkersConfig config, WorkerTaskFactory<K, V> factory) {
        this(config, factory, new DefaultPartitioner<>(), null);
    }

    /**
     * Creates a {@code KafkaWorkers} instance.
     *
     * @param config
     *            {@code KafkaWorkers} configuration
     * @param factory
     *            factory for creating {@link WorkerTask}s
     * @param partitioner
     *            partitioner used for additional sub-partitioning
     */
    public KafkaWorkers(WorkersConfig config, WorkerTaskFactory<K, V> factory, WorkerPartitioner<K, V> partitioner) {
        this(config, factory, partitioner, null);
    }

    /**
     * Creates a {@code KafkaWorkers} instance with default partitioner.
     *
     * @param config
     *            {@code KafkaWorkers} configuration
     * @param factory
     *            factory for creating {@link WorkerTask}s
     * @param callback
     *            callback used to implement custom actions in case of shutdown
     */
    public KafkaWorkers(WorkersConfig config, WorkerTaskFactory<K, V> factory, ShutdownCallback callback) {
        this(config, factory, new DefaultPartitioner<>(), callback);
    }

    /**
     * Creates a {@code KafkaWorkers} instance.
     *
     * @param config
     *            {@code KafkaWorkers} configuration
     * @param factory
     *            factory for creating {@link WorkerTask}s
     * @param partitioner
     *            partitioner used for additional sub-partitioning
     * @param callback
     *            callback used to implement custom actions in case of shutdown
     */
    public KafkaWorkers(
            WorkersConfig config,
            WorkerTaskFactory<K, V> factory,
            WorkerPartitioner<K, V> partitioner,
            ShutdownCallback callback) {
        this.workers = new KafkaWorkersImpl<>(config, factory, partitioner, callback);
    }

    /**
     * Starts the {@code KafkaWorkers} instance by launching consumer and worker threads asynchronously. It is expected
     * to be called only once during the life cycle of the {@link KafkaWorkers} instance.
     */
    public void start() {
        workers.start();
    }

    /**
     * Closes the {@code KafkaWorkers} instance gracefully by closing consumer and worker threads. It is a blocking
     * method and {@code ShutdownCallback} will be called first. However, any uncaught exception thrown in any of
     * background threads will cause instance shutdown too.
     */
    public void shutdown() {
        workers.blockingShutdown();
    }

    public Status getStatus() {
        return workers.getStatus();
    }
}
