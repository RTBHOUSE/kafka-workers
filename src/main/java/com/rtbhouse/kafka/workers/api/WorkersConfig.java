package com.rtbhouse.kafka.workers.api;

import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.MetricsReporter;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.impl.consumer.ConsumerThread;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

/**
 * {@link KafkaWorkers} configuration
 */
public class WorkersConfig extends AbstractConfig {

    public static final long MS_PER_SECOND = ChronoUnit.SECONDS.getDuration().toMillis();
    public static final long MS_PER_MINUTE = ChronoUnit.MINUTES.getDuration().toMillis();
    public static final long MS_PER_HOUR = ChronoUnit.HOURS.getDuration().toMillis();

    /**
     * Prefix for internal {@link KafkaConsumer} configuration used by {@link ConsumerThread}.
     */
    public static final String CONSUMER_PREFIX = "consumer.kafka.";

    /**
     * Prefix for internal {@link WorkerTask} configuration.
     */
    public static final String WORKER_TASK_PREFIX = "worker.task.";

    /**
     * List of kafka topics read by {@link ConsumerThread}.
     */
    public static final String CONSUMER_TOPICS = "consumer.topics";
    private static final String CONSUMER_TOPICS_DOC = "List of kafka topics read by ConsumerThread.";

    /**
     * Timeout in milliseconds for {@link KafkaConsumer}'s poll().
     */
    public static final String CONSUMER_POLL_TIMEOUT_MS = "consumer.poll.timeout.ms";
    private static final String CONSUMER_POLL_TIMEOUT_MS_DOC = "Timeout in milliseconds for KafkaConsumer's poll().";
    private static final long CONSUMER_POLL_TIMEOUT_MS_DEFAULT = MS_PER_SECOND;

    /**
     * Interval in milliseconds with which to commit processed offsets.
     */
    public static final String CONSUMER_COMMIT_INTERVAL_MS = "consumer.commit.interval.ms";
    private static final String CONSUMER_COMMIT_INTERVAL_MS_DOC = "Interval in milliseconds with which to commit processed offsets.";
    private static final long CONSUMER_COMMIT_INTERVAL_MS_DEFAULT = 10 * MS_PER_SECOND;

    /**
     * Timeout in milliseconds for record to be successfully processed.
     */
    public static final String CONSUMER_PROCESSING_TIMEOUT_MS = "consumer.processing.timeout.ms";
    private static final String CONSUMER_PROCESSING_TIMEOUT_MS_DOC = "Timeout in milliseconds for record to be successfully processed.";
    private static final Long CONSUMER_PROCESSING_TIMEOUT_MS_DEFAULT = 5 * MS_PER_MINUTE;

    /**
     * Number of {@link WorkerThread}s per one {@link KafkaWorkers} instance.
     */
    public static final String WORKER_THREADS_NUM = "worker.threads.num";
    private static final String WORKER_THREADS_NUM_DOC = "Number of WorkerThreads per one Kafka Workers instance.";
    private static final int WORKER_THREADS_NUM_DEFAULT = 1;

    /**
     * Sleep time in milliseconds for {@link WorkerThread} in case of not accepted tasks.
     */
    public static final String WORKER_SLEEP_MS = "worker.sleep.ms";
    private static final String WORKER_SLEEP_MS_DOC = "Sleep time in milliseconds for WorkerThread in case of not accepted tasks.";
    private static final long WORKER_SLEEP_MS_DEFAULT = MS_PER_SECOND;

    /**
     * Max number of {@link WorkerRecord}s in {@link WorkerSubpartition}'s internal queue.
     */
    public static final String QUEUE_MAX_SIZE = "queue.max.size";
    private static final String QUEUE_MAX_SIZE_DOC = "Max number of WorkerRecords in WorkerSubpartition's internal queue.";
    private static final int QUEUE_MAX_SIZE_DEFAULT = 10000;

    /**
     * List of {@link MetricsReporter}s which report {@code KafkaWorkers}'s metrics.
     */
    public static final String METRIC_REPORTER_CLASSES = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
    private static final String METRIC_REPORTER_CLASSES_DOC = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;
    private static final String METRIC_REPORTER_CLASSES_DEFAULT = "";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(CONSUMER_TOPICS,
                        Type.LIST,
                        Importance.HIGH,
                        CONSUMER_TOPICS_DOC)
                .define(CONSUMER_POLL_TIMEOUT_MS,
                        Type.LONG,
                        CONSUMER_POLL_TIMEOUT_MS_DEFAULT,
                        Importance.LOW,
                        CONSUMER_POLL_TIMEOUT_MS_DOC)
                .define(CONSUMER_COMMIT_INTERVAL_MS,
                        Type.LONG,
                        CONSUMER_COMMIT_INTERVAL_MS_DEFAULT,
                        Importance.MEDIUM,
                        CONSUMER_COMMIT_INTERVAL_MS_DOC)
                .define(CONSUMER_PROCESSING_TIMEOUT_MS,
                        Type.LONG,
                        CONSUMER_PROCESSING_TIMEOUT_MS_DEFAULT,
                        Importance.MEDIUM,
                        CONSUMER_PROCESSING_TIMEOUT_MS_DOC)
                .define(WORKER_THREADS_NUM,
                        Type.INT,
                        WORKER_THREADS_NUM_DEFAULT,
                        Importance.HIGH,
                        WORKER_THREADS_NUM_DOC)
                .define(WORKER_SLEEP_MS,
                        Type.LONG,
                        WORKER_SLEEP_MS_DEFAULT,
                        Importance.MEDIUM,
                        WORKER_SLEEP_MS_DOC)
                .define(QUEUE_MAX_SIZE,
                        Type.INT,
                        QUEUE_MAX_SIZE_DEFAULT,
                        Importance.MEDIUM,
                        QUEUE_MAX_SIZE_DOC)
                .define(METRIC_REPORTER_CLASSES,
                        Type.LIST,
                        METRIC_REPORTER_CLASSES_DEFAULT,
                        Importance.LOW,
                        METRIC_REPORTER_CLASSES_DOC);
    }

    public WorkersConfig(final Map<?, ?> props) {
        super(CONFIG, props);
    }

    public Map<String, Object> getConsumerConfigs() {
        return originalsWithPrefix(CONSUMER_PREFIX);
    }

    public Map<String, Object> getWorkerTaskConfigs() {
        return originalsWithPrefix(WORKER_TASK_PREFIX);
    }

}
