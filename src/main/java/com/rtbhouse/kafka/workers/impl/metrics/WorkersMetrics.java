package com.rtbhouse.kafka.workers.impl.metrics;

import static com.google.common.base.Preconditions.checkState;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetStatus.CONSUMED;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetStatus.PROCESSED;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;

import com.google.common.collect.ImmutableList;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.impl.offsets.DefaultOffsetsState;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

public class WorkersMetrics {

    public static final String JMX_PREFIX = "kafka.workers";

    public static final String PAUSED_PARTITIONS_METRIC = "consumer-thread.paused-partitions";
    public static final String CONSUMED_OFFSET_METRIC = "consumer-thread.consumed-offset";
    public static final String COMMITTED_OFFSET_METRIC = "consumer-thread.committed-offset";

    public static final String INPUT_RECORDS_SIZE_SENSOR = "consumer-thread.input-records.serialized-size";
    public static final String KAFKA_POLL_RECORDS_COUNT_SENSOR = "consumer-thread.poll.records.count";
    public static final String KAFKA_POLL_RECORDS_SIZE_SENSOR = "consumer-thread.poll.records.serialized-size";

    public static final String ACCEPTING_OFFSET_METRIC = "worker-thread.accepting-offset";
    public static final String ACCEPTED_OFFSET_METRIC = "worker-thread.accepted-offset";
    public static final String PROCESSING_OFFSET_METRIC = "worker-thread.processing-offset";
    public static final String PROCESSED_OFFSET_METRIC = "worker-thread.processed-offset";

    public static final String QUEUES_TOTAL_SIZE_LIMIT_METRIC = "queues-manager.queues-total-size-limit.bytes";
    public static final String QUEUES_TOTAL_SIZE_METRIC = "queues-manager.queues-total-size.bytes";
    public static final String QUEUE_SIZE_LIMIT_METRIC = "queues-manager.queue-size-limit.bytes";

    public static final String WORKER_THREAD_METRIC_GROUP = "worker-threads";
    public static final String WORKER_THREAD_COUNT_METRIC_NAME = "count";
    public static final String WORKER_THREAD_TASK_COUNT_METRIC_NAME = "task-count";

    public static final String OFFSETS_CONSUMED_COUNT = "offsets.consumed.count";
    public static final String OFFSETS_PROCESSED_COUNT = "offsets.processed.count";
    public static final String OFFSET_RANGES_CONSUMED_COUNT = "offset-ranges.consumed.count";
    public static final String OFFSET_RANGES_PROCESSED_COUNT = "offset-ranges.processed.count";

    public static final String METRIC_INFO_COMPUTE_TIME_METRIC = "offsets-state.topic-partition-metric-info.compute-time";

    private static final List<String> ALL_OFFSETS_STATE_METRIC_NAMES = ImmutableList.of(
            OFFSETS_CONSUMED_COUNT,
            OFFSETS_PROCESSED_COUNT,
            OFFSET_RANGES_CONSUMED_COUNT,
            OFFSET_RANGES_PROCESSED_COUNT
    );

    private final Metrics metrics;

    public WorkersMetrics(WorkersConfig config) {
        List<MetricsReporter> reporters = config.getConfiguredInstances(WorkersConfig.METRIC_REPORTER_CLASSES, MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));
        this.metrics = new Metrics(new MetricConfig(), reporters, Time.SYSTEM);
    }

    public void addConsumerThreadMetrics() {
        Stream.of(
                metrics.sensor(INPUT_RECORDS_SIZE_SENSOR),
                metrics.sensor(KAFKA_POLL_RECORDS_COUNT_SENSOR),
                metrics.sensor(KAFKA_POLL_RECORDS_SIZE_SENSOR)
        ).forEach(
                sensor -> {
                    checkState(sensor.add(metrics.metricName("min", sensor.name()), new Min()));
                    checkState(sensor.add(metrics.metricName("max", sensor.name()), new Max()));
                    checkState(sensor.add(metrics.metricName("avg", sensor.name()), new Avg()));
                    checkState(sensor.add(metrics.metricName("count-per-sec", sensor.name()), new Rate(TimeUnit.SECONDS)));
                }
        );
    }

    public void removeConsumerThreadMetrics() {
        Stream.of(
                INPUT_RECORDS_SIZE_SENSOR,
                KAFKA_POLL_RECORDS_COUNT_SENSOR,
                KAFKA_POLL_RECORDS_SIZE_SENSOR
        ).forEach(metrics::removeSensor);
    }

    public void addConsumerThreadPartitionMetrics(TopicPartition partition) {
        addSensor(PAUSED_PARTITIONS_METRIC, partition);
        addSensor(CONSUMED_OFFSET_METRIC, partition);
        addSensor(COMMITTED_OFFSET_METRIC, partition);
    }

    public void removeConsumerThreadPartitionMetrics(TopicPartition partition) {
        removeSensor(PAUSED_PARTITIONS_METRIC, partition);
        removeSensor(CONSUMED_OFFSET_METRIC, partition);
        removeSensor(COMMITTED_OFFSET_METRIC, partition);
    }

    public void addWorkerThreadSubpartitionMetrics(WorkerSubpartition subpartition) {
        addSensor(ACCEPTING_OFFSET_METRIC, subpartition);
        addSensor(ACCEPTED_OFFSET_METRIC, subpartition);
        addSensor(PROCESSING_OFFSET_METRIC, subpartition);
        addSensor(PROCESSED_OFFSET_METRIC, subpartition);
    }

    public void removeWorkerThreadSubpartitionMetrics(WorkerSubpartition subpartition) {
        removeSensor(ACCEPTING_OFFSET_METRIC, subpartition);
        removeSensor(ACCEPTED_OFFSET_METRIC, subpartition);
        removeSensor(PROCESSING_OFFSET_METRIC, subpartition);
        removeSensor(PROCESSED_OFFSET_METRIC, subpartition);
    }

    public void addSensor(String name, TopicPartition partition) {
        addSensor(name + "." + partition);
    }

    public void addSensor(String name, WorkerSubpartition subpartition) {
        addSensor(nameWithSubpartition(name, subpartition));
    }

    private String nameWithSubpartition(String name, WorkerSubpartition subpartition) {
        return String.format("%s.%s.%d", name, subpartition.topicPartition(), subpartition.subpartition());
    }

    public void addSensor(String name) {
        Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName("value", name), new Value());
    }

    public void recordSensor(String name, TopicPartition partition, long value) {
        recordSensor(name + "." + partition, value);
    }

    public void recordSensor(String name, WorkerSubpartition subpartition, long value) {
        recordSensor(nameWithSubpartition(name, subpartition), value);
    }

    public void recordSensor(String name, long value) {
        metrics.sensor(name).record(value);
    }

    public void removeSensor(String name, TopicPartition partition) {
        removeSensor(name + "." + partition);
    }

    public void removeSensor(String name, WorkerSubpartition subpartition) {
        removeSensor(nameWithSubpartition(name, subpartition));
    }

    public void removeSensor(String name) {
        metrics.removeSensor(name);
    }

    public void addSizeMetric(String group, String name, Collection<?> collection) {
        metrics.addMetric(metrics.metricName(name, group),
                (config, now) -> collection.size());
    }

    public void removeMetric(String group, String name) {
        metrics.removeMetric(metrics.metricName(name, group));
    }

    public <K, V> void addWorkerThreadMetrics(WorkerThread<K, V> workerThread) {
        String group = workerThreadGroup(workerThread);

        metrics.addMetric(metrics.metricName(WORKER_THREAD_TASK_COUNT_METRIC_NAME, group),
                (config, now) -> workerThread.getTasksCount());
    }

    private <K, V> String workerThreadGroup(WorkerThread<K, V> workerThread) {
        return WORKER_THREAD_METRIC_GROUP + "." + workerThread.getName();
    }

    public <K, V> void removeWorkerThreadMetrics(WorkerThread<K, V> workerThread) {
        String group = workerThreadGroup(workerThread);

        metrics.removeMetric(metrics.metricName(WORKER_THREAD_TASK_COUNT_METRIC_NAME, group));
    }

    public void addOffsetsStateCurrentMetrics(DefaultOffsetsState offsetsState, TopicPartition partition) {
        String group = offsetsStateCurrInfosPartitionGroup(partition);

        metrics.addMetric(metrics.metricName(OFFSETS_CONSUMED_COUNT, group),
                (config, now) -> offsetsState.getCurrMetricInfo(partition).getOffsetStatusCount(CONSUMED));
        metrics.addMetric(metrics.metricName(OFFSETS_PROCESSED_COUNT, group),
                (config, now) -> offsetsState.getCurrMetricInfo(partition).getOffsetStatusCount(PROCESSED));

        metrics.addMetric(metrics.metricName(OFFSET_RANGES_CONSUMED_COUNT, group),
                (config, now) -> offsetsState.getCurrMetricInfo(partition).getOffsetRangesStatusCount(CONSUMED));
        metrics.addMetric(metrics.metricName(OFFSET_RANGES_PROCESSED_COUNT, group),
                (config, now) -> offsetsState.getCurrMetricInfo(partition).getOffsetRangesStatusCount(PROCESSED));
    }

    private String offsetsStateCurrInfosPartitionGroup(TopicPartition partition) {
        return "offsets-state.curr-infos." + partition;
    }

    public void removeOffsetsStateCurrentMetrics(TopicPartition partition) {
        String group = offsetsStateCurrInfosPartitionGroup(partition);
        ALL_OFFSETS_STATE_METRIC_NAMES.forEach(name -> metrics.removeMetric(metrics.metricName(name, group)));
    }

    public void addOffsetsStateMaxMetrics(DefaultOffsetsState offsetsState, TopicPartition partition) {
        String group = offsetsStateMaxPartitionGroup(partition);

        metrics.addMetric(metrics.metricName(OFFSETS_CONSUMED_COUNT, group),
                (config, now) -> offsetsState.getMaxMetricInfo(partition).getOffsetStatusCount(CONSUMED));
        metrics.addMetric(metrics.metricName(OFFSETS_PROCESSED_COUNT, group),
                (config, now) -> offsetsState.getMaxMetricInfo(partition).getOffsetStatusCount(PROCESSED));

        metrics.addMetric(metrics.metricName(OFFSET_RANGES_CONSUMED_COUNT, group),
                (config, now) -> offsetsState.getMaxMetricInfo(partition).getOffsetRangesStatusCount(CONSUMED));
        metrics.addMetric(metrics.metricName(OFFSET_RANGES_PROCESSED_COUNT, group),
                (config, now) -> offsetsState.getMaxMetricInfo(partition).getOffsetRangesStatusCount(PROCESSED));
    }

    private String offsetsStateMaxPartitionGroup(TopicPartition partition) {
        return "offsets-state.max-ranges." + partition;
    }

    public void removeOffsetsStateMaxMetrics(TopicPartition partition) {
        String group = offsetsStateMaxPartitionGroup(partition);
        ALL_OFFSETS_STATE_METRIC_NAMES.forEach(name -> metrics.removeMetric(metrics.metricName(name, group)));
    }

    public <K, V> void addQueuesManagerMetrics(QueuesManager<K, V> queuesManager) {
        addSensor(QUEUES_TOTAL_SIZE_LIMIT_METRIC);
        addSensor(QUEUE_SIZE_LIMIT_METRIC);
        metrics.addMetric(metrics.metricName(QUEUES_TOTAL_SIZE_METRIC, ""),
                (conf, now) -> queuesManager.getTotalSizeInBytes());
    }

    public void addOffsetsStateMetrics() {
        Sensor sensor = metrics.sensor(METRIC_INFO_COMPUTE_TIME_METRIC);
        checkState(sensor.add(metrics.metricName("min", sensor.name()), new Min()));
        checkState(sensor.add(metrics.metricName("max", sensor.name()), new Max()));
        checkState(sensor.add(metrics.metricName("avg", sensor.name()), new Avg()));
        checkState(sensor.add(metrics.metricName("count-per-sec", sensor.name()), new Rate(TimeUnit.SECONDS)));
    }
}
