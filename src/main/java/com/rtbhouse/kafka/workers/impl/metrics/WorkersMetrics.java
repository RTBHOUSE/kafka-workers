package com.rtbhouse.kafka.workers.impl.metrics;

import java.util.Collection;
import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

public class WorkersMetrics {

    public static final String JMX_PREFIX = "kafka.workers";

    public static final String PAUSED_PARTITIONS_METRIC = "consumer-thread.paused-partitions";
    public static final String CONSUMED_OFFSET_METRIC = "consumer-thread.consumed-offset";
    public static final String COMMITTED_OFFSET_METRIC = "consumer-thread.committed-offset";

    public static final String ACCEPTING_OFFSET_METRIC = "worker-thread.accepting-offset";
    public static final String ACCEPTED_OFFSET_METRIC = "worker-thread.accepted-offset";
    public static final String PROCESSING_OFFSET_METRIC = "worker-thread.processing-offset";
    public static final String PROCESSED_OFFSET_METRIC = "worker-thread.processed-offset";

    public static final String QUEUE_SIZE_METRIC = "queues-manager.queue-size";

    public static final String WORKER_THREAD_METRIC_GROUP = "worker-threads";
    public static final String WORKER_THREAD_COUNT_METRIC_NAME = "count";
    public static final String WORKER_THREAD_TASK_COUNT_METRIC_NAME = "task-count";

    private final Metrics metrics;

    public WorkersMetrics(WorkersConfig config) {
        List<MetricsReporter> reporters = config.getConfiguredInstances(WorkersConfig.METRIC_REPORTER_CLASSES, MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));
        this.metrics = new Metrics(new MetricConfig(), reporters, Time.SYSTEM);
    }

    public void addConsumerThreadMetrics(TopicPartition partition) {
        addSensor(PAUSED_PARTITIONS_METRIC, partition);
        addSensor(CONSUMED_OFFSET_METRIC, partition);
        addSensor(COMMITTED_OFFSET_METRIC, partition);
    }

    public void removeConsumerThreadMetrics(TopicPartition partition) {
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
        addSensor(name + "." + subpartition);
    }

    public void addSensor(String name) {
        Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName("value", name), new Value());
    }

    public void recordSensor(String name, TopicPartition partition, long value) {
        recordSensor(name + "." + partition, value);
    }

    public void recordSensor(String name, WorkerSubpartition subpartition, long value) {
        recordSensor(name + "." + subpartition, value);
    }

    public void recordSensor(String name, long value) {
        metrics.sensor(name).record(value);
    }

    public void removeSensor(String name, TopicPartition partition) {
        removeSensor(name + "." + partition);
    }

    public void removeSensor(String name, WorkerSubpartition subpartition) {
        removeSensor(name + "." + subpartition);
    }

    public void removeSensor(String name) {
        metrics.removeSensor(name);
    }

    public void addSizeMetric(String group, String name, Collection<?> collection) {
        metrics.addMetric(metrics.metricName(name, group),
                (config, now) -> collection.size());
    }

    public void removeSizeMetric(String group, String name) {
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

}
