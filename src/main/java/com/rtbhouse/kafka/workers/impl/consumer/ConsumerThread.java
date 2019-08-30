package com.rtbhouse.kafka.workers.impl.consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Streams;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.AbstractWorkersThread;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;
import com.rtbhouse.kafka.workers.impl.range.RangeUtils;

public class ConsumerThread<K, V> extends AbstractWorkersThread implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    private final Duration consumerPollTimeout;
    private final Duration consumerProcessingTimeout;
    private final long consumerCommitIntervalMs;

    private final QueuesManager<K, V> queuesManager;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final OffsetsState offsetsState;
    private final KafkaConsumer<K, V> consumer;
    private final ConsumerRebalanceListenerImpl<K, V> listener;
    private final OffsetCommitCallback commitCallback;

    private long commitTime = System.currentTimeMillis();

    public ConsumerThread(
            WorkersConfig config,
            WorkersMetrics metrics,
            KafkaWorkersImpl<K, V> workers,
            QueuesManager<K, V> queuesManager,
            SubpartitionSupplier<K, V> subpartitionSupplier,
            OffsetsState offsetsState) {
        super("consumer-thread", config, metrics, workers);

        this.consumerPollTimeout = Duration.ofMillis(config.getLong(WorkersConfig.CONSUMER_POLL_TIMEOUT_MS));
        this.consumerProcessingTimeout = Duration.ofMillis(config.getLong(WorkersConfig.CONSUMER_PROCESSING_TIMEOUT_MS));
        this.consumerCommitIntervalMs = config.getLong(WorkersConfig.CONSUMER_COMMIT_INTERVAL_MS);

        this.queuesManager = queuesManager;
        this.subpartitionSupplier = subpartitionSupplier;
        this.offsetsState = offsetsState;
        this.consumer = new KafkaConsumer<>(config.getConsumerConfigs());
        this.listener = new ConsumerRebalanceListenerImpl<>(workers);
        this.commitCallback = new OffsetCommitCallbackImpl(config, this, offsetsState, metrics);
    }

    @Override
    public void init() {
        consumer.subscribe(config.getList(WorkersConfig.CONSUMER_TOPICS), listener);
    }

    @Override
    public void process() {
        ConsumerRecords<K, V> records;
        try {
            records = consumer.poll(consumerPollTimeout);
        } catch (WakeupException e) {
            if (!shutdown) {
                throw new WorkersException("unexpected consumer wakeup", e);
            }
            logger.info("consumer wakeup because of shutdown");
            return;
        }
        listener.rethrowExceptionCaughtDuringRebalance();

        long currentTime = System.currentTimeMillis();
        addConsumedRanges(records);

        for (ConsumerRecord<K, V> record : records) {
            WorkerSubpartition subpartition = subpartitionSupplier.subpartition(record);
            //TODO: delete
            offsetsState.addConsumed(subpartition.topicPartition(), record.offset(), currentTime);
            queuesManager.push(subpartition, new WorkerRecord<>(record, subpartition.subpartition()));
            metrics.recordSensor(WorkersMetrics.CONSUMED_OFFSET_METRIC, subpartition.topicPartition(), record.offset());
        }

        Set<TopicPartition> partitionsToPause = queuesManager.getPartitionsToPause(consumer.assignment(),
                consumer.paused());
        if (!partitionsToPause.isEmpty()) {
            consumer.pause(partitionsToPause);
            logger.warn("paused partitions: {}", partitionsToPause);
            for (TopicPartition partition : partitionsToPause) {
                metrics.recordSensor(WorkersMetrics.PAUSED_PARTITIONS_METRIC, partition, 1L);
            }
        }

        Set<TopicPartition> partitionsToResume = queuesManager.getPartitionsToResume(consumer.paused());
        if (!partitionsToResume.isEmpty()) {
            consumer.resume(partitionsToResume);
            logger.info("resumed partitions: {}", partitionsToResume);
            for (TopicPartition partition : partitionsToResume) {
                metrics.recordSensor(WorkersMetrics.PAUSED_PARTITIONS_METRIC, partition, 0L);
            }
        }

        if (shouldCommitNow()) {
            commitAsync();
        }
    }

    private void addConsumedRanges(ConsumerRecords<K, V> records) {
        Instant consumedAt = Instant.now();

        @SuppressWarnings("UnstableApiUsage")
        Map<TopicPartition, List<Long>> offsetsMap = Streams.stream(records)
                .collect(Collectors.groupingBy(this::topicPartition, Collectors.mapping(
                        ConsumerRecord::offset,
                        Collectors.toList()
                )));

        offsetsMap.forEach((partition, offsets) -> {
            RangeUtils.rangesFromLongs(offsets).forEach(
                    range -> offsetsState.addConsumed(partition, range, consumedAt)
            );
        });
    }

    private TopicPartition topicPartition(ConsumerRecord<K, V> record) {
        return new TopicPartition(record.topic(), record.partition());
    }

    @Override
    public void close() {
        commitSync();
        consumer.close();
    }

    @Override
    public void shutdown(WorkersException exception) {
        super.shutdown(exception);
        consumer.wakeup();
    }

    @Override
    public void register(Collection<TopicPartition> topicPartitions) {
        for (TopicPartition partition : topicPartitions) {
            metrics.addConsumerThreadMetrics(partition);
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> topicPartitions) {
        // commits all processed records so far to avoid unnecessary work or too many duplicates after rebalance
        commitSync();

        for (TopicPartition partition : topicPartitions) {
            metrics.removeConsumerThreadMetrics(partition);
        }
    }

    private void commitSync() {
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(consumer.assignment(), null);
        logger.debug("committing offsets sync: {}", offsets);
        if (!offsets.isEmpty()) {
            try {
                consumer.commitSync(offsets);
            } catch (WakeupException e) {
                // this has to be repeated if consumer.wakeup() during thread shutdown hasn't woken up any pending poll
                // operation
                consumer.commitSync(offsets);
            }
        }
    }

    private void commitAsync() {
        Instant minCreatedAt = Instant.now().minus(consumerProcessingTimeout);
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(consumer.assignment(), minCreatedAt);
        logger.debug("committing offsets async: {}", offsets);
        if (!offsets.isEmpty()) {
            consumer.commitAsync(offsets, commitCallback);
        }
    }

    private boolean shouldCommitNow() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - commitTime > consumerCommitIntervalMs) {
            commitTime = currentTime;
            return true;
        }
        return false;
    }

}
