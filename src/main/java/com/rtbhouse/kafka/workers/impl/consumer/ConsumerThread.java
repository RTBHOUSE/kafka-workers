package com.rtbhouse.kafka.workers.impl.consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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
import com.rtbhouse.kafka.workers.api.record.RecordProcessingGuarantee;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.AbstractWorkersThread;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;
import com.rtbhouse.kafka.workers.impl.range.RangeUtils;
import com.rtbhouse.kafka.workers.impl.record.weigher.RecordWeigher;

public class ConsumerThread<K, V> extends AbstractWorkersThread implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    // A rebalance occurs per consumer so it's a valid scenario when in a single KW app
    // consumer1 has partition P assigned and consumer2 has it revoked. As offsets and queues
    // are shared between consumers if consumer1 executes register code before consumer2 executes
    // unregister code an app may end up in state where OffsetManager and TaskManager don't see
    // partition P registered,
    // It's always accessed under synchronized block, so it might be an ordinary hash map
    private static final Map<TopicPartition, Set<ConsumerThread<?, ?>>> assignedConsumerThreads = new HashMap<>();

    private final Duration consumerPollTimeout;
    private final Duration consumerProcessingTimeout;
    private final long consumerCommitIntervalMs;

    private final QueuesManager<K, V> queuesManager;
    private final SubpartitionSupplier<K, V> subpartitionSupplier;
    private final OffsetsState offsetsState;
    private final KafkaConsumer<K, V> consumer;
    private final ConsumerRebalanceListenerImpl<K, V> listener;
    private final OffsetCommitCallback commitCallback;
    private final RecordWeigher<K, V> recordWeigher;

    private boolean waitBeforeClose = true;
    private final Object waitBeforeCloseLock = new Object();

    private long commitTime = System.currentTimeMillis();

    public ConsumerThread(
            WorkersConfig config,
            WorkersMetrics metrics,
            KafkaWorkersImpl<K, V> workers,
            QueuesManager<K, V> queuesManager,
            SubpartitionSupplier<K, V> subpartitionSupplier,
            OffsetsState offsetsState,
            RecordWeigher<K, V> recordWeigher, int index) {
        super("consumer-thread-" + index, config, metrics, workers);

        this.consumerPollTimeout = config.getConsumerPollTimeout();
        this.consumerProcessingTimeout = config.getConsumerProcessingTimeout();
        this.consumerCommitIntervalMs = config.getConsumerCommitIntervalMs();

        this.queuesManager = queuesManager;
        this.subpartitionSupplier = subpartitionSupplier;
        this.offsetsState = offsetsState;
        this.consumer = new KafkaConsumer<>(config.getConsumerConfigs());
        this.listener = new ConsumerRebalanceListenerImpl<>(workers, this);
        this.commitCallback = new OffsetCommitCallbackImpl(config, this, offsetsState, metrics);
        this.recordWeigher = recordWeigher;
    }

    @Override
    public void init() {
        metrics.addConsumerThreadMetrics();
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

        addConsumedRanges(records);

        long pollRecordsTotalSize = 0L;
        for (ConsumerRecord<K, V> record : records) {
            WorkerSubpartition subpartition = subpartitionSupplier.subpartition(record);
            WorkerRecord<K, V> workerRecord = new WorkerRecord<>(record, subpartition.subpartition());
            queuesManager.push(subpartition, workerRecord);
            pollRecordsTotalSize += recordWeigher.weigh(workerRecord);
            updateInputRecordMetrics(workerRecord);
        }
        metrics.recordSensor(WorkersMetrics.KAFKA_POLL_RECORDS_COUNT_SENSOR, records.count());
        metrics.recordSensor(WorkersMetrics.KAFKA_POLL_RECORDS_SIZE_SENSOR, pollRecordsTotalSize);

        Set<TopicPartition> partitionsToPause = queuesManager.getPartitionsToPause(consumer.assignment(),
                consumer.paused());
        if (!partitionsToPause.isEmpty()) {
            consumer.pause(partitionsToPause);
            logger.info("paused partitions: {}", partitionsToPause);
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
            timeoutRecords();
            commitAsync();
        }
    }

    private void updateInputRecordMetrics(WorkerRecord<K, V> workerRecord) {
        metrics.recordSensor(WorkersMetrics.CONSUMED_OFFSET_METRIC, workerRecord.topicPartition(), workerRecord.offset());
        metrics.recordSensor(WorkersMetrics.INPUT_RECORDS_SIZE_SENSOR, recordWeigher.weigh(workerRecord));
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
    protected void waitBeforeClose() throws InterruptedException {
        synchronized (waitBeforeCloseLock) {
            while (waitBeforeClose) {
                waitBeforeCloseLock.wait();
            }
        }
    }

    public void allowToClose() {
        synchronized (waitBeforeCloseLock) {
            waitBeforeClose = false;
            waitBeforeCloseLock.notify();
        }
    }

    @Override
    public void close() {
        commitSync();
        if (RecordProcessingGuarantee.AT_LEAST_ONCE.equals(config.getRecordProcessingGuarantee())) {
            logger.info("Uncommitted processed records (probably generating duplicates in the next run) = {}", offsetsState.getProcessedUncommittedRecordsTotal());
        }
        consumer.close();
        metrics.removeConsumerThreadMetrics();
    }

    @Override
    public void shutdown(WorkersException exception) {
        super.shutdown(exception);
        consumer.wakeup();
    }

    @Override
    public void register(Collection<TopicPartition> topicPartitions) {
        for (TopicPartition partition : topicPartitions) {
            metrics.addConsumerThreadPartitionMetrics(partition);
            assignedConsumerThreads.merge(partition, Set.of(this), (s1, s2) ->
                ImmutableSet.copyOf(Sets.union(s1, s2)));
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> topicPartitions) {
        // commits all processed records so far to avoid unnecessary work or too many duplicates after rebalance
        commitSync();

        for (TopicPartition partition : topicPartitions) {
            metrics.removeConsumerThreadPartitionMetrics(partition);
            assignedConsumerThreads.merge(partition, Set.of(), (s1, s2) ->
                ImmutableSet.copyOf(Sets.difference(s1, Set.of(this))));
        }
    }

    public void registerMutex(Collection<TopicPartition> topicPartitions, PartitionsConsumer registerConsumer) throws InterruptedException {
        synchronized (assignedConsumerThreads) {
            register(topicPartitions);
            registerConsumer.accept(topicPartitions);
        }
    }

    public void unregisterMutex(Collection<TopicPartition> topicPartitions,
                PartitionsConsumer actualTopicPartitionsToRevokeConsumer) throws InterruptedException {
        synchronized (assignedConsumerThreads) {
            Collection<TopicPartition> actualPartitionsToUnregister = getPartitionsToUnregister(topicPartitions);
            // only partitions which really might be unregistered might are passed downstream
            unregister(actualPartitionsToUnregister);
            actualTopicPartitionsToRevokeConsumer.accept(actualPartitionsToUnregister);
        }
    }

    public Collection<TopicPartition> getPartitionsToUnregister(Collection<TopicPartition> partitionRevokedForASingleConsumer) {
        return partitionRevokedForASingleConsumer.stream()
            .filter(topicPartition -> assignedConsumerThreads.get(topicPartition).size() == 1)
            .collect(Collectors.toList());
    }

    private void commitSync() {
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit();
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

    private void timeoutRecords() {
        Instant minConsumedAt = Instant.now().minus(consumerProcessingTimeout);
        offsetsState.timeoutRecordsConsumedBefore(minConsumedAt);
    }

    private void commitAsync() {
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit();
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
