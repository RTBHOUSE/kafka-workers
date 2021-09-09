package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkState;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetStatus.CONSUMED;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetStatus.PROCESSED;
import static com.rtbhouse.kafka.workers.impl.util.TimeUtils.age;
import static com.rtbhouse.kafka.workers.impl.util.TimeUtils.isOlderThan;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.impl.errors.BadOffsetException;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingTimeoutException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.ConsumedOffsets.ConsumedOffsetRange;
import com.rtbhouse.kafka.workers.impl.range.ClosedRange;
import com.rtbhouse.kafka.workers.impl.range.SortedRanges;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class DefaultOffsetsState implements OffsetsState {

    private static final Logger logger = LoggerFactory.getLogger(DefaultOffsetsState.class);

    private final boolean computeMetricInfoEnabled;
    private final Duration metricInfoMaxDelay;
    private final Duration computeMetricsDurationWarn;
    private final Duration lastMetricInfoMaxAge;
    private final long lastMetricInfosMaxSize;
    private final WorkersMetrics metrics;

    private final ConcurrentHashMap<TopicPartition, ConsumedOffsets> consumedOffsetsMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<TopicPartition, SortedRanges> processedOffsetsMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<TopicPartition, TopicPartitionMetricInfo> currMetricInfos = new ConcurrentHashMap<>();

    // TODO: Deque inside is not thread-safe (there should be a single thread computing MetricInfos)
    private final ConcurrentHashMap<TopicPartition, Deque<TopicPartitionMetricInfo>> lastMetricInfos = new ConcurrentHashMap<>();

    public DefaultOffsetsState(WorkersConfig config, WorkersMetrics metrics) {
        this.metrics = metrics;

        this.computeMetricInfoEnabled = getBooleanFromConfig(config,"offsets-state.metric-infos.enabled", false);
        this.metricInfoMaxDelay = Duration.ofMillis(getLongFromConfig(config, "offsets-state.metric-infos.delay.ms", 1_000L));
        this.computeMetricsDurationWarn = Duration.ofMillis(getLongFromConfig(config, "offsets-state.metric-infos.compute.warn.ms",
                1_000L));
        this.lastMetricInfoMaxAge = Duration.ofMillis(getLongFromConfig(config, "offsets-state.last-metric-infos.max.age.ms",
                30_000L));
        this.lastMetricInfosMaxSize = getLongFromConfig(config, "offsets-state.last-metric-infos.max.size", 100L);
        checkState(!metricInfoMaxDelay.isNegative());
        checkState(!computeMetricsDurationWarn.isNegative());
    }

    private static boolean getBooleanFromConfig(WorkersConfig config, String key, boolean defaultValue) {
        return Optional
                .ofNullable(config.originals().get(key))
                .map(v -> Boolean.valueOf((String) v))
                .orElse(defaultValue);
    }

    private static long getLongFromConfig(WorkersConfig config, String key, long defaultValue) {
        return Optional
                .ofNullable(config.originals().get(key))
                .map(v -> Long.valueOf((String) v))
                .orElse(defaultValue);
    }

    public TopicPartitionMetricInfo getCurrMetricInfo(TopicPartition partition) {
        return currMetricInfos.computeIfAbsent(partition, this::createTopicPartitionMetricInfo);
    }

    public TopicPartitionMetricInfo getMaxMetricInfo(TopicPartition partition) {
        Deque<TopicPartitionMetricInfo> deque = lastMetricInfos.computeIfAbsent(partition,
                key -> new ArrayDeque<>(ImmutableList.of(getCurrMetricInfo(key))));
        // TODO: check whether synchronization on deques with MetricInfos is consistent and remove if not needed.
        synchronized (deque) {
            removeOldMetricInfos(deque);
            return deque.stream().max(this::cmpMetricInfoByNumRanges).orElseGet(() -> getCurrMetricInfo(partition));
        }
    }

    private void removeOldMetricInfos(Deque<TopicPartitionMetricInfo> deque) {
        while (deque.size() > lastMetricInfosMaxSize
                || (deque.peekFirst() != null && isOlderThan(deque.peekFirst().computedAt, lastMetricInfoMaxAge))) {
            deque.pollFirst();
        }
    }

    @Override
    public void register(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumedOffsetsMap.put(partition, new ConsumedOffsets());
            processedOffsetsMap.put(partition, new SortedRanges());

            if (computeMetricInfoEnabled) {
                metrics.addOffsetsStateCurrentMetrics(this, partition);
                metrics.addOffsetsStateMaxMetrics(this, partition);
            }
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumedOffsetsMap.remove(partition);
            processedOffsetsMap.remove(partition);

            if (computeMetricInfoEnabled) {
                metrics.removeOffsetsStateCurrentMetrics(partition);
                metrics.removeOffsetsStateMaxMetrics(partition);
            }
        }
    }

    @Override
    public void addConsumed(TopicPartition partition, ClosedRange range, Instant consumedAt) {
        ConsumedOffsets consumedOffsets = consumedOffsetsMap.get(partition);

        if (consumedOffsets == null) {
            logger.warn("Aborting addConsumed for partition [{}] (partition probably unregistered)", partition);
            return;
        }

        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (consumedOffsets) {
            Optional<Long> minExistingElement = consumedOffsets.getMinExistingElement(range);
            if (minExistingElement.isPresent()) {
                throw new BadOffsetException("Offset: " + minExistingElement.get() + " for partition: " + partition
                        + " was consumed before");
            }
            consumedOffsets.addConsumedRange(new ConsumedOffsetRange(range, consumedAt));
        }
    }

    @Override
    public void updateProcessed(TopicPartition partition, long offset) {
        ConsumedOffsets consumedOffsets = consumedOffsetsMap.get(partition);
        SortedRanges processedOffsets = processedOffsetsMap.get(partition);

        // unregister() method call may cause consumedOffsets or processedOffsets is null
        if (consumedOffsets == null) {
            logger.warn("Aborting updateProcessed({}, {}) because consumedOffsets == null (partition probably unregistered)",
                    partition, offset);
            return;
        }
        if (processedOffsets == null) {
            logger.warn("Aborting updateProcessed({}, {}) because processedOffsets == null (partition probably unregistered)",
                    partition, offset);
            return;
        }

        synchronized (consumedOffsets) {
            synchronized (processedOffsets) {
                if (processedOffsets.containsSingleElement(offset)) {
                    throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was processed before");
                }

                if (!consumedOffsets.contains(offset)) {
                    throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was not consumed before");
                }
                processedOffsets.addSingleElement(offset);
            }
        }
    }

    // TODO: there should be a separate thread computing MetricInfos and getters (getCurrMetricInfo, getMaxMetricInfo)
    //  should only read precomputed values (should not trigger computation).
    private void computeMetricInfo(TopicPartition partition) {
        if (!shouldComputeMetricInfo(partition)) {
            return;
        }

        TopicPartitionMetricInfo currInfo = createTopicPartitionMetricInfo(partition);
        Instant start = Instant.now().minus(Duration.ofMillis(currInfo.computationTimeMillis));
        currMetricInfos.put(partition, currInfo);
        lastMetricInfos.compute(partition, (key, value) ->
                Optional.ofNullable(value)
                        .map(v -> {
                            v.addLast(currInfo);
                            removeOldMetricInfos(v);
                            return v;
                        })
                        .orElseGet(() -> new ArrayDeque<>(ImmutableList.of(currInfo))));
        if (isOlderThan(start, computeMetricsDurationWarn)) {
            logger.warn("Computing MetricInfo for partition [{}] took too long [{}] ms",
                    partition, Duration.between(start, Instant.now()).toMillis());
        }
    }

    private TopicPartitionMetricInfo createTopicPartitionMetricInfo(TopicPartition partition) {
        TopicPartitionMetricInfo topicPartitionMetricInfo = this.new TopicPartitionMetricInfo(partition);
        metrics.recordSensor("offsets-state.topic-partition-metric-info.compute-time", topicPartitionMetricInfo.computationTimeMillis);
        return topicPartitionMetricInfo;
    }

    private boolean shouldComputeMetricInfo(TopicPartition partition) {
        if (!computeMetricInfoEnabled) {
            return false;
        }

        return Optional.ofNullable(lastMetricInfos.get(partition))
                .map(deque -> deque.isEmpty() || metricInfoMaxDelay.isZero() || isOlderThan(deque.getLast().computedAt, metricInfoMaxDelay))
                .orElse(true);
    }

    private int cmpMetricInfoByNumRanges(TopicPartitionMetricInfo info1, TopicPartitionMetricInfo info2) {

        int rangesCount1 = info1.offsetRangesStatusCounts.values().stream().mapToInt(count -> count).sum();
        int rangesCount2 = info2.offsetRangesStatusCounts.values().stream().mapToInt(count -> count).sum();

        return rangesCount1 - rangesCount2;
    }

    @Override
    public void timeoutRecordsConsumedBefore(Instant minConsumedAt) {
        for (TopicPartition partition : getPartitions()) {
            ConsumedOffsets consumedOffsets = consumedOffsetsMap.get(partition);
            SortedRanges processedOffsets = processedOffsetsMap.get(partition);

            if (consumedOffsets == null || processedOffsets == null) {
                return;
            }

            synchronized (consumedOffsets) {
                synchronized (processedOffsets) {
                    removeProcessedOffsetsFromHeadConsumedOffsets(consumedOffsets, processedOffsets);
                    consumedOffsets.getFirst().ifPresent(consumedFirstRange ->
                            checkConsumedOffsetsTimeout(partition, consumedFirstRange, minConsumedAt));
                }
            }
        }
    }

    private void checkConsumedOffsetsTimeout(TopicPartition partition,
            ConsumedOffsetRange consumedRange,
            Instant minConsumedAt) {

        if (minConsumedAt != null) {
            long minConsumedOffset = consumedRange.lowerEndpoint();
            Instant consumedAt = consumedRange.getConsumedAt();
            if (consumedAt.isBefore(minConsumedAt)) {
                throw new ProcessingTimeoutException(
                        String.format("Offset [%s] for partition [%s] exceeded timeout: consumedAt [%s], age [%s ms]",
                                minConsumedOffset, partition, consumedAt.toEpochMilli(), age(consumedAt).toMillis())
                );
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit() {

        ImmutableMap.Builder<TopicPartition, OffsetAndMetadata> builder = ImmutableMap.builder();

        for (TopicPartition partition : getPartitions()) {
            Long offsetToCommit = getOffsetToCommit(partition);
            if (offsetToCommit != null) {
                builder.put(partition, new OffsetAndMetadata(offsetToCommit + 1));
            }
        }

        return builder.build();
    }

    private Set<TopicPartition> getPartitions() {
        return ImmutableSet.copyOf(consumedOffsetsMap.keySet());
    }

    private Long getOffsetToCommit(TopicPartition partition) {
        ConsumedOffsets consumedOffsets = consumedOffsetsMap.get(partition);
        SortedRanges processedOffsets = processedOffsetsMap.get(partition);

        if (consumedOffsets == null || processedOffsets == null) {
            logger.warn("Aborting getOffsetToCommit for partition [{}] (partition probably unregistered), returning null", partition);
            return null;
        }

        synchronized (consumedOffsets) {
            synchronized (processedOffsets) {

                removeProcessedOffsetsFromHeadConsumedOffsets(consumedOffsets, processedOffsets);
                ConsumedOffsetRange consumedFirstRange = consumedOffsets.getFirst().orElse(null);

                if (consumedFirstRange != null) {
                    return processedOffsets.floorElement(consumedFirstRange.lowerEndpoint() - 1)
                            .orElse(null);
                } else {
                    return processedOffsets.getLast()
                            .map(ClosedRange::upperEndpoint)
                            .orElse(null);
                }
            }
        }
    }

    private void removeProcessedOffsetsFromHeadConsumedOffsets(ConsumedOffsets consumedOffsets, SortedRanges processedOffsets) {
        synchronized (consumedOffsets) {
            synchronized (processedOffsets) {
                for (ClosedRange processedOffset : processedOffsets) {
                    Optional<ClosedRange> removed = consumedOffsets.removeMaximumHeadRange(processedOffset);

                    if (removed.isEmpty() || removed.get().upperEndpoint() < processedOffset.upperEndpoint()) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {

        offsetsAndMetadata.forEach(this::removeCommitted);
    }

    private void removeCommitted(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        ConsumedOffsets consumedOffsets = consumedOffsetsMap.get(partition);
        SortedRanges processedOffsets = processedOffsetsMap.get(partition);
        if (consumedOffsets == null || processedOffsets == null) {
            logger.warn("Aborting removeCommitted for partition [{}] (partition probably unregistered)", partition);
            return;
        }

        synchronized (consumedOffsets) {
            synchronized (processedOffsets) {
                long maxOffsetToRemove = offsetAndMetadata.offset() - 1;
                consumedOffsets.removeElementsLowerOrEqual(maxOffsetToRemove);
                processedOffsets.removeElementsLowerOrEqual(maxOffsetToRemove);
            }
        }
    }

    @Override
    public long getProcessedUncommittedRecordsTotal() {
        return getProcessedUncommittedRecordsByTopic().values().stream().mapToLong(Long::longValue).sum();
    }

    private Map<TopicPartition, Long> getProcessedUncommittedRecordsByTopic() {
        return processedOffsetsMap.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                        entry -> entry.getValue().stream().mapToLong(ClosedRange::size).sum()));
    }

    public class TopicPartitionMetricInfo {

        private final Instant computedAt = Instant.now();
        private final Map<OffsetStatus, Long> offsetStatusCounts;
        private final Map<OffsetStatus, Integer> offsetRangesStatusCounts;
        private final long computationTimeMillis;

        TopicPartitionMetricInfo(TopicPartition partition) {
            ConsumedOffsets consumedRanges = consumedOffsetsMap.get(partition);
            SortedRanges processedRanges = processedOffsetsMap.get(partition);

            Object consumedRangesLock = consumedRanges != null ? consumedRanges : new Object();
            Object processedRangesLock = processedRanges != null ? processedRanges : new Object();

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (consumedRangesLock) {
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (processedRangesLock) {
                    long start = System.currentTimeMillis();
                    this.offsetStatusCounts = calculateOffsetStatusCounts(consumedRanges, processedRanges);
                    this.offsetRangesStatusCounts = calculateOffsetRangesStatusCounts(consumedRanges, processedRanges);
                    this.computationTimeMillis = System.currentTimeMillis() - start;
                }
            }
        }

        private Map<OffsetStatus, Integer> calculateOffsetRangesStatusCounts(ConsumedOffsets consumedRanges,
                                                                             SortedRanges processedOffsetRanges) {
            return ImmutableMap.of(
                    CONSUMED, Optional.ofNullable(consumedRanges).map(ConsumedOffsets::size).orElse(0),
                    PROCESSED, Optional.ofNullable(processedOffsetRanges).map(SortedRanges::size).orElse(0)
            );
        }

        private Map<OffsetStatus, Long> calculateOffsetStatusCounts(ConsumedOffsets consumedRanges,
                                                                    Collection<ClosedRange> processedRanges) {
            long consumedOffsetsCount = Optional.ofNullable(consumedRanges)
                    .map(ConsumedOffsets::getRanges)
                    .orElse(ImmutableList.of()).stream()
                        .mapToLong(ConsumedOffsetRange::size)
                        .sum();

            long processedOffsetsCount = Optional.ofNullable(processedRanges).orElse(ImmutableList.of()).stream()
                    .mapToLong(ClosedRange::size)
                    .sum();

            return ImmutableMap.of(
                CONSUMED, consumedOffsetsCount,
                PROCESSED, processedOffsetsCount
            );
        }

        public long getOffsetStatusCount(OffsetStatus status) {
            return offsetStatusCounts.getOrDefault(status, 0L);
        }

        public long getOffsetRangesStatusCount(OffsetStatus status) {
            return offsetRangesStatusCounts.getOrDefault(status, 0);
        }

        @Override
        public String toString() {
            return "TopicPartitionMetricInfo{"
                    + "offsetStatusCounts=" + offsetStatusCounts + ", "
                    + "offsetRangesStatusCounts=" + offsetRangesStatusCounts
                    + "}";
        }
    }

}
