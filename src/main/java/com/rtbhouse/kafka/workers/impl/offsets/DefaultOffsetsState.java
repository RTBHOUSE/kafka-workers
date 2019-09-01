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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.impl.errors.BadOffsetException;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingTimeoutException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.range.ClosedRange;
import com.rtbhouse.kafka.workers.impl.range.SortedRanges;

public class DefaultOffsetsState implements OffsetsState {

    private static final Logger logger = LoggerFactory.getLogger(DefaultOffsetsState.class);

    private Instant metricInfosComputedAt = Instant.now();
    private final Duration metricInfosMaxDelay;
    private final Duration computeMetricsDurationWarn;
    private final Duration lastMetricInfoMaxAge;
    private final long lastMetricInfosMaxSize;
    private final WorkersMetrics metrics;

    private final Map<TopicPartition, OffsetsTimestamps> consumedOffsetsTimestampsMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, SortedRanges> consumedOffsetsMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, SortedRanges> processedOffsetsMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, TopicPartitionMetricInfo> currMetricInfos = new ConcurrentHashMap<>();

    private final Map<TopicPartition, Deque<TopicPartitionMetricInfo>> lastMetricInfos = new ConcurrentHashMap<>();

    public DefaultOffsetsState(WorkersConfig config, WorkersMetrics metrics) {
        this.metrics = metrics;

        this.metricInfosMaxDelay = Duration.ofMillis(getLongFromConfig(config, "offsets-state.metric-infos.delay.ms", 1_000L));
        this.computeMetricsDurationWarn = Duration.ofMillis(getLongFromConfig(config, "offsets-state.metric-infos.compute.warn.ms", 1_000L));
        this.lastMetricInfoMaxAge = Duration.ofMillis(getLongFromConfig(config, "offsets-state.last-metric-infos.max.age.ms", 30_000L));
        this.lastMetricInfosMaxSize = getLongFromConfig(config, "offsets-state.last-metric-infos.max.size", 100L);
        checkState(!metricInfosMaxDelay.isNegative());
        checkState(!computeMetricsDurationWarn.isNegative());
    }

    private static long getLongFromConfig(WorkersConfig config, String key, long defaultValue) {
        return Optional
                .ofNullable(config.originals().get(key))
                .map(value -> Long.valueOf((String) value))
                .orElse(defaultValue);
    }

    public TopicPartitionMetricInfo getCurrMetricInfo(TopicPartition partition) {
        return currMetricInfos.computeIfAbsent(partition, key -> this.new TopicPartitionMetricInfo(key));
    }

    public TopicPartitionMetricInfo getMaxMetricInfo(TopicPartition partition) {
        Deque<TopicPartitionMetricInfo> deque = lastMetricInfos.computeIfAbsent(partition, key -> new ArrayDeque<>(ImmutableList.of(getCurrMetricInfo(key))));
        removeOldMetricInfos(deque);
        return deque.stream().max(this::cmpMetricInfoByNumRanges).orElse(getCurrMetricInfo(partition));
    }

    private void removeOldMetricInfos(Deque<TopicPartitionMetricInfo> deque) {
        while (deque.size() > lastMetricInfosMaxSize || (deque.peekFirst() != null && isOlderThan(deque.peekFirst().computedAt, lastMetricInfoMaxAge))) {
            deque.pollFirst();
        }
    }

    @Override
    public void register(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumedOffsetsTimestampsMap.put(partition, new OffsetsTimestamps());
            consumedOffsetsMap.put(partition, new SortedRanges());
            processedOffsetsMap.put(partition, new SortedRanges());

            metrics.addOffsetsStateCurrentMetrics(this, partition);
            metrics.addOffsetsStateMaxMetrics(this, partition);
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumedOffsetsTimestampsMap.remove(partition);
            consumedOffsetsMap.remove(partition);
            processedOffsetsMap.remove(partition);

            metrics.removeOffsetsStateCurrentMetrics(partition);
            metrics.removeOffsetsStateMaxMetrics(partition);
        }
    }

    @Override
    public void addConsumed(TopicPartition partition, ClosedRange range, Instant consumedAt) {
        SortedRanges consumedOffsets = consumedOffsetsMap.get(partition);
        OffsetsTimestamps consumedOffsetsTimestamps = consumedOffsetsTimestampsMap.get(partition);

        if (consumedOffsets == null || consumedOffsetsTimestamps == null) {
            logger.warn("Aborting addConsumed for partition [{}]", partition);
            return;
        }

        synchronized (consumedOffsets) {
            Optional<Long> minExistingElement = consumedOffsets.getMinExistingElement(range);
            if (minExistingElement.isPresent()) {
                throw new BadOffsetException("Offset: " + minExistingElement.get() + " for partition: " + partition + " was consumed before");
            }
            consumedOffsetsTimestamps.addConsumedRange(new ConsumedOffsetRange(range, consumedAt));
            consumedOffsets.add(range);
        }

        computeMetricInfos();
    }

    @Override
    public void updateProcessed(TopicPartition partition, long offset) {
        SortedRanges consumedOffsets = consumedOffsetsMap.get(partition);
        SortedRanges processedOffsets = processedOffsetsMap.get(partition);

        // unregister() method call may cause consumedOffsets or processedOffsets is null
        if (consumedOffsets == null) {
            logger.warn("Aborting updateProcessed({}, {}) because consumedOffsets == null", partition, offset);
            return;
        }
        if (processedOffsets == null) {
            logger.warn("Aborting updateProcessed({}, {}) because processedOffsets == null", partition, offset);
            return;
        }

        if (processedOffsets.containsSingleElement(offset)) {
            throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was processed before");
        }

        boolean removed = consumedOffsets.removeSingleElement(offset);
        if (!removed) {
            throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was not consumed before");
        }
        processedOffsets.addSingleElement(offset);

        computeMetricInfos();
    }

    private void computeMetricInfos() {
        if (shouldComputeMetricInfos()) {
            Instant start = Instant.now();
            allPartitions().forEach(this::computeMetricInfo);
            long computationMillis = Duration.between(start, Instant.now()).toMillis();
            if (computeMetricsDurationWarn.isZero() || computationMillis > computeMetricsDurationWarn.toMillis()) {
                logger.warn("Metric infos computed in {} ms", computationMillis);
            }
        }
    }

    private Collection<TopicPartition> allPartitions() {
        return consumedOffsetsTimestampsMap.keySet();
    }

    private synchronized boolean shouldComputeMetricInfos() {
        if (metricInfosMaxDelay.isZero()) {
            return true;
        }
        if (isOlderThan(metricInfosComputedAt, metricInfosMaxDelay)) {
            metricInfosComputedAt = Instant.now();
            return true;
        } else {
            return false;
        }
    }

    private void computeMetricInfo(TopicPartition partition) {
        TopicPartitionMetricInfo currInfo = this.new TopicPartitionMetricInfo(partition);
        currMetricInfos.put(partition, currInfo);
        lastMetricInfos.compute(partition, (key, value) ->
                Optional.ofNullable(value)
                        .map(v -> {
                            v.addLast(currInfo);
                            removeOldMetricInfos(v);
                            return v;
                        })
                        .orElse(new ArrayDeque<>(ImmutableList.of(currInfo))));
    }

    private int cmpMetricInfoByNumRanges(TopicPartitionMetricInfo info1, TopicPartitionMetricInfo info2) {

        int rangesCount1 = info1.offsetRangesStatusCounts.values().stream().mapToInt(count -> count).sum();
        int rangesCount2 = info2.offsetRangesStatusCounts.values().stream().mapToInt(count -> count).sum();

        return rangesCount1 - rangesCount2;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Set<TopicPartition> assignedPartitions, Instant minConsumedAt) {
        logger.debug("OffsetsState.getOffsetsToCommit");

        ImmutableMap.Builder<TopicPartition, OffsetAndMetadata> builder = ImmutableMap.builder();

        for (TopicPartition partition : assignedPartitions) {
            Long offsetToCommit = getOffsetToCommit(partition, minConsumedAt);
            if (offsetToCommit != null) {
                builder.put(partition, new OffsetAndMetadata(offsetToCommit + 1));
            }
        }

        return builder.build();
    }

    private Long getOffsetToCommit(TopicPartition partition, Instant minConsumedAt) {
        SortedRanges consumedOffsets = consumedOffsetsMap.get(partition);
        SortedRanges processedOffsets = this.processedOffsetsMap.get(partition);
        OffsetsTimestamps offsetsTimestamps = consumedOffsetsTimestampsMap.get(partition);

        if (consumedOffsets == null || processedOffsets == null || offsetsTimestamps == null) {
            logger.warn("Aborting getOffsetToCommit for partition[{}] (returning null)", partition);
            return null;
        }

        Optional<ClosedRange> consumedFirstRange = consumedOffsets.getFirst();
        Optional<ClosedRange> processedFirstRange = processedOffsets.getFirst();

        // Timeouts are reported for consumed offsets only (not for processed ones)
        if (minConsumedAt != null && consumedFirstRange.isPresent()) {
            long minConsumedOffset = consumedFirstRange.get().lowerEndpoint();
            Instant consumedAt = offsetsTimestamps.getConsumedAt(minConsumedOffset);
            if (consumedAt.isBefore(minConsumedAt)) {
                checkState(!processedOffsets.containsSingleElement(minConsumedOffset));
                throw new ProcessingTimeoutException(
                        String.format("Offset [%s] for partition [%s] exceeded timeout: consumedAt [%s], age [%s ms]",
                                minConsumedOffset, partition, consumedAt, age(consumedAt).toMillis())
                );
            }
        }

        if (processedFirstRange.isPresent()) {
            long offsetToCommit = processedFirstRange.get().upperEndpoint();
            if (consumedFirstRange.isPresent()) {
                checkState(!processedFirstRange.get().isConnected(consumedFirstRange.get()));
                if (processedFirstRange.get().upperEndpoint() < consumedFirstRange.get().lowerEndpoint()) {
                    return offsetToCommit;
                } else {
                    return null;
                }
            } else {
                return offsetToCommit;
            }
        } else {
            return null;
        }
    }

    @Override
    public void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        logger.debug("OffsetsState.removeCommitted");

        offsetsAndMetadata.forEach(this::removeCommitted);
    }

    private void removeCommitted(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        SortedRanges processedOffsets = processedOffsetsMap.get(partition);
        if (processedOffsets == null) {
            logger.warn("Aborting removeCommitted for partition [{}]", partition);
            return;
        }

        long maxOffsetToRemove = offsetAndMetadata.offset() - 1;
        boolean removed = processedOffsets.removeElementsLowerOrEqual(maxOffsetToRemove);
        checkState(removed, "Cannot remove processed offsets up to [%s]", maxOffsetToRemove);
    }

    public class TopicPartitionMetricInfo {

        private final Instant computedAt = Instant.now();
        private final Map<OffsetStatus, Long> offsetStatusCounts;
        private final Map<OffsetStatus, Integer> offsetRangesStatusCounts;

        TopicPartitionMetricInfo(TopicPartition partition) {
            SortedRanges consumedRanges = consumedOffsetsMap.get(partition);
            SortedRanges processedRanges = processedOffsetsMap.get(partition);

            Object consumedRangesLock = consumedRanges != null ? consumedRanges : new Object();
            Object processedRangesLock = processedRanges != null ? processedRanges : new Object();

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (consumedRangesLock) {
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (processedRangesLock) {
                    this.offsetStatusCounts = calculateOffsetStatusCounts(consumedRanges, processedRanges);
                    this.offsetRangesStatusCounts = calculateOffsetRangesStatusCounts(consumedRanges, processedRanges);
                }
            }

            validate();
        }

        private Map<OffsetStatus, Integer> calculateOffsetRangesStatusCounts(SortedRanges consumedRanges,
                                                                          SortedRanges processedOffsetRanges) {
            return ImmutableMap.of(
                    CONSUMED, Optional.ofNullable(consumedRanges).map(SortedRanges::size).orElse(0),
                    PROCESSED, Optional.ofNullable(processedOffsetRanges).map(SortedRanges::size).orElse(0)
            );
        }

        private Map<OffsetStatus, Long> calculateOffsetStatusCounts(Collection<ClosedRange> consumedRanges,
                                                                    Collection<ClosedRange> processedRanges) {
            long consumedOffsetsCount = Optional.ofNullable(consumedRanges).orElse(ImmutableList.of()).stream()
                    .mapToLong(ClosedRange::size)
                    .sum();

            long processedOffsetsCount = Optional.ofNullable(processedRanges).orElse(ImmutableList.of()).stream()
                    .mapToLong(ClosedRange::size)
                    .sum();

            return ImmutableMap.of(
                CONSUMED, consumedOffsetsCount,
                PROCESSED, processedOffsetsCount
            );
        }

        private void validate() {
            var checks = ImmutableList.<Runnable>of(
            );

            checks.forEach(check -> {
                try {
                    check.run();
                } catch (Exception e) {
                    logger.error("OffsetsState.MetricInfo validation failed", e);
                }
            });
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

    // this implementation assumes that there is a single ConsumerThread
    private static class OffsetsTimestamps {
        private final Deque<ConsumedOffsetRange> ranges = new ArrayDeque<>();

        synchronized void addConsumedRange(ConsumedOffsetRange range) {
            if (!ranges.isEmpty()) {
                ConsumedOffsetRange lastRange = ranges.getLast();
                checkState(range.range.lowerEndpoint() > lastRange.range.upperEndpoint(),
                        "condition not met [range.range.lowerEndpoint() > lastRange.range.upperEndpoint()]: " +
                                "lastRange [%s], range [%s]", lastRange.range, range.range);
                checkState(!range.consumedAt.isBefore(lastRange.consumedAt),
                        "condition not met [range.consumedAt >= lastRange.consumedAt]: " +
                                "lastRange [%s], range [%s]", lastRange, range);
            }

            ranges.addLast(range);
        }

        synchronized Instant getConsumedAt(long offset) {
            while (!ranges.isEmpty() && ranges.peekFirst().range.upperEndpoint() < offset) {
                ranges.pollFirst();
            }

            checkState(!ranges.isEmpty() && ranges.peekFirst().range.contains(offset), "cannot find a range containing offset [%s] in consumed ranges", offset);

            return ranges.peekFirst().consumedAt;
        }
    }

    private static class ConsumedOffsetRange {
        private final ClosedRange range;
        private final Instant consumedAt;

        private ConsumedOffsetRange(ClosedRange range, Instant consumedAt) {
            this.range = range;
            this.consumedAt = consumedAt;
        }

        @Override
        public String toString() {
            return "ConsumedOffsetRange{" +
                    "range=" + range +
                    ", consumedAt=" + consumedAt +
                    '}';
        }
    }

}
