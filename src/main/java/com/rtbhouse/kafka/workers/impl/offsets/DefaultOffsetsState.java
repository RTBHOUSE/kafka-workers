package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetStatus.CONSUMED;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetStatus.PROCESSED;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.range.ClosedRange;
import com.rtbhouse.kafka.workers.impl.range.SortedRanges;

public class DefaultOffsetsState implements Partitioned, OffsetsState {

    private static final Logger logger = LoggerFactory.getLogger(DefaultOffsetsState.class);

    private Instant metricInfosComputedAt = Instant.now();
    private final Duration metricInfosMaxDelay;
    private final Duration computeMetricsDurationWarn;
    private final WorkersMetrics metrics;

    private final Map<TopicPartition, OffsetsTimestamps> consumedOffsetsTimestampsMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, SortedRanges> consumedOffsetsMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, SortedRanges> processedOffsetsMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, TopicPartitionMetricInfo> currMetricInfos = new ConcurrentHashMap<>();

    private final Map<TopicPartition, TopicPartitionMetricInfo> maxMetricInfos = new ConcurrentHashMap<>();

    public DefaultOffsetsState(WorkersConfig config, WorkersMetrics metrics) {
        this.metrics = metrics;

        this.metricInfosMaxDelay = Duration.ofMillis(getLongFromConfig(config, "offsets-state.metric-infos.delay.ms", 1_000L));
        this.computeMetricsDurationWarn = Duration.ofMillis(getLongFromConfig(config, "offsets-state.metric-infos.compute.warn.ms", 1_000L));
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
        return currMetricInfos.getOrDefault(partition, this.new TopicPartitionMetricInfo(partition));
    }

    public TopicPartitionMetricInfo getMaxMetricInfo(TopicPartition partition) {
        return maxMetricInfos.getOrDefault(partition, this.new TopicPartitionMetricInfo(partition));
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
    public void addConsumed(TopicPartition partition, long offset, long timestamp) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void addConsumed(TopicPartition partition, ClosedRange range, Instant consumedAt) {
        consumedOffsetsTimestampsMap.get(partition).addConsumedRange(new ConsumedOffsetRange(range, consumedAt));
        consumedOffsetsMap.get(partition).addRange(range);

        computeMetricInfos();
    }

    @Override
    public void updateProcessed(TopicPartition partition, long offset) {
        consumedOffsetsMap.get(partition).removeSingleElement(offset);
        processedOffsetsMap.get(partition).addSingleElement(offset);

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
        if (!Duration.between(metricInfosComputedAt, Instant.now()).minus(metricInfosMaxDelay).isNegative()) {
            metricInfosComputedAt = Instant.now();
            return true;
        } else {
            return false;
        }
    }

    private void computeMetricInfo(TopicPartition partition) {
        TopicPartitionMetricInfo currInfo = this.new TopicPartitionMetricInfo(partition);
        currMetricInfos.put(partition, currInfo);
        maxMetricInfos.compute(partition, (k, prevInfo) -> infoWithMoreRanges(prevInfo, currInfo));
    }

    private TopicPartitionMetricInfo infoWithMoreRanges(TopicPartitionMetricInfo prevInfo, TopicPartitionMetricInfo currInfo) {
        checkNotNull(currInfo);

        if (prevInfo == null) {
            return currInfo;
        }

        long currProcessedRangesCount = currInfo.offsetRangesStatusCounts.getOrDefault(PROCESSED, 0L);
        long prevProcessedRangesCount = prevInfo.offsetRangesStatusCounts.getOrDefault(PROCESSED, 0L);

        if (currProcessedRangesCount > prevProcessedRangesCount) {
            return currInfo;
        } else if (currProcessedRangesCount == prevProcessedRangesCount) {
            if (currInfo.offsetStatusCounts.size() > prevInfo.offsetStatusCounts.size()) {
                return currInfo;
            } else {
                return prevInfo;
            }
        } else {
            return prevInfo;
        }
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
        Optional<ClosedRange> processedFirstRange = processedOffsetsMap.get(partition).getFirst();
        Optional<ClosedRange> consumedFirstRange = consumedOffsetsMap.get(partition).getFirst();

        if (processedFirstRange.isPresent()) {
            if (consumedFirstRange.isPresent()) {
                checkState(!processedFirstRange.get().isConnected(consumedFirstRange.get()));
                return processedFirstRange.get().lowerEndpoint();
            } else {
                return processedFirstRange.get().lowerEndpoint();
            }
        } else {
            return null;
        }
    }

    @Override
    public void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        logger.debug("OffsetsState.removeCommitted");

        offsetsAndMetadata.forEach((partition, offsetAndMetadata) -> {
            processedOffsetsMap.get(partition).removeElementsLowerOrEqual(offsetAndMetadata.offset());
        });
    }

    public class TopicPartitionMetricInfo {


        private final int infosSize;
        private final boolean infosIsEmpty;
        private final long firstOffset;
        private final long lastOffset;
        private final SortedSet<ClosedRange> processedOffsetRanges;
        private final Map<Status, Long> offsetStatusCounts;
        private final Map<Status, Long> offsetRangesStatusCounts;;
        private final double processedRangesToProcessedOffsetsRatio;

        TopicPartitionMetricInfo(TopicPartition partition) {
//            SortedMap<Long, Info> infos = copyOffsets(offsets.get(partition));
            SortedMap<Long, Info> infos = offsets.get(partition);
            this.infosSize = infos.size();
            this.infosIsEmpty = infos.isEmpty();
            this.firstOffset = infos.isEmpty() ? 0 : infos.firstKey();
            this.lastOffset = infos.isEmpty() ? -1 : infos.lastKey();
            this.processedOffsetRanges = new TreeSet<>(DefaultOffsetsState.this.offsetsMap.get(partition).ranges);
            this.offsetStatusCounts = calculateOffsetStatusCounts(infos, processedOffsetRanges);
            this.offsetRangesStatusCounts = calculateOffsetRangesStatusCounts(processedOffsetRanges);
            this.processedRangesToProcessedOffsetsRatio = calculateProcessedRangesToProcessedOffsetsRatio();
            validate();
        }

        private double calculateProcessedRangesToProcessedOffsetsRatio() {
            long processedOffsets = offsetStatusCounts.getOrDefault(PROCESSED, 0L);
            long processedRanges = offsetRangesStatusCounts.getOrDefault(PROCESSED, 0L);
            return processedOffsets > 0 ? ((double) processedRanges) / ((double) processedOffsets) : 0.0;
        }

        private SortedMap<Long, Info> copyOffsets(SortedMap<Long, Info> offsets) {
            TreeMap<Long, Info> copy = new TreeMap<>();
            offsets.forEach((offset, info) -> copy.put(offset, new Info(info.status, info.timestamp)));
            return copy;
        }

        private Map<Status, Long> calculateOffsetRangesStatusCounts(SortedSet<ClosedRange> processedOffsetRanges) {
            return ImmutableMap.of(
                    PROCESSED, (long) processedOffsetRanges.size()
            );
        }

        private Map<Status, Long> calculateOffsetStatusCounts(SortedMap<Long, Info> infos, SortedSet<ClosedRange> processedOffsetRanges) {
            long processedOffsetsCount = processedOffsetRanges.stream()
                    .mapToLong(ClosedRange::size)
                    .sum();

            long consumedOffsetsCount = infos.size() - processedOffsetsCount;

            return ImmutableMap.of(
                CONSUMED, consumedOffsetsCount,
                PROCESSED, processedOffsetsCount
            );
        }

        private void validate() {
            var checks = ImmutableList.<Runnable>of(
//                    () -> checkState(infosIsEmpty == processedOffsetRanges.isEmpty(),
//                            "infosIsEmpty [%s] != [%s] offsetRanges.isEmpty()\n%s",
//                            infosIsEmpty, processedOffsetRanges.isEmpty(), processedOffsetRanges),
//                    () -> {
//                        if (!processedOffsetRanges.isEmpty()) {
//                            checkState(firstOffset == processedOffsetRanges.get(0).range.lowerEndpoint(),
//                                    "firstOffset [%s] != [%s] offsetRanges.get(0).range.lowerEndpoint()",
//                                    firstOffset, processedOffsetRanges.get(0).range.lowerEndpoint());
//                        }
//                    },
//                    () -> {
//                        if (!processedOffsetRanges.isEmpty()) {
//                            checkState(lastOffset == processedOffsetRanges.get(processedOffsetRanges.size() - 1).range.upperEndpoint(),
//                                    "lastOffset [%s] != [%s] offsetRanges.get(offsetRanges.size() - 1).range.upperEndpoint()",
//                                    lastOffset, processedOffsetRanges.get(processedOffsetRanges.size() - 1).range.upperEndpoint());
//                        }
//                    },
//                    () -> checkState(offsetStatusCounts.values().stream().mapToLong(v -> v).sum() == infosSize),
//                    () -> checkState(Objects.equals(offsetStatusCounts.get(CONSUMED), offsetStatusCountsFromRanges.get(CONSUMED)),
//                            "offsetStatusCounts.get(CONSUMED) [%s] != [%s] offsetStatusCountsFromRanges.get(CONSUMED)",
//                            offsetStatusCounts.get(CONSUMED), offsetStatusCountsFromRanges.get(CONSUMED)),
//                    () -> checkState(Objects.equals(offsetStatusCounts.get(PROCESSED), offsetStatusCountsFromRanges.get(PROCESSED)),
//                            "offsetStatusCounts.get(PROCESSED) [%s] != [%s] offsetStatusCountsFromRanges.get(PROCESSED)",
//                            offsetStatusCounts.get(PROCESSED), offsetStatusCountsFromRanges.get(PROCESSED))
            );

            checks.forEach(check -> {
                try {
                    check.run();
                } catch (Exception e) {
                    logger.error("OffsetsState.MetricInfo validation failed", e);
                }
            });
        }

        private List<ClosedRange> calculateRanges(SortedMap<Long, Info> infos) {
            ImmutableList.Builder<ClosedRange> listBuilder = ImmutableList.builder();

            ClosedRange.Builder rangeBuilder = null;
            for (Entry<Long, Info> entry : infos.entrySet()) {
                final long offset = entry.getKey();
                final Info info = entry.getValue();

                if (rangeBuilder == null) {
                    rangeBuilder = DefaultOffsetsState.OffsetRange.builder(offset, info.status);
                } else if (offset == rangeBuilder.lastOffset + 1) {
                    if (info.status == rangeBuilder.status) {
                        rangeBuilder.extend(offset);
                    } else {
                        listBuilder.add(rangeBuilder.build());
                        rangeBuilder = DefaultOffsetsState.OffsetRange.builder(offset, info.status);
                    }
                } else {
                    listBuilder.add(rangeBuilder.build());
                    ClosedRange rangeWithEmptyStatus = DefaultOffsetsState.OffsetRange.withEmptyStatus(rangeBuilder.lastOffset + 1, offset - 1);
//                    logger.warn("found range with EMPTY status {} (size={}) between [{}, {}] and [{}, {}]",
//                            rangeWithEmptyStatus,
//                            rangeWithEmptyStatus.range.upperEndpoint() - rangeWithEmptyStatus.range.lowerEndpoint() + 1,
//                            rangeBuilder.lastOffset, rangeBuilder.status,
//                            offset, info.status);
                    listBuilder.add(rangeWithEmptyStatus);
                    rangeBuilder = DefaultOffsetsState.OffsetRange.builder(offset, info.status);
                }
            }

            if (rangeBuilder != null) {
                listBuilder.add(rangeBuilder.build());
            }

            return listBuilder.build();
        }

        public double getProcessedRangesToProcessedOffsetsRatio() {
            return processedRangesToProcessedOffsetsRatio;
        }

        public long getOffsetStatusCount(Status status) {
            return offsetStatusCounts.getOrDefault(status, 0L);
        }

        public long getOffsetRangesStatusCount(Status status) {
            return offsetRangesStatusCounts.getOrDefault(status, 0L);
        }

        public long getOffsetsCount() {
            return infosSize;
        }

        @Override
        public String toString() {
            return "TopicPartitionMetricInfo{"
                    + "firstOffset=" + firstOffset + ", "
                    + "lastOffset=" + lastOffset + ", "
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

            checkState(!ranges.isEmpty(), "cannot find a range containing offset [%s] in consumed ranges", offset);

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
