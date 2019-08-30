package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetStatus.CONSUMED;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetStatus.PROCESSED;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
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

public class DefaultOffsetsState implements Partitioned, OffsetsState {

    private static final Logger logger = LoggerFactory.getLogger(DefaultOffsetsState.class);

    private Instant metricInfosComputedAt = Instant.now();
    private final Duration metricInfosMaxDelay;
    private final Duration computeMetricsDurationWarn;
    private final WorkersMetrics metrics;

    private final Map<TopicPartition, OffsetsTimestamps> offsetsTimestampsMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, Offsets> offsetsMap = new ConcurrentHashMap<>();

//    private final Map<TopicPartition, Long> committedOffsetsMap = new ConcurrentHashMap<>();

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
            offsetsTimestampsMap.put(partition, new OffsetsTimestamps());
            offsetsMap.put(partition, new Offsets());

            metrics.addOffsetsStateCurrentMetrics(this, partition);
            metrics.addOffsetsStateMaxMetrics(this, partition);
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsetsTimestampsMap.remove(partition);
            offsetsMap.remove(partition);
            committedOffsetsMap.remove(partition);

            metrics.removeOffsetsStateCurrentMetrics(partition);
            metrics.removeOffsetsStateMaxMetrics(partition);
        }
    }

    @Override
    public void addConsumed(TopicPartition partition, long offset, long timestamp) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void addConsumed(TopicPartition partition, OffsetRange range, Instant consumedAt) {
        offsetsTimestampsMap.get(partition).addConsumedRange(new ConsumedOffsetRange(range, consumedAt));
        offsetsMap.get(partition).addRange(range);

        computeMetricInfos();
    }

    @Override
    public void updateProcessed(TopicPartition partition, long offset) {
        //TODO: check if offset is consumed
        offsetsMap.get(partition).changeOffsetStatus(offset, CONSUMED, PROCESSED);

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
        return offsetsTimestampsMap.keySet();
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
        Offsets processedOffsets = offsetsMap.get(partition);
        long lastCommitted = committedOffsetsMap.get(partition);

        OffsetRange processedRange = processedOffsets.getFirst();
        if (processedRange != null) {
            checkState(processedRange.lowerEndpoint() > lastCommitted);
            if (processedRange.lowerEndpoint() == lastCommitted + 1) {
                return processedRange.upperEndpoint();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        logger.debug("OffsetsState.removeCommitted");

        offsetsAndMetadata.forEach((partition, offsetAndMetadata) -> {
            Offsets ranges = offsetsMap.get(partition);
            ranges.removeOffsetsUntil(offsetAndMetadata.offset());
        });
    }

    public class TopicPartitionMetricInfo {


        private final int infosSize;
        private final boolean infosIsEmpty;
        private final long firstOffset;
        private final long lastOffset;
        private final SortedSet<OffsetRange> processedOffsetRanges;
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

        private Map<Status, Long> calculateOffsetRangesStatusCounts(SortedSet<OffsetRange> processedOffsetRanges) {
            return ImmutableMap.of(
                    PROCESSED, (long) processedOffsetRanges.size()
            );
        }

        private Map<Status, Long> calculateOffsetStatusCounts(SortedMap<Long, Info> infos, SortedSet<OffsetRange> processedOffsetRanges) {
            long processedOffsetsCount = processedOffsetRanges.stream()
                    .mapToLong(OffsetRange::size)
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

        private List<OffsetRange> calculateRanges(SortedMap<Long, Info> infos) {
            ImmutableList.Builder<OffsetRange> listBuilder = ImmutableList.builder();

            OffsetRange.Builder rangeBuilder = null;
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
                    OffsetRange rangeWithEmptyStatus = DefaultOffsetsState.OffsetRange.withEmptyStatus(rangeBuilder.lastOffset + 1, offset - 1);
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
        private final OffsetRange range;
        private final Instant consumedAt;

        private ConsumedOffsetRange(OffsetRange range, Instant consumedAt) {
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

    private static class Offsets {

//        private Long minOffset = null;

        private NavigableSet<OffsetRange> ranges = new TreeSet<>(Comparator.comparingLong(OffsetRange::lowerEndpoint));

//        synchronized void registerMinOffset(long offset) {
//            minOffset = minOffset == null ? offset : Long.min(minOffset, offset);
//        }

        synchronized void changeOffsetState(long offset, OffsetStatus fromStatus, OffsetStatus toStatus) {
            checkArgument(fromStatus != toStatus);
            removeRangeContaining(offset, fromStatus);
        }

        private OffsetRange removeRangeContaining(long offset, OffsetStatus fromStatus) {
            OffsetRange findRange = OffsetRange.of(offset, offset, fromStatus);
            ranges.ceiling(findRange);
            ranges.floor(findRange);
            //TODO: NEXT
        }


        synchronized void addRange(OffsetRange range) {
            OffsetRange prevRange = ranges.ceiling(range);
            OffsetRange nextRange = ranges.floor(range);

            if (prevRange != null) {
                checkState(prevRange.upperEndpoint() < range.lowerEndpoint(),
                        "condition not met [prevRange.upperEndpoint() < range.lowerEndpoint()]: %s, %s",
                        prevRange, range);
            }
            if (nextRange != null) {
                checkState(range.upperEndpoint() < nextRange.lowerEndpoint(),
                        "condition not met [range.upperEndpoint() < nextRange.lowerEndpoint()]: %s, %s",
                        range, nextRange);
            }

            if (prevRange != null && touchingRanges(prevRange, range)) {
                if (nextRange != null && touchingRanges(range, nextRange)) {
                    // the given range joins prev and next ranges
                    ranges.remove(prevRange);
                    ranges.remove(nextRange);
                    ranges.add(OffsetRange.of(prevRange.lowerEndpoint(), nextRange.upperEndpoint(), range.getStatus()));
                } else {
                    ranges.remove(prevRange);
                    ranges.add(OffsetRange.of(prevRange.lowerEndpoint(), range.upperEndpoint(), range.getStatus()));
                }
            } else {
                if (nextRange != null && touchingRanges(range, nextRange)) {
                    ranges.remove(nextRange);
                    ranges.add(OffsetRange.of(range.lowerEndpoint(), nextRange.upperEndpoint(), range.getStatus()));
                } else {
                    ranges.add(range);
                }
            }
        }

        private boolean touchingRanges(OffsetRange range1, OffsetRange range2) {
            return range1.getStatus() == range2.getStatus() && range1.upperEndpoint() + 1 == range2.lowerEndpoint();
        }

        synchronized void removeOffsetsUntil(long offset) {
            OffsetRange first = ranges.pollFirst();
            checkState(first != null && first.contains(offset), "first range {} does not contain {}",
                    first, offset);
            if (!(first.upperEndpoint() == offset)) {
                ranges.add(OffsetRange.of(offset + 1, first.upperEndpoint()));
            }
        }

        OffsetRange getFirst() {
            try {
                return ranges.first();
            } catch (NoSuchElementException e) {
                return null;
            }
        }
    }

}
