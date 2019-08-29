package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetsState.Status.CONSUMED;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetsState.Status.EMPTY;
import static com.rtbhouse.kafka.workers.impl.offsets.OffsetsState.Status.PROCESSED;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.errors.BadOffsetException;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingTimeoutException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;

public class OffsetsState implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(OffsetsState.class);

    public enum Status {
        CONSUMED, PROCESSED, EMPTY;
    }
    private static class Info {
        public Status status;
        public long timestamp;

        public Info(Status status, long timestamp) {
            this.status = status;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Info{" +
                    "status=" + status +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    private Instant metricInfosComputedAt = Instant.now();
    private final Duration metricInfosMaxDelay;
    private final Duration computeMetricsDurationWarn;

    private final WorkersMetrics metrics;

    private final Map<TopicPartition, SortedMap<Long, Info>> offsets = new ConcurrentHashMap<>();

    private final Map<TopicPartition, OffsetRanges> processedOffsetRanges = new ConcurrentHashMap<>();

    private final Map<TopicPartition, TopicPartitionMetricInfo> currMetricInfos = new ConcurrentHashMap<>();

    private final Map<TopicPartition, TopicPartitionMetricInfo> maxMetricInfos = new ConcurrentHashMap<>();

    public OffsetsState(WorkersConfig config, WorkersMetrics metrics) {
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
            offsets.put(partition, new ConcurrentSkipListMap<>());
            processedOffsetRanges.put(partition, new OffsetRanges());

            metrics.addOffsetsStateCurrentMetrics(this, partition);
            metrics.addOffsetsStateMaxMetrics(this, partition);
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsets.remove(partition);
            processedOffsetRanges.remove(partition);

            metrics.removeOffsetsStateCurrentMetrics(partition);
            metrics.removeOffsetsStateMaxMetrics(partition);
        }
    }

    public void addConsumed(TopicPartition partition, long offset, long timestamp) {
        //TODO: what if not present? BUG?
        offsets.computeIfPresent(partition, (k, v) -> {
            if (v.get(offset) != null) {
                throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was consumed before");
            }
            v.put(offset, new Info(Status.CONSUMED, timestamp));
            return v;
        });

        computeMetricInfos();
    }

    public void updateProcessed(TopicPartition partition, long offset) {
        //TODO: what if not present? BUG?
        offsets.computeIfPresent(partition, (k, v) -> {
            Info info = v.get(offset);
            if (info == null) {
                throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was not consumed before");
            }
            if (info.status != Status.CONSUMED) {
                throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was processed before");
            }
            info.status = Status.PROCESSED;
            return v;
        });

        processedOffsetRanges.get(partition).addOffset(offset);

        computeMetricInfos();
    }

    private void computeMetricInfos() {
        if (shouldComputeMetricInfos()) {
            Instant start = Instant.now();
            offsets.keySet().forEach(this::computeMetricInfo);
            long computationMillis = Duration.between(start, Instant.now()).toMillis();
            if (computeMetricsDurationWarn.isZero() || computationMillis > computeMetricsDurationWarn.toMillis()) {
                logger.warn("Metric infos computed in {} ms", computationMillis);
            }
        }
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

    public Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Set<TopicPartition> assignedPartitions, Long minTimestamp) {
        logger.debug("OffsetsState.getOffsetsToCommit");
        Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = new HashMap<>();
        for (TopicPartition partition : assignedPartitions) {
            Map<Long, Info> partitionOffsets = offsets.get(partition);
            if (partitionOffsets == null) {
                continue;
            }
            Long offsetToCommit = getOffsetToCommit(partition, partitionOffsets, minTimestamp);
            if (offsetToCommit != null) {
                offsetsAndMetadata.put(partition, new OffsetAndMetadata(offsetToCommit + 1));
            }
        }
        return offsetsAndMetadata;
    }

    public void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        logger.debug("OffsetsState.removeCommitted");
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsAndMetadata.entrySet()) {
            TopicPartition partition = entry.getKey();
            long offset = entry.getValue().offset();
            Map<Long, Info> partitionOffsets = offsets.get(partition);
            if (partitionOffsets == null) {
                continue;
            }
            removeCommitted(partitionOffsets, offset);
        }

        //new impl
        removeCommittedProcessedOffsets(offsetsAndMetadata);
    }

    private void removeCommittedProcessedOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        offsetsAndMetadata.forEach((partition, offsetAndMetadata) -> {
            OffsetRanges ranges = processedOffsetRanges.get(partition);
            ranges.removeOffsetsUntil(offsetAndMetadata.offset());
        });
    }

    private Long getOffsetToCommit(TopicPartition partition, Map<Long, Info> partitionOffsets, Long minTimestamp) {
        Long offsetToCommit = null;
        for (Map.Entry<Long, Info> entry : partitionOffsets.entrySet()) {
            Long offset = entry.getKey();
            Info info = entry.getValue();
            if (info.status == Status.PROCESSED) {
                offsetToCommit = offset;
            } else {
                if (minTimestamp != null && info.timestamp < minTimestamp) {
                    throw new ProcessingTimeoutException("Offset: " + offset + " for partition: " + partition + " exceeded timeout");
                }
                // there is no need to check greater offsets because:
                // 1. gap was found in processed offsets so we could not commit greater ones
                // 2. the smallest consumed (not processed) offset is not time out so greater ones would not be too
                break;
            }
        }

        Optional<OffsetRanges.ClosedRange> processedOffsetsFirstRange = Optional.ofNullable(
                processedOffsetRanges.get(partition).getFirst());

        if (!Objects.equals(offsetToCommit, processedOffsetsFirstRange.map(OffsetRanges.ClosedRange::upperEndpoint).orElse(null))) {
            logger.warn("old offsetToCommit [{}] not equal to {} upper endpoint", offsetToCommit, processedOffsetsFirstRange);
        }

        return offsetToCommit;
    }

    private void removeCommitted(Map<Long, Info> partitionOffsets, long offset) {
        Iterator<Entry<Long, Info>> it = partitionOffsets.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Info> entry = it.next();
            if (entry.getKey() < offset) {
                it.remove();
            } else {
                break;
            }
        }
    }

    public class TopicPartitionMetricInfo {

        private final int infosSize;
        private final boolean infosIsEmpty;
        private final long firstOffset;
        private final long lastOffset;
        private final SortedSet<OffsetRanges.ClosedRange> processedOffsetRanges;
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
            this.processedOffsetRanges = new TreeSet<>(OffsetsState.this.processedOffsetRanges.get(partition).ranges);
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

        private Map<Status, Long> calculateOffsetRangesStatusCounts(SortedSet<OffsetRanges.ClosedRange> processedOffsetRanges) {
            return ImmutableMap.of(
                    PROCESSED, (long) processedOffsetRanges.size()
            );
        }

        private Map<Status, Long> calculateOffsetStatusCounts(SortedMap<Long, Info> infos, SortedSet<OffsetRanges.ClosedRange> processedOffsetRanges) {
            long processedOffsetsCount = processedOffsetRanges.stream()
                    .mapToLong(OffsetRanges.ClosedRange::size)
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
                    rangeBuilder = OffsetRange.builder(offset, info.status);
                } else if (offset == rangeBuilder.lastOffset + 1) {
                    if (info.status == rangeBuilder.status) {
                        rangeBuilder.extend(offset);
                    } else {
                        listBuilder.add(rangeBuilder.build());
                        rangeBuilder = OffsetRange.builder(offset, info.status);
                    }
                } else {
                    listBuilder.add(rangeBuilder.build());
                    OffsetRange rangeWithEmptyStatus = OffsetRange.withEmptyStatus(rangeBuilder.lastOffset + 1, offset - 1);
//                    logger.warn("found range with EMPTY status {} (size={}) between [{}, {}] and [{}, {}]",
//                            rangeWithEmptyStatus,
//                            rangeWithEmptyStatus.range.upperEndpoint() - rangeWithEmptyStatus.range.lowerEndpoint() + 1,
//                            rangeBuilder.lastOffset, rangeBuilder.status,
//                            offset, info.status);
                    listBuilder.add(rangeWithEmptyStatus);
                    rangeBuilder = OffsetRange.builder(offset, info.status);
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

    private static class OffsetRange {
        private final Range<Long> range;
        private final Status status;

        private OffsetRange(Builder builder) {
            this(builder.firstOffset, builder.lastOffset, builder.status);
        }

        private OffsetRange(long firstOffset, long lastOffset, Status status) {
            this.range = Range.closed(firstOffset, lastOffset);
            this.status = status;
        }

        public static Builder builder(long firstOffset, Status status) {
            return new Builder(firstOffset, status);
        }

        public static OffsetRange withEmptyStatus(long firstOffset, long lastOffset) {
            return new OffsetRange(firstOffset, lastOffset, EMPTY);
        }

        @Override
        public String toString() {
            return "OffsetRange{" +
                    "range=" + range +
                    ", status=" + status +
                    '}';
        }

        private static class Builder {
            final long firstOffset;
            long lastOffset;
            final Status status;

            Builder(long firstOffset, Status status) {
                this.firstOffset = this.lastOffset = firstOffset;
                this.status = checkNotNull(status);
            }

            Builder extend(long offset) {
                checkArgument(offset == lastOffset + 1);
                this.lastOffset = offset;
                return this;
            }

            OffsetRange build() {
                return new OffsetRange(this);
            }
        }
    }

    private static class OffsetRanges {

        private NavigableSet<ClosedRange> ranges = new TreeSet<>(Comparator.comparingLong(ClosedRange::lowerEndpoint));

        synchronized void addOffset(long offset) {
            addRange(new ClosedRange(offset, offset));
        }

        synchronized void addRange(ClosedRange range) {
            ClosedRange prevRange = ranges.ceiling(range);
            ClosedRange nextRange = ranges.floor(range);

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

            if (prevRange != null && range.lowerEndpoint() == prevRange.upperEndpoint() + 1) {
                if (nextRange != null && range.upperEndpoint() == nextRange.lowerEndpoint() - 1) {
                    // the given range joins prev and next ranges
                    ranges.remove(prevRange);
                    ranges.remove(nextRange);
                    ranges.add(new ClosedRange(prevRange.lowerEndpoint(), nextRange.upperEndpoint()));
                } else {
                    ranges.remove(prevRange);
                    ranges.add(new ClosedRange(prevRange.lowerEndpoint(), prevRange.upperEndpoint() + 1));
                }
            } else {
                if (nextRange != null && range.upperEndpoint() == nextRange.lowerEndpoint() - 1) {
                    ranges.remove(nextRange);
                    ranges.add(new ClosedRange(nextRange.lowerEndpoint() - 1, nextRange.upperEndpoint()));
                } else {
                    ranges.add(range);
                }
            }
        }

        synchronized void removeOffsetsUntil(long offset) {
            ClosedRange first = ranges.pollFirst();
            checkState(first != null && first.contains(offset), "first range {} does not contain {}",
                    first, offset);
            if (!(first.upperEndpoint() == offset)) {
                ranges.add(new ClosedRange(offset + 1, first.upperEndpoint()));
            }
        }

        private static class ClosedRange {
            private final long lowerEndpoint;
            private final long upperEndpoint;

            private ClosedRange(long lowerEndpoint, long upperEndpoint) {
                checkArgument(lowerEndpoint <= upperEndpoint);
                this.lowerEndpoint = lowerEndpoint;
                this.upperEndpoint = upperEndpoint;
            }

            public long lowerEndpoint() {
                return lowerEndpoint;
            }

            public long upperEndpoint() {
                return upperEndpoint;
            }

            @Override
            public String toString() {
                return "ClosedRange{" + lowerEndpoint + ", " + upperEndpoint + "}";
            }

            public long size() {
                return upperEndpoint - lowerEndpoint + 1;
            }

            public boolean contains(long offset) {
                return lowerEndpoint <= offset && offset <= upperEndpoint;
            }
        }

        ClosedRange getFirst() {
            try {
                return ranges.first();
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        Optional<ClosedRange> pollFirst() {
            return Optional.ofNullable(ranges.pollFirst());
        }
    }

}
