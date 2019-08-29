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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
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

    private final WorkersMetrics metrics;

    private final Map<TopicPartition, SortedMap<Long, Info>> offsets;

//    private final Map<TopicPartition, OffsetRanges> processedOffsetRanges;

    private final Map<TopicPartition, TopicPartitionMetricInfo> maxMetricInfos = new ConcurrentHashMap<>();

    public OffsetsState(WorkersConfig config, WorkersMetrics metrics) {
        this.metrics = metrics;
        this.offsets = new ConcurrentHashMap<>();

        long metricInfosMaxDelayMillis = Optional
                .ofNullable(config.originals().get("offsets-state.metric-infos.delay.ms"))
                .map(value -> Long.valueOf((String) value))
                .orElse(1_000L);
        this.metricInfosMaxDelay = Duration.ofMillis(metricInfosMaxDelayMillis);
        logger.info("metricInfosMaxDelay = {} ms", metricInfosMaxDelay.toMillis());
    }

    public TopicPartitionMetricInfo getMetricInfo(TopicPartition partition) {
        TopicPartitionMetricInfo info = this.new TopicPartitionMetricInfo(partition);
        logger.info("created {}", info);
        return info;
    }

    public TopicPartitionMetricInfo getMaxMetricInfo(TopicPartition partition) {
        return maxMetricInfos.getOrDefault(partition, this.new TopicPartitionMetricInfo(partition));
    }

    @Override
    public void register(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            logger.info("offsets.put({}, new)", partition);
            offsets.put(partition, new ConcurrentSkipListMap<>());
//            metrics.addOffsetsStateMetrics(this, partition);
            metrics.addOffsetsStateMaxMetrics(this, partition);
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            logger.info("offsets.remove({})", partition);
            offsets.remove(partition);
//            metrics.removeOffsetsStateMetrics(partition);
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

        computeMetricInfos();
    }

    private void computeMetricInfos() {
        if (shouldComputeMetricInfos()) {
            Instant start = Instant.now();
            offsets.keySet().forEach(this::computeMetricInfo);
            logger.info("Metric infos computed in {} ms", Duration.between(start, Instant.now()).toMillis());
        }
    }

    private synchronized boolean shouldComputeMetricInfos() {
        if (!Duration.between(metricInfosComputedAt, Instant.now()).minus(metricInfosMaxDelay).isNegative()) {
            metricInfosComputedAt = Instant.now();
            return true;
        } else {
            return false;
        }
    }

    private void computeMetricInfo(TopicPartition partition) {
        TopicPartitionMetricInfo currInfo = this.new TopicPartitionMetricInfo(partition);
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
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsAndMetadata.entrySet()) {
            TopicPartition partition = entry.getKey();
            long offset = entry.getValue().offset();
            Map<Long, Info> partitionOffsets = offsets.get(partition);
            if (partitionOffsets == null) {
                continue;
            }
            removeCommitted(partitionOffsets, offset);
        }
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

//    public class MetricInfo {
//        private final Map<Status, Long> offsetStatusCounts = new EnumMap<>(Status.class);
//        private final Map<Status, Long> offsetRangesStatusCounts = new EnumMap<>(Status.class);
//
//        addPartitionInfo(TopicPartitionMetricInfo partitionInfo) {
//            offsetStatusCounts.compute()
//        }
//    }

    public class TopicPartitionMetricInfo {

        //TODO: remove
//        private final SortedMap<Long, Info> copyInfos;
        private final int infosSize;
        private final boolean infosIsEmpty;
        private final long firstOffset;
        private final long lastOffset;
        private final List<OffsetRange> offsetRanges;
        private final Map<Status, Long> offsetStatusCounts;
        private final Map<Status, Long> offsetStatusCountsFromRanges;
        private final Map<Status, Long> offsetRangesStatusCounts;;
        private final double processedRangesToProcessedOffsetsRatio;

        TopicPartitionMetricInfo(TopicPartition partition) {
            SortedMap<Long, Info> infos = copyOffsets(offsets.get(partition));
//            this.copyInfos = infos;
            this.infosSize = infos.size();
            this.infosIsEmpty = infos.isEmpty();
            this.firstOffset = infos.isEmpty() ? 0 : infos.firstKey();
            this.lastOffset = infos.isEmpty() ? -1 : infos.lastKey();
            this.offsetRanges = calculateRanges(infos);
            this.offsetStatusCounts = calculateOffsetStatusCounts(infos);
            this.offsetStatusCountsFromRanges = calculateOffsetStatusCountsFromRanges(offsetRanges);
            this.offsetRangesStatusCounts = calculateOffsetRangesStatusCounts(offsetRanges);
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

        private Map<Status, Long> calculateOffsetStatusCountsFromRanges(List<OffsetRange> offsetRanges) {
            return offsetRanges.stream()
                    .collect(Collectors.groupingBy(r -> r.status, Collectors.summingLong(r -> r.range.upperEndpoint() - r.range.lowerEndpoint() + 1)));
        }

        private Map<Status, Long> calculateOffsetRangesStatusCounts(List<OffsetRange> offsetRanges) {
            return offsetRanges.stream()
                    .collect(Collectors.groupingBy(r -> r.status, Collectors.counting()));
        }

        private Map<Status, Long> calculateOffsetStatusCounts(SortedMap<Long, Info> infos) {
            return infos.values().stream()
                    .collect(Collectors.groupingBy(i -> i.status, Collectors.counting()));
        }

        private void validate() {
            var checks = ImmutableList.<Runnable>of(
                    () -> checkState(infosIsEmpty == offsetRanges.isEmpty(),
                            "infosIsEmpty [%s] != [%s] offsetRanges.isEmpty()\n%s",
                            infosIsEmpty, offsetRanges.isEmpty(), offsetRanges),
                    () -> {
                        if (!offsetRanges.isEmpty()) {
                            checkState(firstOffset == offsetRanges.get(0).range.lowerEndpoint(),
                                    "firstOffset [%s] != [%s] offsetRanges.get(0).range.lowerEndpoint()",
                                    firstOffset, offsetRanges.get(0).range.lowerEndpoint());
                        }
                    },
                    () -> {
                        if (!offsetRanges.isEmpty()) {
                            checkState(lastOffset == offsetRanges.get(offsetRanges.size() - 1).range.upperEndpoint(),
                                    "lastOffset [%s] != [%s] offsetRanges.get(offsetRanges.size() - 1).range.upperEndpoint()",
                                    lastOffset, offsetRanges.get(offsetRanges.size() - 1).range.upperEndpoint());
                        }
                    },
                    () -> checkState(offsetStatusCounts.values().stream().mapToLong(v -> v).sum() == infosSize),
                    () -> checkState(Objects.equals(offsetStatusCounts.get(CONSUMED), offsetStatusCountsFromRanges.get(CONSUMED)),
                            "offsetStatusCounts.get(CONSUMED) [%s] != [%s] offsetStatusCountsFromRanges.get(CONSUMED)",
                            offsetStatusCounts.get(CONSUMED), offsetStatusCountsFromRanges.get(CONSUMED)),
                    () -> checkState(Objects.equals(offsetStatusCounts.get(PROCESSED), offsetStatusCountsFromRanges.get(PROCESSED)),
                            "offsetStatusCounts.get(PROCESSED) [%s] != [%s] offsetStatusCountsFromRanges.get(PROCESSED)",
                            offsetStatusCounts.get(PROCESSED), offsetStatusCountsFromRanges.get(PROCESSED))
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

    }

}
