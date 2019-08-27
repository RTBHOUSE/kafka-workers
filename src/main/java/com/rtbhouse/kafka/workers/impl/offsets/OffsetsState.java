package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.errors.BadOffsetException;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingTimeoutException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;

public class OffsetsState implements Partitioned {

    private static final Logger logger = LoggerFactory.getLogger(OffsetsState.class);

    public enum Status {
        CONSUMED, PROCESSED;
    }
    private static class Info {
        public Status status;

        public long timestamp;
        public Info(Status status, long timestamp) {
            this.status = status;
            this.timestamp = timestamp;
        }

    }
    private final WorkersMetrics metrics;

    private final Map<TopicPartition, SortedMap<Long, Info>> offsets;

    public OffsetsState(WorkersMetrics metrics) {
        this.metrics = metrics;
        this.offsets = new ConcurrentHashMap<>();
    }

    public MetricInfo getMetricInfo(TopicPartition partition) {
        return this.new MetricInfo(partition);
    }

    @Override
    public void register(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsets.put(partition, new ConcurrentSkipListMap<>());
            metrics.addOffsetsStateMetrics(this, partition);
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsets.remove(partition);
            metrics.removeOffsetsStateMetrics(partition);
        }
    }

    public void addConsumed(TopicPartition partition, long offset, long timestamp) {
        offsets.computeIfPresent(partition, (k, v) -> {
            if (v.get(offset) != null) {
                throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was consumed before");
            }
            v.put(offset, new Info(Status.CONSUMED, timestamp));
            return v;
        });
    }

    public void updateProcessed(TopicPartition partition, long offset) {
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

    public class MetricInfo {

        private final SortedMap<Long, Info> infos;
        private final long firstOffset;
        private final long lastOffset;
        private final List<OffsetRange> offsetRanges;
        private final Map<Status, Long> offsetStatusCounts;
        private final Map<Status, Long> offsetRangesStatusCounts;

        MetricInfo(TopicPartition partition) {
            this.infos = offsets.get(partition);
            this.firstOffset = infos.isEmpty() ? 0 : infos.firstKey();
            this.lastOffset = infos.isEmpty() ? -1 : infos.lastKey();
            this.offsetRanges = calculateRanges(infos);
            this.offsetStatusCounts = calculateOffsetStatusCounts(infos);
            this.offsetRangesStatusCounts = calculateOffsetRangesStatusCounts(offsetRanges);
            validate();
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
                    () -> checkState(lastOffset - firstOffset + 1 == infos.size(),
                            "lastOffset - firstOffset + 1 != infos.size()"),
                    () -> checkState((infos.isEmpty() && offsetRanges.isEmpty()) || firstOffset == offsetRanges.get(0).range.lowerEndpoint(),
                            "firstOffset != offsetRanges.get(0).range.lowerEndpoint()"),
                    () -> checkState((infos.isEmpty() && offsetRanges.isEmpty()) || lastOffset == offsetRanges.get(offsetRanges.size() - 1).range.upperEndpoint(),
                            "lastOffset != offsetRanges.get(offsetRanges.size() - 1).range.upperEndpoint()")
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
                    listBuilder.add(OffsetRange.withNullStatus(rangeBuilder.lastOffset + 1, offset - 1));
                    rangeBuilder = OffsetRange.builder(offset, info.status);
                }
            }

            if (rangeBuilder != null) {
                listBuilder.add(rangeBuilder.build());
            }

            return listBuilder.build();
        }

        public long getOffsetStatusCount(Status status) {
            return offsetStatusCounts.getOrDefault(status, 0L);
        }

        public long getOffsetRangesStatusCount(Status status) {
            return offsetRangesStatusCounts.getOrDefault(status, 0L);
        }

        public long getOffsetsCount() {
            return infos.size();
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

        public static OffsetRange withNullStatus(long firstOffset, long lastOffset) {
            return new OffsetRange(firstOffset, lastOffset, null);
        }

        private static class Builder {
            long firstOffset;
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

}
