package com.rtbhouse.kafka.workers.impl.offsets;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.errors.BadOffsetException;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingTimeoutException;

public class OffsetsState implements Partitioned {

    private enum Status {
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

    private Map<TopicPartition, Map<Long, Info>> offsetsMap = new ConcurrentHashMap<>();

    @Override
    public void register(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsetsMap.put(partition, new ConcurrentSkipListMap<>());
        }
    }

    @Override
    public void unregister(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsetsMap.remove(partition);
        }
    }

    public void addConsumed(TopicPartition partition, long offset, long timestamp) {
        offsetsMap.computeIfPresent(partition, (k, v) -> {
            if (v.get(offset) != null) {
                throw new BadOffsetException("Offset: " + offset + " for partition: " + partition + " was consumed before");
            }
            v.put(offset, new Info(Status.CONSUMED, timestamp));
            return v;
        });
    }

    public void updateProcessed(TopicPartition partition, long offset) {
        offsetsMap.computeIfPresent(partition, (k, v) -> {
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
            Map<Long, Info> partitionOffsets = offsetsMap.get(partition);
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
            Map<Long, Info> partitionOffsets = offsetsMap.get(partition);
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

}
