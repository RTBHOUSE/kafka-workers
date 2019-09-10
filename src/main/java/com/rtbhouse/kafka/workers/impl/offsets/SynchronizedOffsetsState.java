package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.impl.range.ClosedRange;

public class SynchronizedOffsetsState implements OffsetsState {

    private final OffsetsState offsetsState;

    public SynchronizedOffsetsState(OffsetsState offsetsState) {
        this.offsetsState = checkNotNull(offsetsState);
    }

    @Override
    public synchronized void addConsumed(TopicPartition partition, ClosedRange range, Instant consumedAt) {
        offsetsState.addConsumed(partition, range, consumedAt);
    }

    @Override
    public synchronized void updateProcessed(TopicPartition partition, long offset) {
        offsetsState.updateProcessed(partition, offset);
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Set<TopicPartition> assignedPartitions, Instant minConsumedAt) {
        return offsetsState.getOffsetsToCommit(assignedPartitions, minConsumedAt);
    }

    @Override
    public synchronized void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        offsetsState.removeCommitted(offsetsAndMetadata);
    }

    @Override
    public synchronized void register(Collection<TopicPartition> partitions) throws InterruptedException {
        offsetsState.register(partitions);
    }

    @Override
    public synchronized void unregister(Collection<TopicPartition> partitions) throws InterruptedException {
        offsetsState.unregister(partitions);
    }
}
