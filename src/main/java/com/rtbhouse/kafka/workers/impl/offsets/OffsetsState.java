package com.rtbhouse.kafka.workers.impl.offsets;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.impl.range.ClosedRange;

public interface OffsetsState {

    //TODO: remove
    void addConsumed(TopicPartition partition, long offset, long timestamp);

    void addConsumed(TopicPartition partition, ClosedRange range, Instant consumedAt);

    void updateProcessed(TopicPartition partition, long offset);

    Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Set<TopicPartition> assignedPartitions, Instant minConsumedAt);

    void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata);
}
