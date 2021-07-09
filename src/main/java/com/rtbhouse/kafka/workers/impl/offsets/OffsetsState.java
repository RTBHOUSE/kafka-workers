package com.rtbhouse.kafka.workers.impl.offsets;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.range.ClosedRange;

public interface OffsetsState extends Partitioned {

    default void addConsumed(TopicPartition partition, long offset, Instant consumedAt) {
        addConsumed(partition, ClosedRange.singleElementRange(offset), consumedAt);
    }

    void addConsumed(TopicPartition partition, ClosedRange range, Instant consumedAt);

    default void addConsumed(TopicPartition partition, ClosedRange range) {
        addConsumed(partition, range, Instant.now());
    }

    void updateProcessed(TopicPartition partition, long offset);

    void timeoutRecordsConsumedBefore(Instant minConsumedAt);

    Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit();

    void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata);

    long getProcessedUncommittedRecordsTotal();
}
