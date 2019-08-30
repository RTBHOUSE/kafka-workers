package com.rtbhouse.kafka.workers.impl.offsets;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class ComparingOffsetsState implements OffsetsState {


    @Override
    public void addConsumed(TopicPartition partition, long offset, long timestamp) {

    }

    @Override
    public void addConsumed(TopicPartition partition, OffsetRange range, Instant consumedAt) {

    }

    @Override
    public void updateProcessed(TopicPartition partition, long offset) {

    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Set<TopicPartition> assignedPartitions, Instant minConsumedAt) {
//        logger.warn("old offsetToCommit [{}] not equal to {} upper endpoint", offsetToCommit, processedOffsetsFirstRange);
        return null;
    }

    @Override
    public void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {

    }
}
