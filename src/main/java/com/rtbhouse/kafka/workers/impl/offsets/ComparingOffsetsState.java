package com.rtbhouse.kafka.workers.impl.offsets;

import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.rtbhouse.kafka.workers.impl.range.ClosedRange;

public class ComparingOffsetsState implements OffsetsState {

    private static final Logger logger = LoggerFactory.getLogger(ComparingOffsetsState.class);

    private final OffsetsState impl;

    private final OffsetsState referenceImpl;

    public ComparingOffsetsState(OffsetsState impl, OffsetsState referenceImpl) {
        this.impl = impl;
        this.referenceImpl = referenceImpl;
    }

    @Override
    public synchronized void addConsumed(TopicPartition partition, ClosedRange range, Instant consumedAt) {
        impl.addConsumed(partition, range, consumedAt);
        referenceImpl.addConsumed(partition, range, consumedAt);
    }

    @Override
    public synchronized void updateProcessed(TopicPartition partition, long offset) {
        impl.updateProcessed(partition, offset);
        referenceImpl.updateProcessed(partition, offset);
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Set<TopicPartition> assignedPartitions, Instant minConsumedAt) {
        var offsetsToCommit = impl.getOffsetsToCommit(assignedPartitions, minConsumedAt);
        var referenceOffsetsToCommit = referenceImpl.getOffsetsToCommit(assignedPartitions, minConsumedAt);

        if (!Objects.equals(offsetsToCommit, referenceOffsetsToCommit)) {
            Set<TopicPartition> allKeys = ImmutableSet.<TopicPartition>builder()
                    .addAll(offsetsToCommit.keySet())
                    .addAll(referenceOffsetsToCommit.keySet())
                    .build();

            String diffLines = allKeys.stream().sorted(Comparator.comparing(TopicPartition::toString))
                    .map(key -> {
                        OffsetAndMetadata om1 = offsetsToCommit.get(key);
                        OffsetAndMetadata om2 = referenceOffsetsToCommit.get(key);
                        Long offsetDiff = (om1 != null && om2 != null) ? om1.offset() - om2.offset() : null;
                        return new Object[]{key, om1, om2, offsetDiff};
                    })
                    .filter(array -> !Objects.equals(array[1], array[2]))
                    .map(array -> String.format("%s: %s != %s, offsetDiff=%s", array))
                    .collect(Collectors.joining("\n"));

            throw new IllegalStateException(String.format("Offsets to commit DO NOT match between implementations %s, %s:\n%s\n%s\ndiff:\n%s",
                    impl.getClass().getSimpleName(), referenceImpl.getClass().getSimpleName(),
                    offsetsToCommit, referenceOffsetsToCommit,
                    diffLines));
        }

        logger.debug("Offsets to commit match between implementations: {}, {}",
                impl.getClass().getSimpleName(), referenceImpl.getClass().getSimpleName());
        return offsetsToCommit;
    }

    @Override
    public synchronized void removeCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        impl.removeCommitted(offsetsAndMetadata);
        referenceImpl.removeCommitted(offsetsAndMetadata);
    }

    @Override
    public synchronized void register(Collection<TopicPartition> partitions) throws InterruptedException {
        impl.register(partitions);
        referenceImpl.register(partitions);
    }

    @Override
    public synchronized void unregister(Collection<TopicPartition> partitions) throws InterruptedException {
        impl.unregister(partitions);
        referenceImpl.unregister(partitions);
    }
}
