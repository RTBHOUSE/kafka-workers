package com.rtbhouse.kafka.workers.impl.offsets;

import static com.rtbhouse.kafka.workers.impl.range.ClosedRange.range;
import static junitparams.JUnitParamsRunner.$;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.impl.errors.BadOffsetException;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingTimeoutException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.range.ClosedRange;
import com.rtbhouse.kafka.workers.impl.range.RangeUtils;
import com.rtbhouse.kafka.workers.integration.utils.TestProperties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public abstract class OffsetsStateTest {

    private static final TopicPartition TOPIC_PARTITION_0 = new TopicPartition("topic", 0);
    private static final TopicPartition TOPIC_PARTITION_1 = new TopicPartition("topic", 1);

    WorkersMetrics mockMetrics = Mockito.mock(WorkersMetrics.class);

    WorkersConfig config = new WorkersConfig(TestProperties.workersProperties());

    abstract OffsetsState createOffsetsStateSubject();

    @Test
    public void shouldNotConsumeAlreadyConsumedOffset() throws InterruptedException {
        //given
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);

        long offset = 123L;
        offsetsState.addConsumed(TOPIC_PARTITION_0, offset, Instant.now());

        //then
        assertThatThrownBy(() -> offsetsState.addConsumed(TOPIC_PARTITION_0, offset, Instant.now()))
                .isInstanceOf(BadOffsetException.class)
                .hasMessage("Offset: 123 for partition: topic-0 was consumed before");
    }

    private Object[] parametersForShouldNotConsumeAlreadyConsumedOffsetFromRange() {
        return $(
                $(range(10L, 20L), range(0L, 10L), 10L),
                $(range(10L, 20L), range(0L, 15L), 10L),
                $(range(10L, 20L), range(0L, 20L), 10L),
                $(range(10L, 20L), range(0L, 30L), 10L),
                $(range(10L, 20L), range(10L, 10L), 10L),
                $(range(10L, 20L), range(10L, 20L), 10L),
                $(range(10L, 20L), range(15L, 20L), 15L),
                $(range(10L, 20L), range(15L, 30L), 15L),
                $(range(10L, 20L), range(20L, 20L), 20L),
                $(range(10L, 20L), range(20L, 30L), 20L)
        );
    }

    @Test
    @Parameters
    public void shouldNotConsumeAlreadyConsumedOffsetFromRange(ClosedRange consumedOffsets,
                                                               ClosedRange offsetsToConsume,
                                                               Long minExistingOffset) throws InterruptedException {
        //given
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        Instant consumedAt = Instant.now();
        OffsetsState offsetsState = offsetsStateWithConsumedOffsets(partitions, consumedOffsets, consumedAt);

        //then
        assertThatThrownBy(() -> offsetsState.addConsumed(TOPIC_PARTITION_0, offsetsToConsume, consumedAt))
                .isInstanceOf(BadOffsetException.class)
                .hasMessage("Offset: %s for partition: %s was consumed before", minExistingOffset, TOPIC_PARTITION_0);
    }

    private OffsetsState offsetsStateWithConsumedOffsets(Set<TopicPartition> partitions, ClosedRange consumedOffsets, Instant consumedAt) throws InterruptedException {
        Preconditions.checkState(partitions.size() == 1);
        TopicPartition partition = partitions.iterator().next();
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        offsetsState.addConsumed(partition, consumedOffsets, consumedAt);
        return offsetsState;
    }

    @Test
    public void shouldUpdateOffsetsInconsecutively() throws InterruptedException {

        // given (consumed [0-10], processed [0-5])
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        Instant consumedAt = Instant.ofEpochMilli(10L);
        for (long offset = 0L; offset <= 10L; offset++) {
            offsetsState.addConsumed(TOPIC_PARTITION_0, offset, consumedAt);
        }
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 1L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 3L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 0L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 5L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 2L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 4L);

        // when
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(partitions, consumedAt);

        // then
        assertThat(offsets.size()).isEqualTo(1);
        assertThat(offsets.get(TOPIC_PARTITION_0).offset()).isEqualTo(6L);
    }

    @Test
    public void shouldUpdateOffsetsDiscontinuously() throws InterruptedException {

        // given (consumed: [0-6], processed: [0-2], [4-6])
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        Instant consumedAt = Instant.ofEpochMilli(10L);
        for (long offset = 0L; offset <= 6L; offset++) {
            offsetsState.addConsumed(TOPIC_PARTITION_0, offset, consumedAt);
        }

        offsetsState.updateProcessed(TOPIC_PARTITION_0, 0L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 1L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 2L);

        offsetsState.updateProcessed(TOPIC_PARTITION_0, 4L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 5L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 6L);

        // when
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(partitions, consumedAt);

        // then
        assertThat(offsets).containsOnly(
                entry(TOPIC_PARTITION_0, new OffsetAndMetadata(3L))
        );
    }

    @Test
    public void shouldUpdateOffsetsIndependently() throws InterruptedException {

        // given (consumed and processed: 0 -> 0,1,2; 1 -> 3,4,5)
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0, TOPIC_PARTITION_1);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        Instant consumedAt = Instant.ofEpochMilli(10L);
        for (long l = 0L; l <= 2L; l++) {
            offsetsState.addConsumed(TOPIC_PARTITION_0, l, consumedAt);
        }
        for (long l = 3L; l <= 5L; l++) {
            offsetsState.addConsumed(TOPIC_PARTITION_1, l, consumedAt);
        }
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 0L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 1L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 2L);
        offsetsState.updateProcessed(TOPIC_PARTITION_1, 3L);
        offsetsState.updateProcessed(TOPIC_PARTITION_1, 4L);
        offsetsState.updateProcessed(TOPIC_PARTITION_1, 5L);

        // when
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(partitions, consumedAt);

        // then
        assertThat(offsets).containsOnly(
                entry(TOPIC_PARTITION_0, new OffsetAndMetadata(3L)),
                entry(TOPIC_PARTITION_1, new OffsetAndMetadata(6L))
        );
    }

    @Test
    public void shouldNotReturnNotProcessedOffsets() throws InterruptedException {

        // given (consumed: 0,1,2,3, processed: 1,2,3)
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        Instant consumedAt = Instant.ofEpochMilli(10L);
        for (long l = 0L; l <= 3L; l++) {
            offsetsState.addConsumed(TOPIC_PARTITION_0, l, consumedAt);
        }
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 1L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 2L);
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 3L);

        // when
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(partitions, consumedAt);

        // then
        assertThat(offsets).isEmpty();
    }

    @Test
    public void shouldNotUpdateNotConsumedOffset() throws InterruptedException {

        // given (consumed: 0,1,2, processed: 0,1,2,3!)
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        for (long l = 0L; l <= 2L; l++) {
            offsetsState.addConsumed(TOPIC_PARTITION_0, l, Instant.ofEpochMilli(10L));
        }

        assertThatCode(() -> {
            offsetsState.updateProcessed(TOPIC_PARTITION_0, 0L);
            offsetsState.updateProcessed(TOPIC_PARTITION_0, 1L);
            offsetsState.updateProcessed(TOPIC_PARTITION_0, 2L);
        }).doesNotThrowAnyException();

        assertThatThrownBy(() -> {
            offsetsState.updateProcessed(TOPIC_PARTITION_0, 3L);
        }).isInstanceOf(BadOffsetException.class).hasMessageContaining("Offset: 3 for partition: topic-0 was not consumed before");
    }

    @Test
    public void shouldNotUpdateProcessedOffset() throws InterruptedException {

        // given (consumed: 0,1,2, processed: 0,1,2,2!)
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        for (long l = 0L; l <= 2L; l++) {
            offsetsState.addConsumed(TOPIC_PARTITION_0, l, Instant.ofEpochMilli(10L));
        }

        assertThatCode(() -> {
            offsetsState.updateProcessed(TOPIC_PARTITION_0, 0L);
            offsetsState.updateProcessed(TOPIC_PARTITION_0, 1L);
            offsetsState.updateProcessed(TOPIC_PARTITION_0, 2L);
        }).doesNotThrowAnyException();

        assertThatThrownBy(() -> {
            offsetsState.updateProcessed(TOPIC_PARTITION_0, 2L);
        }).isInstanceOf(BadOffsetException.class).hasMessageContaining("Offset: 2 for partition: topic-0 was processed before");
    }

    @Test
    public void shouldTimeoutConsumedOffsets() throws InterruptedException {

        // given (consumed: 0,1,2, processed: 0
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        offsetsState.addConsumed(TOPIC_PARTITION_0, 0L, Instant.ofEpochMilli(10L));
        offsetsState.addConsumed(TOPIC_PARTITION_0, 1L, Instant.ofEpochMilli(15L));
        offsetsState.addConsumed(TOPIC_PARTITION_0, 2L, Instant.ofEpochMilli(20L));
        offsetsState.updateProcessed(TOPIC_PARTITION_0, 0L);

        assertThatThrownBy(() -> {
            offsetsState.getOffsetsToCommit(partitions, Instant.ofEpochMilli(20L));
        }).isInstanceOf(ProcessingTimeoutException.class).hasMessageContaining("Offset: 1 for partition: topic-0 exceeded timeout");
    }

    //TODO: NEXT

    // ten test to może być klucz do różnic wykrytych w logach
    // TAK: jeśli są różnice między Heavy a Default to wartości Heavy są zawsze większe
    private Object[] parametersForShouldReturnOffsetToCommit() {
        return $(
                $(List.of(range(0L, 5L), range(10L, 15L)), List.of(range(0L, 3L)), 0L, 3L),
                $(List.of(range(0L, 5L), range(10L, 15L)), List.of(range(0L, 3L)), 1L, 3L),
                $(List.of(range(0L, 5L), range(10L, 15L)), List.of(range(0L, 3L)), 2L, 3L),
                $(List.of(range(0L, 5L), range(10L, 15L)), List.of(range(0L, 5L), range(10L, 12L)), 4L, 12L),
                $(List.of(range(0L, 5L), range(10L, 15L)), List.of(range(0L, 5L), range(10L, 12L)), 5L, 12L),
                $(List.of(range(0L, 5L), range(10L, 15L)), List.of(range(0L, 5L), range(10L, 12L)), 10L, 12L),
                $(List.of(range(0L, 5L), range(10L, 15L)), List.of(range(0L, 5L), range(10L, 12L)), 11L, 12L)
        );
    }

    @Test
    @Parameters
    public void shouldReturnOffsetToCommit(List<ClosedRange> consumed, List<ClosedRange> processed, long committed, long expectedToCommit)
            throws InterruptedException {
        //given
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);

        consumed.forEach(
                consumedRange -> offsetsState.addConsumed(TOPIC_PARTITION_0, consumedRange)
        );

        processed.stream()
                .flatMapToLong(RangeUtils::elementsStream)
                .forEach(processedOffset -> offsetsState.updateProcessed(TOPIC_PARTITION_0, processedOffset));

        offsetsState.removeCommitted(Map.of(TOPIC_PARTITION_0, new OffsetAndMetadata(committed + 1)));

        //when
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = offsetsState.getOffsetsToCommit(partitions);

        //then
        assertThat(offsetsToCommit).containsOnly(
                entry(TOPIC_PARTITION_0, new OffsetAndMetadata(expectedToCommit + 1))
        );
    }

    public void shouldProcessOffsetsWithGaps() throws InterruptedException {
        //given
        Set<TopicPartition> partitions = ImmutableSet.of(TOPIC_PARTITION_0);
        OffsetsState offsetsState = createOffsetsStateSubject();
        offsetsState.register(partitions);
        Instant minConsumedAt = Instant.now();

        //when
        offsetsState.addConsumed(TOPIC_PARTITION_0, range(0L, 3L), minConsumedAt);
        offsetsState.addConsumed(TOPIC_PARTITION_0, range(0L, 3L), minConsumedAt);
    }

    // add this test if the one above doesn't cover this scenario
    public void shouldReturnNoOffsetsToCommitWhenCommittedAndNextNotProcessed() {

    }
}
