package com.rtbhouse.kafka.workers.impl.offsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.rtbhouse.kafka.workers.impl.errors.BadOffsetException;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingTimeoutException;

public class OffsetsStateTest {

    @Test
    public void shouldUpdateOffsetsInconsecutively() {

        // given (consumed and processed: 0-5)
        Set<TopicPartition> partitions = Stream.of(new TopicPartition("topic", 0)).collect(Collectors.toSet());
        OffsetsState offsetsState = new OffsetsState();
        offsetsState.register(partitions);
        for (long l = 0L; l <= 5L; l++) {
            offsetsState.addConsumed(new TopicPartition("topic", 0), l, 10L);
        }
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 1L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 3L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 0L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 5L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 2L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 4L);

        // when
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(partitions, 10L);

        // then
        assertThat(offsets.size()).isEqualTo(1);
        assertThat(offsets.get(new TopicPartition("topic", 0)).offset()).isEqualTo(6L);
    }

    @Test
    public void shouldUpdateOffsetsDiscontinuously() {

        // given (consumed: 0-6, processed: 0,1,2,4,5,6)
        Set<TopicPartition> partitions = Stream.of(new TopicPartition("topic", 0)).collect(Collectors.toSet());
        OffsetsState offsetsState = new OffsetsState();
        offsetsState.register(partitions);
        for (long l = 0L; l <= 6L; l++) {
            offsetsState.addConsumed(new TopicPartition("topic", 0), l, 10L);
        }
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 0L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 1L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 2L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 4L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 5L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 6L);

        // when
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(partitions, 10L);

        // then
        assertThat(offsets.size()).isEqualTo(1);
        assertThat(offsets.get(new TopicPartition("topic", 0)).offset()).isEqualTo(3L);
    }

    @Test
    public void shouldUpdateOffsetsIndependently() {

        // given (consumed and processed: 0 -> 0,1,2; 1 -> 3,4,5)
        Set<TopicPartition> partitions = Stream
                .of(new TopicPartition("topic", 0), new TopicPartition("topic", 1))
                .collect(Collectors.toSet());
        OffsetsState offsetsState = new OffsetsState();
        offsetsState.register(partitions);
        for (long l = 0L; l <= 2L; l++) {
            offsetsState.addConsumed(new TopicPartition("topic", 0), l, 10L);
        }
        for (long l = 3L; l <= 5L; l++) {
            offsetsState.addConsumed(new TopicPartition("topic", 1), l, 10L);
        }
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 0L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 1L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 2L);
        offsetsState.updateProcessed(new TopicPartition("topic", 1), 3L);
        offsetsState.updateProcessed(new TopicPartition("topic", 1), 4L);
        offsetsState.updateProcessed(new TopicPartition("topic", 1), 5L);

        // when
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(partitions, 10L);

        // then
        assertThat(offsets.size()).isEqualTo(2);
        assertThat(offsets.get(new TopicPartition("topic", 0)).offset()).isEqualTo(3L);
        assertThat(offsets.get(new TopicPartition("topic", 1)).offset()).isEqualTo(6L);
    }

    @Test
    public void shouldNotReturnNotProcessedOffsets() {

        // given (consumed: 0,1,2,3, processed: 1,2,3)
        Set<TopicPartition> partitions = Stream.of(new TopicPartition("topic", 0)).collect(Collectors.toSet());
        OffsetsState offsetsState = new OffsetsState();
        offsetsState.register(partitions);
        for (long l = 0L; l <= 3L; l++) {
            offsetsState.addConsumed(new TopicPartition("topic", 0), l, 10L);
        }
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 1L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 2L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 3L);

        // when
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsState.getOffsetsToCommit(partitions, 10L);

        // then
        assertThat(offsets.size()).isEqualTo(0);
    }

    @Test
    public void shouldNotUpdateNotConsumedOffset() {

        // given (consumed: 0,1,2, processed: 0,1,2,3!)
        Set<TopicPartition> partitions = Stream.of(new TopicPartition("topic", 0)).collect(Collectors.toSet());
        OffsetsState offsetsState = new OffsetsState();
        offsetsState.register(partitions);
        for (long l = 0L; l <= 2L; l++) {
            offsetsState.addConsumed(new TopicPartition("topic", 0), l, 10L);
        }

        assertThatCode(() -> {
            offsetsState.updateProcessed(new TopicPartition("topic", 0), 0L);
            offsetsState.updateProcessed(new TopicPartition("topic", 0), 1L);
            offsetsState.updateProcessed(new TopicPartition("topic", 0), 2L);
        }).doesNotThrowAnyException();

        assertThatThrownBy(() -> {
            offsetsState.updateProcessed(new TopicPartition("topic", 0), 3L);
        }).isInstanceOf(BadOffsetException.class).hasMessageContaining("Offset: 3 for partition: topic-0 was not consumed before");
    }

    @Test
    public void shouldNotUpdateProcessedOffset() {

        // given (consumed: 0,1,2, processed: 0,1,2,2!)
        Set<TopicPartition> partitions = Stream.of(new TopicPartition("topic", 0)).collect(Collectors.toSet());
        OffsetsState offsetsState = new OffsetsState();
        offsetsState.register(partitions);
        for (long l = 0L; l <= 2L; l++) {
            offsetsState.addConsumed(new TopicPartition("topic", 0), l, 10L);
        }

        assertThatCode(() -> {
            offsetsState.updateProcessed(new TopicPartition("topic", 0), 0L);
            offsetsState.updateProcessed(new TopicPartition("topic", 0), 1L);
            offsetsState.updateProcessed(new TopicPartition("topic", 0), 2L);
        }).doesNotThrowAnyException();

        assertThatThrownBy(() -> {
            offsetsState.updateProcessed(new TopicPartition("topic", 0), 2L);
        }).isInstanceOf(BadOffsetException.class).hasMessageContaining("Offset: 2 for partition: topic-0 was processed before");
    }

    @Test
    public void shouldTimeoutConsumedOffsets() {

        // given (consumed: 0,1,2, processed: 0
        Set<TopicPartition> partitions = Stream.of(new TopicPartition("topic", 0)).collect(Collectors.toSet());
        OffsetsState offsetsState = new OffsetsState();
        offsetsState.register(partitions);
        offsetsState.addConsumed(new TopicPartition("topic", 0), 0L, 10L);
        offsetsState.addConsumed(new TopicPartition("topic", 0), 1L, 15L);
        offsetsState.addConsumed(new TopicPartition("topic", 0), 2L, 20L);
        offsetsState.updateProcessed(new TopicPartition("topic", 0), 0L);

        assertThatThrownBy(() -> {
            offsetsState.getOffsetsToCommit(partitions, 20L);
        }).isInstanceOf(ProcessingTimeoutException.class).hasMessageContaining("Offset: 1 for partition: topic-0 exceeded timeout");
    }

}
