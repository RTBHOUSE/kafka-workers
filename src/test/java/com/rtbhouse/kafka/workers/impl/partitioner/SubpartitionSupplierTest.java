package com.rtbhouse.kafka.workers.impl.partitioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerPartitioner;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.impl.errors.BadSubpartitionException;

public class SubpartitionSupplierTest {

    @Test
    public void shouldReturnSubpartition() {

        // given
        SubpartitionSupplier<byte[], byte[]> subpartitionSupplier = new SubpartitionSupplier<>(new WorkerPartitioner<byte[], byte[]>() {

            @Override
            public int subpartition(ConsumerRecord<byte[], byte[]> consumerRecord) {
                return 2;
            }

            @Override
            public int count(TopicPartition topicPartition) {
                return 3;
            }
        });

        // when
        WorkerSubpartition subpartition = subpartitionSupplier.subpartition(new ConsumerRecord<>("topic", 1, 0L, null, null));

        // then
        assertThat(subpartition).isEqualTo(new WorkerSubpartition("topic", 1, 2));
    }

    @Test
    public void shouldNotReturnSubpartitionOutOfRange() {

        // given
        SubpartitionSupplier<byte[], byte[]> subpartitionSupplier = new SubpartitionSupplier<>(new WorkerPartitioner<byte[], byte[]>() {

            @Override
            public int subpartition(ConsumerRecord<byte[], byte[]> consumerRecord) {
                return 3;
            }

            @Override
            public int count(TopicPartition topicPartition) {
                return 3;
            }
        });

        assertThatThrownBy(() -> {
            subpartitionSupplier.subpartition(new ConsumerRecord<>("topic", 0, 0L, null, null));
        }).isInstanceOf(BadSubpartitionException.class).hasMessageContaining("Invalid subpartition: 3");
    }

    @Test
    public void shouldReturnSubpartitions() {

        // given
        SubpartitionSupplier<byte[], byte[]> subpartitionSupplier = new SubpartitionSupplier<>(new WorkerPartitioner<byte[], byte[]>() {

            @Override
            public int subpartition(ConsumerRecord<byte[], byte[]> consumerRecord) {
                return 1;
            }

            @Override
            public int count(TopicPartition topicPartition) {
                return topicPartition.partition();
            }
        });

        // when
        List<WorkerSubpartition> subpartitions = subpartitionSupplier
                .subpartitions(Arrays.asList(new TopicPartition("topic", 2), new TopicPartition("topic", 3)));

        // then
        assertThat(subpartitions.size()).isEqualTo(5);
        assertThat(subpartitions.get(0)).isEqualTo(new WorkerSubpartition("topic", 2, 0));
        assertThat(subpartitions.get(1)).isEqualTo(new WorkerSubpartition("topic", 2, 1));
        assertThat(subpartitions.get(2)).isEqualTo(new WorkerSubpartition("topic", 3, 0));
        assertThat(subpartitions.get(3)).isEqualTo(new WorkerSubpartition("topic", 3, 1));
        assertThat(subpartitions.get(4)).isEqualTo(new WorkerSubpartition("topic", 3, 2));
    }

}
