package com.rtbhouse.kafka.workers.impl.consumer;

import java.util.Collection;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;

public class ConsumerRebalanceListenerImpl<K, V> implements ConsumerRebalanceListener {

    private final KafkaWorkersImpl<K, V> workers;
    private final Set<TopicPartition> currentlyAssignedPartitions;
    private RuntimeException exception;

    public ConsumerRebalanceListenerImpl(KafkaWorkersImpl<K, V> workers, Set<TopicPartition> currentlyAssignedPartitions) {
        this.workers = workers;
        this.currentlyAssignedPartitions = currentlyAssignedPartitions;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        try {
            currentlyAssignedPartitions.removeAll(partitions);
            workers.unregister(partitions);
        } catch (InterruptedException e) {
            exception = new WorkersException("InterruptedException", e);
            throw exception;
        } catch (RuntimeException e) {
            exception = e;
            throw exception;
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        try {
            currentlyAssignedPartitions.addAll(partitions);
            workers.register(partitions);
        } catch (InterruptedException e) {
            exception = new WorkersException("InterruptedException", e);
            throw exception;
        } catch (RuntimeException e) {
            exception = e;
            throw exception;
        }
    }

    public void rethrowExceptionCaughtDuringRebalance() {
        // this hack is due to https://issues.apache.org/jira/browse/KAFKA-4600
        if (exception != null) {
            throw new RuntimeException(exception);
        }
    }
}
