package com.rtbhouse.kafka.workers.api.task;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

/**
 * User-defined task which is associated with one of {@link WorkerSubpartition}s.
 */
public interface WorkerTask<K, V> {

    /**
     * Will be called every time when given {@link WorkerSubpartition} is being associated with {@code KafkaWorkers}
     * instance.
     *
     * @param config
     *            {@link WorkerSubpartition} subpartition associated with given task
     * @param config
     *            {@link KafkaWorkers} configuration
     */
    default void init(WorkerSubpartition subpartition, WorkersConfig config) {
    }

    /**
     * Checks if given {@link WorkerRecord} could be polled from internal {@link WorkerSubpartition}'s queue peek and
     * passed to {@link #process(WorkerRecord, RecordStatusObserver)} method.
     *
     * @param record
     *            {@link WorkerRecord} to check
     *
     * @return false if processing should be paused for given partition, true otherwise
     */
    default boolean accept(WorkerRecord<K, V> record) {
        return true;
    }

    /**
     * Processes just polled {@link WorkerRecord} from given {@link WorkerSubpartition}'s internal queue. It could be
     * done synchronously or asynchronously. In both cases, one of the {@link RecordStatusObserver#onSuccess()} or
     * {@link RecordStatusObserver#onFailure(Exception)} methods has to be called. Not calling any of these methods for
     * configurable amount of time (by {@link WorkersConfig#CONSUMER_PROCESSING_TIMEOUT_MS}) will be considered as a
     * failure.
     *
     * @param record
     *            {@link WorkerRecord} to process
     * @param observer
     *            {@link RecordStatusObserver} associated with given {@link WorkerRecord}
     */
    void process(WorkerRecord<K, V> record, RecordStatusObserver observer);

    /**
     * Will be called every time when given {@link WorkerSubpartition} is being revoked from {@code KafkaWorkers}
     * instance.
     */
    default void close() {
    }

}
