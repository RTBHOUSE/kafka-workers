package com.rtbhouse.kafka.workers.api.task;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.observer.BatchStatusObserver;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.observer.StatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

/**
 * User-defined task which is associated with one of {@link WorkerSubpartition}s.
 */
public interface WorkerTask<K, V> {

    /**
     * Will be called every time when given {@link WorkerSubpartition} is being associated with {@code KafkaWorkers}
     * instance.
     *
     * @param subpartition
     *            {@link WorkerSubpartition} subpartition associated with given task
     * @param config
     *            {@link WorkersConfig} configuration
     */
    default void init(WorkerSubpartition subpartition, WorkersConfig config) {
    }

    /**
     * Checks if given {@link WorkerRecord} could be polled from internal {@link WorkerSubpartition}'s queue peek and
     * passed to {@link #process(WorkerRecord, StatusObserver)} method.
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
     * done synchronously or asynchronously. In both cases, one of the {@link StatusObserver#onSuccess()} or
     * {@link StatusObserver#onFailure(Exception)} methods has to be called. Not calling any of these methods for
     * configurable amount of time (by {@link WorkersConfig#CONSUMER_PROCESSING_TIMEOUT_MS}) will be considered as a
     * failure.
     *
     * @param record
     *            {@link WorkerRecord} to process
     * @param observer
     *            {@link StatusObserver} associated with given {@link WorkerRecord}
     */
    void process(WorkerRecord<K, V> record, StatusObserver observer);

    /**
     * Allows to do maintenance tasks every configurable amount of time independently if there are records to process or not.
     * All the methods: accept(), process() and punctuate() are executed in a single thread so synchronization is not necessary.
     *
     * @param punctuateTime
     *              current time when punctuate() is called
     */
    default void punctuate(long punctuateTime) {
    }

    /**
     * Will be called every time when given {@link WorkerSubpartition} is being revoked from {@code KafkaWorkers}
     * instance.
     */
    default void close() {
    }

}
