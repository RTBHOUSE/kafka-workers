package com.rtbhouse.kafka.workers.api.observer;

import java.util.Set;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.rtbhouse.kafka.workers.api.task.WorkerTask;

/**
 * Every {@link WorkerRecord} is associated with its {@link StatusObserver} which purpose is to report
 * record final status. It means that eventually one of methods: {@link #onSuccess()} or {@link #onFailure(Exception)}
 * has to be called for every {@link WorkerRecord} previously passed to {@link WorkerTask#process(WorkerRecord, StatusObserver)}.
 */
public interface StatusObserver {

    /**
     * Should be called when {@link WorkerRecord} or batch of records was processed successfully. Information will be used to commit
     * related offset or offsets by internal {@link KafkaConsumer}.
     */
    void onSuccess();

    /**
     * Should be called when {@link WorkerRecord} or batch of record could not be processed because of any failure. In that case whole
     * processing will be resumed from last committed offset.
     *
     * @param exception
     *            exception which caused a failure
     */
    void onFailure(Exception exception);

    /**
     * Could be used for efficient batching of observers.
     *
     * @param observer
     */
    void combine(StatusObserver observer);

    WorkerSubpartition subpartition();

    Set<Long> offsets();

}
