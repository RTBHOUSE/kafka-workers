package com.rtbhouse.kafka.workers.api.record;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.rtbhouse.kafka.workers.api.task.WorkerTask;

/**
 * Every {@link WorkerRecord} is associated with its {@link RecordStatusObserver} which purpose is to report
 * record final status. It means that eventually one of methods: {@link #onSuccess()} or {@link #onFailure(Exception)}
 * has to be called for every {@link WorkerRecord} previously passed to
 * {@link WorkerTask#process(WorkerRecord, RecordStatusObserver)}.
 */
public interface RecordStatusObserver {

    /**
     * Should be called when {@link WorkerRecord} was processed successfully. Information will be used to commit related
     * offset by internal {@link KafkaConsumer}.
     */
    void onSuccess();

    /**
     * Should be called when {@link WorkerRecord} could not be processed because of any failure. In that case whole
     * processing will be resumed from last committed offsets.
     *
     * @param exception
     *            exception which caused a failure
     */
    void onFailure(Exception exception);

}
