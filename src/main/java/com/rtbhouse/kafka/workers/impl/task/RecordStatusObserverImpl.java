package com.rtbhouse.kafka.workers.impl.task;

import static com.rtbhouse.kafka.workers.impl.task.RecordStatusObserverImpl.RecordStatus.FAILED;
import static com.rtbhouse.kafka.workers.impl.task.RecordStatusObserverImpl.RecordStatus.PROCESSING;
import static com.rtbhouse.kafka.workers.impl.task.RecordStatusObserverImpl.RecordStatus.SUCCEEDED;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.record.RecordProcessingOnFailureAction;
import com.rtbhouse.kafka.workers.api.record.RecordProcessingOnSuccessAction;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

public class RecordStatusObserverImpl<K, V> implements RecordStatusObserver {

    enum RecordStatus {
        PROCESSING,
        SUCCEEDED,
        FAILED
    }

    private static final Logger logger = LoggerFactory.getLogger(RecordStatusObserverImpl.class);

    private final WorkerRecord<K, V> record;
    private final AtomicReference<RecordStatus> recordStatus;
    private final RecordProcessingOnSuccessAction<K, V> onSuccessAction;
    private final RecordProcessingOnFailureAction<K, V> onFailureAction;

    public RecordStatusObserverImpl(
            WorkerRecord<K, V> record,
            RecordProcessingOnSuccessAction<K, V> onSuccessAction,
            RecordProcessingOnFailureAction<K, V> onFailureAction) {
        this.record = record;
        this.recordStatus = new AtomicReference<>(PROCESSING);
        this.onSuccessAction = onSuccessAction;
        this.onFailureAction = onFailureAction;
    }

    @Override
    public void onSuccess() {
        if (recordStatus.compareAndSet(PROCESSING, SUCCEEDED)) {
            onSuccessAction.handleSuccess(record);
        } else {
            logIllegalObserverUsage(recordStatus.get(), "onSuccess");
        }
    }

    @Override
    public void onFailure(Exception exception) {
        if (recordStatus.compareAndSet(PROCESSING, FAILED)) {
            onFailureAction.handleFailure(record, exception);
        } else {
            logIllegalObserverUsage(recordStatus.get(), "onFailure");
        }
    }

    private void logIllegalObserverUsage(RecordStatus recordStatus, String method) {
        logger.warn("Record {} is marked as {}, but observer's {} method has been called which should not happen",
                record, recordStatus, method);
    }

}
