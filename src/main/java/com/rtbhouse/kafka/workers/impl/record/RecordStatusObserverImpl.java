package com.rtbhouse.kafka.workers.impl.record;

import static com.rtbhouse.kafka.workers.impl.record.RecordStatusObserverImpl.RecordStatus.FAILED;
import static com.rtbhouse.kafka.workers.impl.record.RecordStatusObserverImpl.RecordStatus.PROCESSING;
import static com.rtbhouse.kafka.workers.impl.record.RecordStatusObserverImpl.RecordStatus.SUCCEEDED;

import java.util.concurrent.atomic.AtomicReference;

import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.IllegalObserverUsageException;
import com.rtbhouse.kafka.workers.impl.record.action.RecordProcessingOnFailureAction;
import com.rtbhouse.kafka.workers.impl.record.action.RecordProcessingOnSuccessAction;

public class RecordStatusObserverImpl<K, V> implements RecordStatusObserver {

    public enum RecordStatus {
        PROCESSING,
        SUCCEEDED,
        FAILED
    }

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
            throw new IllegalObserverUsageException(record, recordStatus.get(), "onSuccess");
        }
    }

    @Override
    public void onFailure(Exception exception) {
        if (recordStatus.compareAndSet(PROCESSING, FAILED)) {
            onFailureAction.handleFailure(record, exception);
        } else {
            throw new IllegalObserverUsageException(record, recordStatus.get(), "onFailure", exception);
        }
    }

}
