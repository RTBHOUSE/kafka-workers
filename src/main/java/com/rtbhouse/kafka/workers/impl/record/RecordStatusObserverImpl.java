package com.rtbhouse.kafka.workers.impl.record;

import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.impl.errors.IllegalObserverUsageException;
import com.rtbhouse.kafka.workers.impl.record.action.RecordProcessingOnFailureAction;
import com.rtbhouse.kafka.workers.impl.record.action.RecordProcessingOnSuccessAction;
import com.rtbhouse.kafka.workers.impl.record.action.RecordRetainingAction;

import java.util.concurrent.atomic.AtomicReference;

import static com.rtbhouse.kafka.workers.impl.record.RecordStatusObserverImpl.RecordStatus.FAILED;
import static com.rtbhouse.kafka.workers.impl.record.RecordStatusObserverImpl.RecordStatus.PROCESSING;
import static com.rtbhouse.kafka.workers.impl.record.RecordStatusObserverImpl.RecordStatus.SUCCEEDED;

public class RecordStatusObserverImpl implements RecordStatusObserver {

    public enum RecordStatus {
        PROCESSING,
        SUCCEEDED,
        FAILED
    }

    private final AtomicReference<RecordStatus> recordStatus;
    private final RecordProcessingOnSuccessAction onSuccessAction;
    private final RecordProcessingOnFailureAction onFailureAction;

    public RecordStatusObserverImpl(
            RecordProcessingOnSuccessAction onSuccessAction,
            RecordProcessingOnFailureAction onFailureAction) {
        this.recordStatus = new AtomicReference<>(PROCESSING);
        this.onSuccessAction = onSuccessAction;
        this.onFailureAction = onFailureAction;
    }

    @Override
    public void onSuccess() {
        if (recordStatus.compareAndSet(PROCESSING, SUCCEEDED)) {
            onSuccessAction.handleSuccess();
        } else {
            if (RecordRetainingAction.class.isAssignableFrom(onSuccessAction.getClass())) {
                throw new IllegalObserverUsageException(((RecordRetainingAction) onSuccessAction).getWorkerRecord(),
                        recordStatus.get(), "onSuccess");
            } else {
                throw new IllegalObserverUsageException(recordStatus.get(), "onSuccess");
            }
        }
    }

    @Override
    public void onFailure(Exception exception) {
        if (recordStatus.compareAndSet(PROCESSING, FAILED)) {
            onFailureAction.handleFailure(exception);
        } else {
            if (RecordRetainingAction.class.isAssignableFrom(onFailureAction.getClass())) {
                throw new IllegalObserverUsageException(((RecordRetainingAction) onFailureAction).getWorkerRecord(),
                        recordStatus.get(), "onFailure");
            } else {
                throw new IllegalObserverUsageException(recordStatus.get(), "onFailure");
            }
        }
    }

}
