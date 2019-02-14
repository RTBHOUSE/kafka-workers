package com.rtbhouse.kafka.workers.impl.errors;

import java.util.Arrays;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.record.RecordStatusObserverImpl.RecordStatus;

public class IllegalObserverUsageException extends RuntimeException {

    public IllegalObserverUsageException(RecordStatus status, String observerMethod) {
        super(String.format("Record is marked as [%s], but observer's [%s] method has been called", status, observerMethod));
    }

    public IllegalObserverUsageException(WorkerRecord record, RecordStatus status, String observerMethod) {
        super(String.format("Record %s is marked as [%s], but observer's [%s] method has been called",
                record, status, observerMethod));
    }

    public IllegalObserverUsageException(WorkerRecord record, RecordStatus status, String observerMethod, Object... params) {
        super(String.format("Record %s is marked as [%s], but observer's [%s] method has been called with params: %s",
                record, status, observerMethod, Arrays.toString(params)));
    }
}
