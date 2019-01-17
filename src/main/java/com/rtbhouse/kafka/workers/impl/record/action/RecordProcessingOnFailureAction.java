package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.RecordProcessingActionException;

public interface RecordProcessingOnFailureAction<K, V> {

    void handleFailure(WorkerRecord<K, V> record, Exception exception) throws RecordProcessingActionException;
}
