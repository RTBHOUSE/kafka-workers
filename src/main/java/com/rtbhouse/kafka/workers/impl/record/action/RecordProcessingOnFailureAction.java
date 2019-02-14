package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.impl.errors.RecordProcessingActionException;

public interface RecordProcessingOnFailureAction {

    void handleFailure(Exception exception) throws RecordProcessingActionException;
}
