package com.rtbhouse.kafka.workers.impl.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.record.RecordProcessingOnFailureAction;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

class BaseFailureAction<K, V> implements RecordProcessingOnFailureAction<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(BaseFailureAction.class);

    private final FailureActionName actionName;
    private final RecordProcessingOnFailureAction<K, V> action;

    BaseFailureAction(FailureActionName actionName, RecordProcessingOnFailureAction<K, V> action) {
        this.actionName = actionName;
        this.action = action;
    }

    @Override
    public void handleFailure(WorkerRecord<K, V> record, Exception exception) {
        logger.warn("Exception when processing record (action: {}): {}", actionName, record);
        action.handleFailure(record, exception);
    }
}
