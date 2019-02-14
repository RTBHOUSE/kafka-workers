package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.record.action.FailureActionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingFailureAction<K, V> implements RecordProcessingOnFailureAction {

    private static final Logger logger = LoggerFactory.getLogger(LoggingFailureAction.class);

    private final FailureActionName actionName;
    private final RecordProcessingOnFailureAction action;

    public LoggingFailureAction(FailureActionName actionName, RecordProcessingOnFailureAction action) {
        this.actionName = actionName;
        this.action = action;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleFailure(Exception exception) {
        if (RecordRetainingAction.class.isAssignableFrom(action.getClass())) {
            logger.warn("Exception when processing record: {} (action: {})", ((RecordRetainingAction<K, V>) action).getWorkerRecord(),
                    actionName, exception);
        } else if (BaseAction.class.isAssignableFrom(action.getClass())) {
            BaseAction<K, V> baseAction = (BaseAction<K, V>) action;
            logger.warn("Exception when processing record from topic: {} and offset: {} (action: {})",
                    baseAction.subpartition, baseAction.offset, actionName, exception);
        } else {
            logger.warn("Exception when processing record (action: {})", actionName, exception);
        }
        action.handleFailure(exception);
    }
}
