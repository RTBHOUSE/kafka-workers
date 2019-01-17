package com.rtbhouse.kafka.workers.impl.record.action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.record.action.FailureActionName;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class LoggingFailureAction<K, V> extends BaseAction<K, V> implements RecordProcessingOnFailureAction<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(LoggingFailureAction.class);

    private final FailureActionName actionName;
    private final RecordProcessingOnFailureAction<K, V> action;

    public LoggingFailureAction(WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState,
                                FailureActionName actionName, RecordProcessingOnFailureAction<K, V> action) {
        super(config, metrics, offsetsState);
        this.actionName = actionName;
        this.action = action;
    }

    @Override
    public void handleFailure(WorkerRecord<K, V> record, Exception exception) {
        logger.warn("Exception when processing record {} (action: {})", record, actionName, exception);
        action.handleFailure(record, exception);
    }
}
