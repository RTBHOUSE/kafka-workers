package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class SkipRecordProcessingAction<K, V> extends BaseAction<K, V> implements RecordProcessingOnFailureAction {

    public SkipRecordProcessingAction(WorkerRecord<K, V> workerRecord, WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState) {
        super(workerRecord, config, metrics, offsetsState);
    }

    @Override
    public void handleFailure(Exception exception) {
        //TODO: increment skipped records counter
        markRecordProcessed();
    }
}
