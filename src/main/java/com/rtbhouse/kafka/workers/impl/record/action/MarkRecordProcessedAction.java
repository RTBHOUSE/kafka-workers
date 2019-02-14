package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class MarkRecordProcessedAction<K, V> extends BaseAction<K, V> implements RecordProcessingOnSuccessAction {

    public MarkRecordProcessedAction(WorkerRecord<K, V> workerRecord, WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState) {
        super(workerRecord, config, metrics, offsetsState);
    }

    @Override
    public void handleSuccess() {
        markRecordProcessed();
    }

}
