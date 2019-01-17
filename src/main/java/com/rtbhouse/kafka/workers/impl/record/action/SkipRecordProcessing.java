package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class SkipRecordProcessing<K, V> extends BaseAction<K, V> implements RecordProcessingOnFailureAction<K, V> {

    public SkipRecordProcessing(WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState) {
        super(config, metrics, offsetsState);
    }

    @Override
    public void handleFailure(WorkerRecord<K, V> record, Exception exception) {
        //TODO: increment skipped records counter
        markRecordProcessed(record);
    }
}
