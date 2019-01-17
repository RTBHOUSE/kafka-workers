package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class MarkRecordProcessed<K, V> extends BaseAction<K, V> implements RecordProcessingOnSuccessAction<K, V> {

    public MarkRecordProcessed(WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState) {
        super(config, metrics, offsetsState);
    }

    @Override
    public void handleSuccess(WorkerRecord<K, V> record) {
        markRecordProcessed(record);
    }
}
