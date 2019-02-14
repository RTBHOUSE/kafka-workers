package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public abstract class BaseAction<K, V> {

    protected final WorkerSubpartition subpartition;

    protected final long offset;

    protected final WorkersConfig config;

    protected final WorkersMetrics metrics;

    protected final OffsetsState offsetsState;

    BaseAction(WorkerRecord<K, V> workerRecord, WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState) {
        this.subpartition = workerRecord.workerSubpartition();
        this.offset = workerRecord.offset();
        this.config = config;
        this.metrics = metrics;
        this.offsetsState = offsetsState;
    }

    protected void markRecordProcessed() {
        metrics.recordSensor(WorkersMetrics.PROCESSED_OFFSET_METRIC, subpartition, offset);
        offsetsState.updateProcessed(subpartition.topicPartition(), offset);
    }

}
