package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public abstract class BaseAction<K, V> {

    protected final WorkersConfig config;

    protected final WorkersMetrics metrics;

    protected final OffsetsState offsetsState;

    BaseAction(WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState) {
        this.config = config;
        this.metrics = metrics;
        this.offsetsState = offsetsState;
    }

    protected void markRecordProcessed(WorkerRecord<K, V> record) {
        WorkerSubpartition subpartition = record.workerSubpartition();
        long offset = record.offset();
        metrics.recordSensor(WorkersMetrics.PROCESSED_OFFSET_METRIC, subpartition, offset);
        offsetsState.updateProcessed(subpartition.topicPartition(), offset);
    }
}
