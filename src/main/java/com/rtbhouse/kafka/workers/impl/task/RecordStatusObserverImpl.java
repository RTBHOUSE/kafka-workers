package com.rtbhouse.kafka.workers.impl.task;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingFailureException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class RecordStatusObserverImpl implements RecordStatusObserver {

    private final WorkerSubpartition subpartition;
    private final long offset;

    private final WorkerThread<?, ?> workerThread;
    private final OffsetsState offsetsState;
    private final WorkersMetrics metrics;

    public RecordStatusObserverImpl(
            WorkerRecord<?, ?> record,
            WorkerThread<?, ?> workerThread,
            OffsetsState offsetsState,
            WorkersMetrics metrics) {
        this.subpartition = record.workerSubpartition();
        this.offset = record.offset();
        this.workerThread = workerThread;
        this.offsetsState = offsetsState;
        this.metrics = metrics;
    }

    @Override
    public void onSuccess() {
        metrics.recordSensor(WorkersMetrics.PROCESSED_OFFSET_METRIC, subpartition, offset);
        offsetsState.updateProcessed(subpartition.topicPartition(), offset);
    }

    @Override
    public void onFailure(Exception exception) {
        workerThread.shutdown(new ProcessingFailureException(
                "record processing failed, subpartition: " + subpartition + " , offset: " + offset, exception));
    }

}
