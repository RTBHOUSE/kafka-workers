package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingFailureException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

public class ShutdownWorkerThread<K, V> extends BaseAction<K, V> implements RecordProcessingOnFailureAction<K, V> {

    private final WorkerThread<K, V> workerThread;

    public ShutdownWorkerThread(WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState,
                                WorkerThread<K, V> workerThread) {
        super(config, metrics, offsetsState);
        this.workerThread = workerThread;
    }

    @Override
    public void handleFailure(WorkerRecord<K, V> record, Exception exception) {
        workerThread.shutdown(new ProcessingFailureException(
                "record processing failed, subpartition: " + record.workerSubpartition() +
                        " , offset: " + record.offset(), exception));
    }
}
