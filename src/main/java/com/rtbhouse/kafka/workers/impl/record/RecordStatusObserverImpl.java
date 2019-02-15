package com.rtbhouse.kafka.workers.impl.record;

import org.slf4j.Logger;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.RecordProcessingGuarantee;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingFailureException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;
import org.slf4j.LoggerFactory;

public class RecordStatusObserverImpl<K, V> implements RecordStatusObserver {

    private static final Logger logger = LoggerFactory.getLogger(RecordStatusObserverImpl.class);

    protected WorkerSubpartition subpartition;
    protected long offset;
    protected final WorkersMetrics metrics;
    protected final RecordProcessingGuarantee recordProcessingGuarantee;
    protected final OffsetsState offsetsState;
    protected final WorkerThread<K, V> workerThread;

    public RecordStatusObserverImpl(
            WorkerRecord<K, V> record,
            WorkersMetrics metrics,
            WorkersConfig config,
            OffsetsState offsetsState,
            WorkerThread<K, V> workerThread
    ) {
        this.subpartition = record.workerSubpartition();
        this.offset = record.offset();
        this.metrics = metrics;
        this.recordProcessingGuarantee = config.getRecordProcessingGuarantee();
        this.offsetsState = offsetsState;
        this.workerThread = workerThread;
    }

    @Override
    public void onSuccess() {
        markRecordProcessed();
    }

    @Override
    public void onFailure(Exception exception) {
        if (RecordProcessingGuarantee.AT_LEAST_ONCE.equals(recordProcessingGuarantee)) {
            workerThread.shutdown(new ProcessingFailureException(
                    "record processing failed, subpartition: " + subpartition + " , offset: " + offset, exception));
        } else {
            logger.warn("record processing failed, subpartition: " + subpartition + "offset: " + offset, exception);
            markRecordProcessed();
        }
    }

    private void markRecordProcessed() {
        metrics.recordSensor(WorkersMetrics.PROCESSED_OFFSET_METRIC, subpartition, offset);
        offsetsState.updateProcessed(subpartition.topicPartition(), offset);
    }
}
