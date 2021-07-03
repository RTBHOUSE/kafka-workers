package com.rtbhouse.kafka.workers.impl.record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.RecordProcessingGuarantee;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingFailureException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

public class RecordStatusObserverImpl<K, V> implements RecordStatusObserver {

    private static final Logger logger = LoggerFactory.getLogger(RecordStatusObserverImpl.class);

    // TODO: revert public -> protected (or event private) for all fields
    public final WorkerSubpartition subpartition;
    public final long offset;
    public final Context<K, V> context;

    public RecordStatusObserverImpl(
            WorkerRecord<K, V> record,
            Context<K, V> context
    ) {
        this.subpartition = WorkerSubpartition.getInstance(record.topicPartition(), record.subpartition());
        this.offset = record.offset();
        this.context = context;
    }

    @Override
    public void onSuccess() {
        markRecordProcessed();
    }

    @Override
    public void onFailure(Exception exception) {
        if (RecordProcessingGuarantee.AT_LEAST_ONCE.equals(context.getRecordProcessingGuarantee())) {
            context.workerThread.shutdown(new ProcessingFailureException(
                    "record processing failed, subpartition: " + subpartition + " , offset: " + offset, exception));
        } else {
            logger.warn("record processing failed, subpartition: " + subpartition + "offset: " + offset, exception);
            markRecordProcessed();
        }
    }

    private void markRecordProcessed() {
        context.metrics.recordSensor(WorkersMetrics.PROCESSED_OFFSET_METRIC, subpartition, offset);
        context.offsetsState.updateProcessed(subpartition.topicPartition(), offset);
    }

    public static class Context<K, V> {

        final WorkersMetrics metrics;
        final WorkersConfig config;
        final OffsetsState offsetsState;
        final WorkerThread<K, V> workerThread;

        public Context(WorkersMetrics metrics, WorkersConfig config,
                       OffsetsState offsetsState, WorkerThread<K, V> workerThread) {
            this.metrics = metrics;
            this.config = config;
            this.offsetsState = offsetsState;
            this.workerThread = workerThread;
        }

        RecordProcessingGuarantee getRecordProcessingGuarantee() {
            return config.getRecordProcessingGuarantee();
        }
    }
}
