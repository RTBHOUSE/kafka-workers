package com.rtbhouse.kafka.workers.impl.observer;

import java.util.Set;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.task.RecordProcessingGuarantee;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingFailureException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class SubpartitionObserver {

    private final WorkerSubpartition subpartition;
    private final RecordProcessingGuarantee recordProcessingGuarantee;
    private final OffsetsState offsetsState;
    private final KafkaWorkersImpl<?, ?> workers;
    private final WorkersMetrics metrics;

    public SubpartitionObserver(
            WorkerSubpartition subpartition,
            WorkersConfig config,
            OffsetsState offsetsState,
            KafkaWorkersImpl<?, ?> workers,
            WorkersMetrics metrics) {
        this.subpartition = subpartition;
        this.recordProcessingGuarantee = config.getRecordProcessingGuarantee();
        this.offsetsState = offsetsState;
        this.workers = workers;
        this.metrics = metrics;
    }

    public void onSuccess(Set<Long> offsets) {
        markProcessed(offsets);
    }

    public void onFailure(Set<Long> offsets, Exception exception) {
        if (RecordProcessingGuarantee.AT_LEAST_ONCE.equals(recordProcessingGuarantee)) {
            workers.shutdown(new ProcessingFailureException(
                    "record processing failed, subpartition: " + subpartition + " , offsets: " + offsets, exception));
        } else {
            markProcessed(offsets);
        }
    }

    public WorkerSubpartition subpartition() {
        return subpartition;
    }

    private void markProcessed(Set<Long> offsets) {
        offsets.forEach(offset -> {
            offsetsState.updateProcessed(subpartition.topicPartition(), offset);
            metrics.recordSensor(WorkersMetrics.PROCESSED_OFFSET_METRIC, subpartition, offset);
        });
    }

}
