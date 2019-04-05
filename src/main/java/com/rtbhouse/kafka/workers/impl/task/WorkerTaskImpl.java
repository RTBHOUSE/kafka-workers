package com.rtbhouse.kafka.workers.impl.task;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;

public class WorkerTaskImpl<K, V> implements WorkerTask<K, V> {

    // user-defined task to process
    private final WorkerTask<K, V> task;

    private final WorkersMetrics metrics;

    // subpartition which is associated with given task in one-to-one relation
    private WorkerSubpartition subpartition;

    private WorkerThread<K, V> thread;

    public WorkerTaskImpl(WorkerTask<K, V> task, WorkersMetrics metrics) {
        this.task = task;
        this.metrics = metrics;
    }

    @Override
    public void init(WorkerSubpartition subpartition, WorkersConfig config) {
        this.subpartition = subpartition;
        metrics.addWorkerThreadSubpartitionMetrics(subpartition);
        task.init(subpartition, config);
    }

    @Override
    public boolean accept(WorkerRecord<K, V> record) {
        metrics.recordSensor(WorkersMetrics.ACCEPTING_OFFSET_METRIC, subpartition, record.offset());
        boolean accepted = task.accept(record);
        if (accepted) {
            metrics.recordSensor(WorkersMetrics.ACCEPTED_OFFSET_METRIC, subpartition, record.offset());
        }
        return accepted;
    }

    @Override
    public void process(WorkerRecord<K, V> record, RecordStatusObserver observer) {
        metrics.recordSensor(WorkersMetrics.PROCESSING_OFFSET_METRIC, subpartition, record.offset());
        try {
            task.process(record, observer);
        } catch (Exception e) {
            observer.onFailure(e);
        }
    }

    @Override
    public void punctuate(long punctuateTime) {
        task.punctuate(punctuateTime);
    }

    @Override
    public void close() {
        task.close();
        metrics.removeWorkerThreadSubpartitionMetrics(subpartition);
    }

    public WorkerSubpartition subpartition() {
        return subpartition;
    }

    public void setThread(WorkerThread<K, V> thread) {
        this.thread = thread;
    }

    public void notifyTask() {
        if (thread != null) {
            thread.notifyThread();
        }
    }

}
