package com.rtbhouse.kafka.workers.api.task;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;

/**
 * This interface represents a factory for user-defined {@link WorkerTask}s.
 */
public interface WorkerTaskFactory<K, V> {

    /**
     * Provides new instances of {@link WorkerTask}s.
     *
     * @param config
     *            {@link KafkaWorkers} configuration
     *
     * @return user-defined {@link WorkerTask}
     */
    WorkerTask<K, V> createTask(WorkersConfig config);

    /**
     * Provides new instances of {@link WorkerTask}s.
     * <p></p>
     * May be used to eager initialization of the task. It extends (and keeps) semantics of
     * {@link WorkerTask#init(WorkerSubpartition, WorkersConfig)} and gives more flexibility to a programmer.
     *
     * @param config
     *            {@link KafkaWorkers} configuration
     *
     * @param subpartition
     *            {@link WorkerSubpartition} source <code>subpartition</code> of records
     *
     * @return user-defined {@link WorkerTask}
     */
    default WorkerTask<K, V> createTask(WorkersConfig config, @SuppressWarnings("unused") WorkerSubpartition subpartition) {
        return createTask(config);
    }

}
