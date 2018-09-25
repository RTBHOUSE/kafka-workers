package com.rtbhouse.kafka.workers.api.task;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;

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

}
