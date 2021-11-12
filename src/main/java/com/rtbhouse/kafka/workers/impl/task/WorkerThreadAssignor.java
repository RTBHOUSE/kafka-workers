package com.rtbhouse.kafka.workers.impl.task;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Assignor used for assigning {@link WorkerSubpartition} to {@link WorkerThread}.
 */
public interface WorkerThreadAssignor<K, V> {

    /**
     * Determines assignments between subpartitions and worker threads.
     * @param subpartitions subpartitions to be assigned.
     * @param threads worker threads collection.
     * @return subpartitions assigned to each thread.
     */
    Map<WorkerThread<K, V>, List<WorkerSubpartition>> assign(Collection<WorkerSubpartition> subpartitions,
            List<WorkerThread<K, V>> threads);
}