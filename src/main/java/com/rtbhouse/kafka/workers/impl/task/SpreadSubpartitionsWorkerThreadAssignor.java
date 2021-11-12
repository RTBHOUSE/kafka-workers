package com.rtbhouse.kafka.workers.impl.task;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpreadSubpartitionsWorkerThreadAssignor<K, V> implements WorkerThreadAssignor<K, V> {

    private static final Comparator<WorkerSubpartition> COMPARATOR = Comparator.comparing(WorkerSubpartition::topic)
            .thenComparingInt(WorkerSubpartition::partition)
            .thenComparingInt(WorkerSubpartition::subpartition);

    @Override
    public Map<WorkerThread<K, V>, List<WorkerSubpartition>> assign(Collection<WorkerSubpartition> subpartitions,
            List<WorkerThread<K, V>> threads) {
        final List<WorkerSubpartition> sortedSubpartitions = new ArrayList<>(subpartitions);
        sortedSubpartitions.sort(COMPARATOR);
        final Map<WorkerThread<K, V>, List<WorkerSubpartition>> assignment = new HashMap<>();
        int index = 0;
        for (WorkerSubpartition workerSubpartition : sortedSubpartitions) {
            final WorkerThread<K, V> workerThread = threads.get(index);
            assignment.compute(workerThread, (thread, assignments) -> {
                if (assignments == null) {
                    final List<WorkerSubpartition> list = new ArrayList<>();
                    list.add(workerSubpartition);
                    return list;
                } else {
                    assignments.add(workerSubpartition);
                    return assignments;
                }
            });
            index = (index + 1) % threads.size();
        }
        return assignment;
    }
}