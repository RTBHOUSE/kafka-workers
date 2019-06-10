package com.rtbhouse.kafka.workers.impl.observer;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.rtbhouse.kafka.workers.api.observer.BatchStatusObserver;
import com.rtbhouse.kafka.workers.api.observer.StatusObserver;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.impl.errors.FailedCombineObserversException;
import com.rtbhouse.kafka.workers.impl.task.WorkerTaskImpl;

public class BatchStatusObserverImpl<K, V> extends SubpartitionObserver<K, V> implements BatchStatusObserver {

    private long[] offsets;
    private int size;

    private SubpartitionObserver observer;

    private long offset;

    public StatusObserverImpl(SubpartitionObserver observer, long offset) {
        this.observer = observer;
        this.offset = offset;
    }

    public BatchStatusObserverImpl(WorkerTaskImpl<K, V> task, long offset) {
        super(task);
        this.offsets = new long[] {offset};
        this.size = 1;
    }

    @Override
    public void add(StatusObserver observer) {
        addAll(observer.subpartition(), Stream.of(observer.offset()).collect(Collectors.toSet()));
    }

    @Override
    public void addAll(BatchStatusObserver observer) {
        addAll(observer.subpartition(), observer.offsets());
    }

    public Set<Long> offsets() {
        return Arrays.stream(offsets, 0, size).boxed().collect(Collectors.toSet());
    }

    private void addAll(WorkerSubpartition otherSubpartition, Set<Long> otherOffsets) {
        if (!subpartition().equals(otherSubpartition)) {
            throw new FailedCombineObserversException(
                    "could not combine observers for different subpartitions: " + subpartition() + ", " + otherSubpartition);
        }
        if (offsets.length < size + otherOffsets.size()) {
            long[] newOffsets = new long[2 * Math.max(offsets.length, otherOffsets.size())];
            System.arraycopy(offsets, 0, newOffsets, 0, size);
            offsets = newOffsets;
        }
        otherOffsets.forEach(offset -> offsets[size++] = offset);
    }

}
