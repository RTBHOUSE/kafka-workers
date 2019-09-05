package com.rtbhouse.kafka.workers.impl.collection;

public interface RandomAccess<E> {

    E get(int index);

    int size();
}
