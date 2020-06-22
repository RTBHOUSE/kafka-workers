package com.rtbhouse.kafka.workers.api.record.weigher;

public interface Weigher<T> {

    long weigh(T object);
}
