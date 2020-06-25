package com.rtbhouse.kafka.workers.api.record.weigher;

public interface Weigher<T> {

    /**
     * @return weight (in bytes) for the given object
     */
    long weigh(T object);
}
