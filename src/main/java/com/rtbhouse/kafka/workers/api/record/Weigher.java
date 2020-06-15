package com.rtbhouse.kafka.workers.api.record;

public interface Weigher<T> {

    long weight(T object);
}
