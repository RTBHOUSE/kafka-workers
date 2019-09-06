package com.rtbhouse.kafka.workers.impl.range;

public interface ClosedRange extends Iterable<Long> {

    static ClosedRange range(long lowerEndpoint, long upperEndpoint) {
        return new BasicClosedRange(lowerEndpoint, upperEndpoint);
    }

    static ClosedRange singleElementRange(long element) {
        return range(element, element);
    }

    long lowerEndpoint();

    long upperEndpoint();

    default long size() {
        return upperEndpoint() - lowerEndpoint() + 1;
    }

    default boolean contains(long offset) {
        return lowerEndpoint() <= offset && offset <= upperEndpoint();
    }

    default boolean encloses(ClosedRange other) {
        return this.lowerEndpoint() <= other.lowerEndpoint() && other.upperEndpoint() <= this.upperEndpoint();
    }
}
