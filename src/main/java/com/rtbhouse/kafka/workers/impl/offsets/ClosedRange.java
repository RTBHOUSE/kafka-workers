package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkArgument;

public class ClosedRange {
    private final long lowerEndpoint;
    private final long upperEndpoint;

    public static ClosedRange of(long lowerEndpoint, long upperEndpoint) {
        return new ClosedRange(lowerEndpoint, upperEndpoint);
    }

    private ClosedRange(long lowerEndpoint, long upperEndpoint) {
        checkArgument(lowerEndpoint <= upperEndpoint);
        this.lowerEndpoint = lowerEndpoint;
        this.upperEndpoint = upperEndpoint;
    }

    public long lowerEndpoint() {
        return lowerEndpoint;
    }

    public long upperEndpoint() {
        return upperEndpoint;
    }

    @Override
    public String toString() {
        return "ClosedRange{" + lowerEndpoint + ", " + upperEndpoint + "}";
    }

    public long size() {
        return upperEndpoint - lowerEndpoint + 1;
    }

    public boolean contains(long offset) {
        return lowerEndpoint <= offset && offset <= upperEndpoint;
    }
}
