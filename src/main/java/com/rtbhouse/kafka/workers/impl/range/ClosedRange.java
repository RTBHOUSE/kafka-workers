package com.rtbhouse.kafka.workers.impl.range;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.stream.LongStream;

public class ClosedRange implements Iterable<Long> {
    private final long lowerEndpoint;
    private final long upperEndpoint;

    public static ClosedRange range(long lowerEndpoint, long upperEndpoint) {
        return new ClosedRange(lowerEndpoint, upperEndpoint);
    }

    public static ClosedRange singleElementRange(long element) {
        return range(element, element);
    }

    private ClosedRange(long lowerEndpoint, long upperEndpoint) {
        checkArgument(lowerEndpoint <= upperEndpoint);
        this.lowerEndpoint = lowerEndpoint;
        this.upperEndpoint = upperEndpoint;
    }

    private ClosedRange(Builder builder) {
        this(builder.firstOffset, builder.lastOffset);
    }

    public long lowerEndpoint() {
        return lowerEndpoint;
    }

    public long upperEndpoint() {
        return upperEndpoint;
    }

    public long size() {
        return upperEndpoint - lowerEndpoint + 1;
    }

    public boolean contains(long offset) {
        return lowerEndpoint <= offset && offset <= upperEndpoint;
    }

    public static Builder builder(long firstOffset) {
        return new Builder(firstOffset);
    }

    public boolean encloses(ClosedRange other) {
        return this.lowerEndpoint <= other.lowerEndpoint && other.upperEndpoint <= this.upperEndpoint;
    }

    public boolean isConnected(ClosedRange other) {
        return this.lowerEndpoint <= other.upperEndpoint && other.lowerEndpoint <= this.upperEndpoint;
    }

    @Override
    public String toString() {
        return "ClosedRange{" + lowerEndpoint + ", " + upperEndpoint + "}";
    }

    @Override
    public Iterator<Long> iterator() {
        return LongStream.rangeClosed(lowerEndpoint, upperEndpoint).iterator();
    }

    public static class Builder {
        private final long firstOffset;
        private long lastOffset;

        Builder(long firstOffset) {
            this.firstOffset = this.lastOffset = firstOffset;
        }

        public long getLastOffset() {
            return lastOffset;
        }

        public Builder extend(long offset) {
            checkArgument(offset == lastOffset + 1);
            this.lastOffset = offset;
            return this;
        }

        public ClosedRange build() {
            return new ClosedRange(this);
        }
    }
}
