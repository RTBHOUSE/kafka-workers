package com.rtbhouse.kafka.workers.impl.range;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.stream.LongStream;

public class BasicClosedRange implements ClosedRange {
    private final long lowerEndpoint;
    private final long upperEndpoint;

    BasicClosedRange(long lowerEndpoint, long upperEndpoint) {
        checkArgument(lowerEndpoint <= upperEndpoint);
        this.lowerEndpoint = lowerEndpoint;
        this.upperEndpoint = upperEndpoint;
    }

    private BasicClosedRange(Builder builder) {
        this(builder.firstOffset, builder.lastOffset);
    }

    @Override
    public long lowerEndpoint() {
        return lowerEndpoint;
    }

    @Override
    public long upperEndpoint() {
        return upperEndpoint;
    }

    public static Builder builder(long firstOffset) {
        return new Builder(firstOffset);
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
            return new BasicClosedRange(this);
        }
    }
}
