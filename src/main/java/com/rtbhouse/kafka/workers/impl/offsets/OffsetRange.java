package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class OffsetRange {
    private final long lowerEndpoint;
    private final long upperEndpoint;
    private final OffsetStatus status;

    public static OffsetRange of(long lowerEndpoint, long upperEndpoint, OffsetStatus status) {
        return new OffsetRange(lowerEndpoint, upperEndpoint, status);
    }

    private OffsetRange(long lowerEndpoint, long upperEndpoint, OffsetStatus status) {
        checkArgument(lowerEndpoint <= upperEndpoint);
        this.lowerEndpoint = lowerEndpoint;
        this.upperEndpoint = upperEndpoint;
        this.status = status;
    }

    private OffsetRange(Builder builder) {
        this(builder.firstOffset, builder.lastOffset, builder.status);
    }

    public long lowerEndpoint() {
        return lowerEndpoint;
    }

    public long upperEndpoint() {
        return upperEndpoint;
    }

    public OffsetStatus getStatus() {
        return status;
    }

    public long size() {
        return upperEndpoint - lowerEndpoint + 1;
    }

    public boolean contains(long offset) {
        return lowerEndpoint <= offset && offset <= upperEndpoint;
    }

    public static Builder builder(long firstOffset, OffsetStatus status) {
        return new Builder(firstOffset, status);
    }

    @Override
    public String toString() {
        return "OffsetRange{" +
                "range=[" + lowerEndpoint + ", " + upperEndpoint + "]" +
                ", status=" + status +
                '}';
    }

    public static class Builder {
        private final long firstOffset;
        private long lastOffset;
        private final OffsetStatus status;

        Builder(long firstOffset, OffsetStatus status) {
            this.firstOffset = this.lastOffset = firstOffset;
            this.status = checkNotNull(status);
        }

        public long getLastOffset() {
            return lastOffset;
        }

        public Builder extend(long offset) {
            checkArgument(offset == lastOffset + 1);
            this.lastOffset = offset;
            return this;
        }

        public OffsetRange build() {
            return new OffsetRange(this);
        }
    }
}
