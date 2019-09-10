package com.rtbhouse.kafka.workers.impl.offsets;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.rtbhouse.kafka.workers.impl.range.ClosedRange.range;
import static com.rtbhouse.kafka.workers.impl.range.ClosedRange.singleElementRange;
import static java.util.Comparator.comparing;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.rtbhouse.kafka.workers.impl.collection.CollectionUtils;
import com.rtbhouse.kafka.workers.impl.collection.EnhancedArrayDeque;
import com.rtbhouse.kafka.workers.impl.range.ClosedRange;

class ConsumedOffsets {
    private final EnhancedArrayDeque<ConsumedOffsetRange> consumedRanges = new EnhancedArrayDeque<>();

    synchronized Optional<Long> getMinExistingElement(ClosedRange range) {
        Optional<ConsumedOffsetRange> prevRange = floor(range);
        if (prevRange.isPresent()) {
            if (range.lowerEndpoint() <= prevRange.get().upperEndpoint()) {
                return Optional.of(range.lowerEndpoint());
            }
        }

        Optional<ConsumedOffsetRange> nextRange = ceiling(range);
        if (nextRange.isPresent()) {
            if (nextRange.get().lowerEndpoint() <= range.upperEndpoint()) {
                return Optional.of(nextRange.get().lowerEndpoint());
            }
        }

        return Optional.empty();
    }

    private Optional<ConsumedOffsetRange> floor(ClosedRange range) {
        return Optional.ofNullable(CollectionUtils.floorBinarySearch(consumedRanges, range,
                comparing(ClosedRange::lowerEndpoint)));
    }

    private Optional<ConsumedOffsetRange> ceiling(ClosedRange range) {
        return Optional.ofNullable(CollectionUtils.ceilingBinarySearch(consumedRanges, range,
                comparing(ClosedRange::lowerEndpoint)));
    }

    synchronized void addConsumedRange(ConsumedOffsetRange range) {
        if (!consumedRanges.isEmpty()) {
            ConsumedOffsetRange lastRange = consumedRanges.getLast();
            checkState(range.lowerEndpoint() > lastRange.upperEndpoint(),
                    "condition not met [range.lowerEndpoint() > lastRange.upperEndpoint()]: " +
                            "lastRange [%s], range [%s]", lastRange, range);
            checkState(!range.getConsumedAt().isBefore(lastRange.getConsumedAt()),
                    "condition not met [range.consumedAt >= lastRange.consumedAt]: " +
                            "lastRange [%s], range [%s]", lastRange, range);
        }

        consumedRanges.addLast(range);
    }

    synchronized boolean contains(long offset) {
        return floor(singleElementRange(offset))
                .map(range -> offset <= range.upperEndpoint())
                .orElse(false);
    }

    synchronized Optional<ConsumedOffsetRange> getFirst() {
        try {
            return Optional.of(consumedRanges.getFirst());
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    synchronized int size() {
        return consumedRanges.size();
    }

    synchronized List<ConsumedOffsetRange> getRanges() {
        return ImmutableList.copyOf(consumedRanges);
    }

    synchronized Optional<ClosedRange> removeMaximumHeadRange(ClosedRange processedRange) {
        if (consumedRanges.isEmpty()) {
            return Optional.empty();
        }

        ConsumedOffsetRange firstConsumedRange = consumedRanges.getFirst();
        if (firstConsumedRange.lowerEndpoint() == processedRange.lowerEndpoint()) {
            Optional<Long> maxRemovedOffset = doRemoveElementsLowerOrEqual(processedRange.upperEndpoint());
            return maxRemovedOffset
                    .map(offset -> range(processedRange.lowerEndpoint(), offset));
        } else {
            return Optional.empty();
        }
    }

    private Optional<Long> doRemoveElementsLowerOrEqual(long maxOffset) {
        Iterator<ConsumedOffsetRange> it = consumedRanges.iterator();
        Long maxRemoved = null;
        ConsumedOffsetRange range = null;
        while (it.hasNext()) {
            range = it.next();
            if (range.upperEndpoint() <= maxOffset) {
                it.remove();
                maxRemoved = range.upperEndpoint();
            } else {
                break;
            }
        }

        if (range != null && range.lowerEndpoint() <= maxOffset && maxOffset < range.upperEndpoint()) {
            it.remove();
            consumedRanges.addFirst(range.shrinkFromLeft(maxOffset + 1));
            maxRemoved = maxOffset;
        }

        return Optional.ofNullable(maxRemoved);
    }

    synchronized void removeElementsLowerOrEqual(long maxOffset) {
        doRemoveElementsLowerOrEqual(maxOffset);
    }

    static class ConsumedOffsetRange implements ClosedRange {
        private final ClosedRange range;
        private final Instant consumedAt;

        ConsumedOffsetRange(ClosedRange range, Instant consumedAt) {
            this.range = range;
            this.consumedAt = consumedAt;
        }

        Instant getConsumedAt() {
            return consumedAt;
        }

        @Override
        public String toString() {
            return "ConsumedOffsetRange{" +
                    "range=" + range +
                    ", consumedAt=" + consumedAt +
                    '}';
        }

        @Override
        public long lowerEndpoint() {
            return range.lowerEndpoint();
        }

        @Override
        public long upperEndpoint() {
            return range.upperEndpoint();
        }

        public long size() {
            return range.size();
        }

        ConsumedOffsetRange shrinkFromLeft(long newLowerEndpoint) {
            checkArgument(newLowerEndpoint >= range.lowerEndpoint());
            return new ConsumedOffsetRange(range(newLowerEndpoint, range.upperEndpoint()), consumedAt);
        }

        @Override
        public Iterator<Long> iterator() {
            return range.iterator();
        }
    }
}
