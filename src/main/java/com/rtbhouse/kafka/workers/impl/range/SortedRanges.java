package com.rtbhouse.kafka.workers.impl.range;

import static com.google.common.base.Preconditions.checkState;
import static com.rtbhouse.kafka.workers.impl.range.ClosedRange.singleElementRange;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeSet;

public class SortedRanges {

    private NavigableSet<ClosedRange> ranges = new TreeSet<>(Comparator.comparingLong(ClosedRange::lowerEndpoint));

    public synchronized void addRange(ClosedRange range) {
        ClosedRange prevRange = ranges.floor(range);
        ClosedRange nextRange = ranges.ceiling(range);

        if (prevRange != null) {
            checkState(prevRange.upperEndpoint() < range.lowerEndpoint(),
                    "condition not met [prevRange.upperEndpoint() < range.lowerEndpoint()]: %s, %s",
                    prevRange, range);
        }
        if (nextRange != null) {
            checkState(range.upperEndpoint() < nextRange.lowerEndpoint(),
                    "condition not met [range.upperEndpoint() < nextRange.lowerEndpoint()]: %s, %s",
                    range, nextRange);
        }

        if (prevRange != null && touchingRanges(prevRange, range)) {
            if (nextRange != null && touchingRanges(range, nextRange)) {
                // the given range joins prev and next ranges
                ranges.remove(prevRange);
                ranges.remove(nextRange);
                ranges.add(ClosedRange.range(prevRange.lowerEndpoint(), nextRange.upperEndpoint()));
            } else {
                ranges.remove(prevRange);
                ranges.add(ClosedRange.range(prevRange.lowerEndpoint(), range.upperEndpoint()));
            }
        } else {
            if (nextRange != null && touchingRanges(range, nextRange)) {
                ranges.remove(nextRange);
                ranges.add(ClosedRange.range(range.lowerEndpoint(), nextRange.upperEndpoint()));
            } else {
                ranges.add(range);
            }
        }
    }

    public synchronized void addSingleElement(long element) {
        addRange(singleElementRange(element));
    }

    public synchronized void removeRange(ClosedRange range) {
        ClosedRange enclosingRange = ranges.floor(range);
        if (enclosingRange == null || !enclosingRange.encloses(range)) {
            throw new NoSuchElementException("SortedRanges does not contain " + range);
        }

        ranges.remove(enclosingRange);
        if (enclosingRange.lowerEndpoint() < range.lowerEndpoint()) {
            ranges.add(ClosedRange.range(enclosingRange.lowerEndpoint(), range.lowerEndpoint() - 1));
        }
        if (enclosingRange.upperEndpoint() > range.upperEndpoint()) {
            ranges.add(ClosedRange.range(range.upperEndpoint() + 1, enclosingRange.upperEndpoint()));
        }
    }

    public synchronized void removeSingleElement(long element) {
        removeRange(singleElementRange(element));
    }

    public synchronized void removeElementsLowerOrEqual(long element) {
        if (getFirst().isEmpty()) {
            throw new NoSuchElementException("SortedRanges is empty");
        } else {
            long first = getFirst().get().lowerEndpoint();
            if (first > element) {
                throw new NoSuchElementException(String.format("first [%s] > [%s] element", first, element));
            } else {
                removeRange(ClosedRange.range(first, element));
            }
        }
    }

    private boolean touchingRanges(ClosedRange range1, ClosedRange range2) {
        return range1.upperEndpoint() + 1 == range2.lowerEndpoint();
    }

    public Optional<ClosedRange> getFirst() {
        try {
            return Optional.of(ranges.first());
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }
}
