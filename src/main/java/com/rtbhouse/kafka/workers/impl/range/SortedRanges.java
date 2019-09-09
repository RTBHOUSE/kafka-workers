package com.rtbhouse.kafka.workers.impl.range;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.rtbhouse.kafka.workers.impl.range.ClosedRange.singleElementRange;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: check which methods need to be synchronized
public class SortedRanges extends AbstractCollection<ClosedRange> {

    private static final Logger logger = LoggerFactory.getLogger(SortedRanges.class);

    private NavigableSet<ClosedRange> ranges = new TreeSet<>(Comparator.comparingLong(ClosedRange::lowerEndpoint));

    public Optional<ClosedRange> getFirst() {
        try {
            return Optional.of(ranges.first());
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    public Optional<ClosedRange> getLast() {
        try {
            return Optional.of(ranges.last());
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    @Override
    public int size() {
        return ranges.size();
    }

    public synchronized Optional<Long> getMinExistingElement(ClosedRange range) {
        checkNotNull(range);
        ClosedRange prevRange = ranges.floor(range);
        if (prevRange != null) {
            if (range.lowerEndpoint() <= prevRange.upperEndpoint()) {
                return Optional.of(range.lowerEndpoint());
            }
        }

        ClosedRange nextRange = ranges.ceiling(range);
        if (nextRange != null) {
            if (nextRange.lowerEndpoint() <= range.upperEndpoint()) {
                return Optional.of(nextRange.lowerEndpoint());
            }
        }

        return Optional.empty();
    }

    @Override
    public boolean contains(Object range) {
        return ranges.contains(range);
    }

    public boolean containsSingleElement(long element) {
        ClosedRange enclosingRange = ranges.floor(singleElementRange(element));
        if (enclosingRange != null && element <= enclosingRange.upperEndpoint()) {
            logger.debug("enclosingRange for [{}] is {} (contains: {})", element, enclosingRange, enclosingRange.contains(element));
        }
        return enclosingRange != null && element <= enclosingRange.upperEndpoint();
    }

    @Override
    public Iterator<ClosedRange> iterator() {
        return ranges.iterator();
    }

    @Override
    public synchronized boolean add(ClosedRange range) {
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
                return ranges.add(ClosedRange.range(prevRange.lowerEndpoint(), nextRange.upperEndpoint()));
            } else {
                ranges.remove(prevRange);
                return ranges.add(ClosedRange.range(prevRange.lowerEndpoint(), range.upperEndpoint()));
            }
        } else {
            if (nextRange != null && touchingRanges(range, nextRange)) {
                ranges.remove(nextRange);
                return ranges.add(ClosedRange.range(range.lowerEndpoint(), nextRange.upperEndpoint()));
            } else {
                return ranges.add(range);
            }
        }
    }

    private boolean touchingRanges(ClosedRange range1, ClosedRange range2) {
        return range1.upperEndpoint() + 1 == range2.lowerEndpoint();
    }

    public synchronized void addSingleElement(long element) {
        add(singleElementRange(element));
    }

    @Override
    public synchronized boolean remove(Object o) {
        ClosedRange range = (ClosedRange) o;
        ClosedRange enclosingRange = ranges.floor(range);
        if (enclosingRange == null || !enclosingRange.encloses(range)) {
            logger.warn("SortedRanges does not contain/enclose {} (cannot remove)", range);
            return false;
        }

        ranges.remove(enclosingRange);
        if (enclosingRange.lowerEndpoint() < range.lowerEndpoint()) {
            ranges.add(ClosedRange.range(enclosingRange.lowerEndpoint(), range.lowerEndpoint() - 1));
        }
        if (enclosingRange.upperEndpoint() > range.upperEndpoint()) {
            ranges.add(ClosedRange.range(range.upperEndpoint() + 1, enclosingRange.upperEndpoint()));
        }

        return true;
    }

    public synchronized boolean removeSingleElement(long element) {
        return remove(singleElementRange(element));
    }

    public synchronized void removeElementsLowerOrEqual(long maxElement) {
        Iterator<ClosedRange> it = ranges.iterator();
        ClosedRange range = null;
        while (it.hasNext()) {
            range = it.next();
            if (range.upperEndpoint() <= maxElement) {
                it.remove();
            } else {
                break;
            }
        }

        if (range != null && range.lowerEndpoint() <= maxElement && maxElement < range.upperEndpoint()) {
            it.remove();
            add(ClosedRange.range(maxElement + 1, range.upperEndpoint()));
        }
    }

    @Override
    public synchronized boolean addAll(Collection<? extends ClosedRange> c) {
        return super.addAll(c);
    }

    @Override
    public synchronized boolean removeAll(Collection<?> c) {
        return super.removeAll(c);
    }

    @Override
    public synchronized boolean retainAll(Collection<?> c) {
        return super.retainAll(c);
    }

    @Override
    public synchronized void clear() {
        super.clear();
    }

    public synchronized Optional<Long> floorElement(long element) {
        return Optional.ofNullable(ranges.floor(singleElementRange(element)))
                .map(floorRange -> Long.min(element, floorRange.upperEndpoint()));
    }
}
