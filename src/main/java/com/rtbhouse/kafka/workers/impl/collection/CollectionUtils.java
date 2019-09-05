package com.rtbhouse.kafka.workers.impl.collection;

import java.util.Comparator;
import java.util.function.Function;

public class CollectionUtils {

    /**
     * Returns the greatest element in this sorted collection with a key less than or equal to the given key, or null if there is no such element.
     */
    public static <E, T> E floorBinarySearch(RandomAccess<E> sortedElements, T key, Function<E, T> getKey, Comparator<T> comparator) {
        int low = 0;
        int high = sortedElements.size()-1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            E midVal = sortedElements.get(mid);
            T midKey = getKey.apply(midVal);
            int cmp = comparator.compare(midKey, key);

            if (cmp <= 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return low - 1 >= 0 ? sortedElements.get(low - 1) : null;
    }

    /**
     * Returns the least element in this sorted collection with a key greater than or equal to the given key, or null if there is no such element.
     */
    public static <E, T> E ceilingBinarySearch(RandomAccess<E> sortedElements, T key, Function<E, T> getKey, Comparator<T> comparator) {
        int low = 0;
        int high = sortedElements.size()-1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            E midVal = sortedElements.get(mid);
            T midKey = getKey.apply(midVal);
            int cmp = comparator.compare(midKey, key);

            if (cmp < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return high + 1 < sortedElements.size() ? sortedElements.get(high + 1) : null;
    }
}
