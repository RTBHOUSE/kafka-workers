package com.rtbhouse.kafka.workers.impl.collection;

import static com.rtbhouse.kafka.workers.impl.util.FunctionUtils.downcastIdentity;

import java.util.Comparator;
import java.util.function.Function;

public class CollectionUtils {

    /**
     * Returns the greatest element in this sorted collection with a key less than or equal to the given maxKey, or null if there is no such element.
     */
    public static <K, E extends K> E floorBinarySearch(RandomAccess<E> sortedElements, K maxKey, Comparator<K> keyComparator) {
        return floorBinarySearch(sortedElements, maxKey, downcastIdentity(), keyComparator);
    }

    /**
     * Returns the greatest element in this sorted collection with a key less than or equal to the given maxKey, or null if there is no such element.
     */
    public static <K, E> E floorBinarySearch(RandomAccess<E> sortedElements, K maxKey, Function<E, K> getKey, Comparator<K> keyComparator) {
        int low = 0;
        int high = sortedElements.size()-1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            E midVal = sortedElements.get(mid);
            K midKey = getKey.apply(midVal);
            int cmp = keyComparator.compare(midKey, maxKey);

            if (cmp <= 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return low - 1 >= 0 ? sortedElements.get(low - 1) : null;
    }

    /**
     * Returns the least element in this sorted collection with a key greater than or equal to the given minKey, or null if there is no such element.
     */
    public static <K, E extends K> E ceilingBinarySearch(RandomAccess<E> sortedElements, K minKey, Comparator<K> keyComparator) {
        return ceilingBinarySearch(sortedElements, minKey, downcastIdentity(), keyComparator);
    }

    /**
     * Returns the least element in this sorted collection with a key greater than or equal to the given minKey, or null if there is no such element.
     */
    public static <K, E> E ceilingBinarySearch(RandomAccess<E> sortedElements, K minKey, Function<E, K> getKey, Comparator<K> keyComparator) {
        int low = 0;
        int high = sortedElements.size()-1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            E midVal = sortedElements.get(mid);
            K midKey = getKey.apply(midVal);
            int cmp = keyComparator.compare(midKey, minKey);

            if (cmp < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return high + 1 < sortedElements.size() ? sortedElements.get(high + 1) : null;
    }
}
