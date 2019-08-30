package com.rtbhouse.kafka.workers.impl.range;

import static com.google.common.base.Preconditions.checkState;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class RangeUtils {

    public static List<ClosedRange> rangesFromLongs(Iterable<Long> longs) {
        ImmutableList.Builder<ClosedRange> listBuilder = ImmutableList.builder();
        ClosedRange.Builder rangeBuilder = null;

        for (Long offset : longs) {
            if (rangeBuilder == null) {
                rangeBuilder = ClosedRange.builder(offset);
                continue;
            }

            checkState(offset > rangeBuilder.getLastOffset());

            if (offset == rangeBuilder.getLastOffset() + 1) {
                rangeBuilder.extend(offset);
            } else {
                listBuilder.add(rangeBuilder.build());
                rangeBuilder = ClosedRange.builder(offset);
            }
        }

        if (rangeBuilder != null) {
            listBuilder.add(rangeBuilder.build());
        }

        return listBuilder.build();
    }
}
