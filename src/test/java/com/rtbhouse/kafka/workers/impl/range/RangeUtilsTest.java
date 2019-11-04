package com.rtbhouse.kafka.workers.impl.range;

import static com.rtbhouse.kafka.workers.impl.range.ClosedRange.range;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class RangeUtilsTest {

    private Object[] parametersForTestRangesFromLongs() {
        return new Object[] {
                new Object[] {List.of(), new ClosedRange[] {}},
                new Object[] {List.of(1L), new ClosedRange[] {range(1L, 1L)}},
                new Object[] {List.of(1L, 3L, 5L), new ClosedRange[] {range(1L, 1L), range(3L, 3L), range(5L, 5L)}},
                new Object[] {List.of(1L, 2L, 5L), new ClosedRange[] {range(1L, 2L), range(5L, 5L)}},
                new Object[] {List.of(1L, 4L, 5L), new ClosedRange[] {range(1L, 1L), range(4L, 5L)}},
                new Object[] {List.of(1L, 2L, 3L), new ClosedRange[] {range(1L, 3L)}},
                new Object[] {List.of(1L, 2L, 3L, 5L), new ClosedRange[] {range(1L, 3L), range(5L, 5L)}},
                new Object[] {List.of(1L, 2L, 3L, 5L, 6L, 7L), new ClosedRange[] {range(1L, 3L), range(5L, 7L)}},
                new Object[] {List.of(10L, 11L, 12L, 20L, 30L, 31L), new ClosedRange[] {range(10L, 12L), range(20L, 20L), range(30L, 31L)}}
        };
    }

    @Test
    @Parameters
    public void testRangesFromLongs(List<Long> longs, ClosedRange[] expectedRanges) {
        //when
        List<ClosedRange> ranges = RangeUtils.rangesFromLongs(longs);

        //then
        assertThat(ranges).containsExactly(expectedRanges);
    }
}
