package com.rtbhouse.kafka.workers.test.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;

public interface ListShuffler<E> {
    List<E> getShuffled(List<E> input);

    static <E> ListShuffler<E> forward() {
        return input -> input;
    }

    static <E> ListShuffler<E> reversed() {
        return Lists::reverse;
    }

    static <E> ListShuffler<E> random(int seed) {
        return input -> {
            ArrayList<E> copy = new ArrayList<>(input);
            Collections.shuffle(copy, new Random(seed));
            return copy;
        };
    }
}
