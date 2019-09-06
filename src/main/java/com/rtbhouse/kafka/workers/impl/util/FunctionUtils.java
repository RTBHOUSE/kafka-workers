package com.rtbhouse.kafka.workers.impl.util;

import java.util.function.Function;

public class FunctionUtils {

    public static <T, S extends T> Function<S, T> downcastIdentity() {
        return o -> o;
    }
}
