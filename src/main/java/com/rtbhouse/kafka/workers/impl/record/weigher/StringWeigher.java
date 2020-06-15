package com.rtbhouse.kafka.workers.impl.record.weigher;

import com.rtbhouse.kafka.workers.api.record.Weigher;

public class StringWeigher implements Weigher<String> {

    public static final StringWeigher INSTANCE = new StringWeigher();

    private static final int STRING_SHALLOW_SIZE = WeigherHelpers.shallowSize(String.class, "(String)");

    private StringWeigher() {
    }

    @Override
    public long weight(String s) {
        // we assume LATIN1 coder, thus s.length() == s.value.length
        return STRING_SHALLOW_SIZE + s.length();
    }
}
