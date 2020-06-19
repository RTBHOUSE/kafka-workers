package com.rtbhouse.kafka.workers.api.record.weigher;

public class StringWeigher implements Weigher<String> {

    public static final StringWeigher INSTANCE = new StringWeigher();

    private static final int STRING_SHALLOW_SIZE = WeigherHelpers.estimateInstanceSize(String.class, "(String)");

    private StringWeigher() {
    }

    @Override
    public long weight(String s) {
        // we assume LATIN1 coder, thus s.length() == s.value.length
        return STRING_SHALLOW_SIZE + s.length();
    }
}
