package com.rtbhouse.kafka.workers.api.record.weigher;

public class StringWeigher implements Weigher<String> {

    public static final StringWeigher INSTANCE = new StringWeigher();

    static final int STRING_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(String.class);

    private StringWeigher() {
    }

    @Override
    public long weigh(String s) {
        // we assume LATIN1 coder, thus s.length() == s.value.length
        return STRING_INSTANCE_SIZE
                - ByteArrayWeigher.BYTE_ARRAY_INSTANCE_SIZE
                + ByteArrayWeigher.INSTANCE.weight(s.length());
    }
}
