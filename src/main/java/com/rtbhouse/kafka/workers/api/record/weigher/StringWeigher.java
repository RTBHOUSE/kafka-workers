package com.rtbhouse.kafka.workers.api.record.weigher;

public class StringWeigher implements Weigher<String> {

    public static final StringWeigher INSTANCE = new StringWeigher();

    public static final int STRING_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(String.class);

    /**
     * @return weight in bytes the given String object takes on the heap (assuming LATIN1 coder)
     */
    @Override
    public long weigh(String s) {
        if (s == null) {
            return 0;
        }
        // we assume LATIN1 coder, thus s.length() == s.value.length
        return STRING_INSTANCE_SIZE
                - ByteArrayWeigher.BYTE_ARRAY_INSTANCE_SIZE
                + ByteArrayWeigher.INSTANCE.weight(s.length());
    }
}
