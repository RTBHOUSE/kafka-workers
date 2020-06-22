package com.rtbhouse.kafka.workers.api.record.weigher;

public class ByteArrayWeigher implements Weigher<byte[]> {

    public static final ByteArrayWeigher INSTANCE = new ByteArrayWeigher();

    static final int BYTE_ARRAY_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(byte[].class, "(byte[])");


    private ByteArrayWeigher() {
    }

    @Override
    public long weigh(byte[] byteArray) {
        return weight(byteArray.length);
    }

    public long weight(int length) {
        return WeigherHelpers.paddedSize(BYTE_ARRAY_INSTANCE_SIZE
                + length);
    }
}
