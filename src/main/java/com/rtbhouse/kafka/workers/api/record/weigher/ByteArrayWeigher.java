package com.rtbhouse.kafka.workers.api.record.weigher;

public class ByteArrayWeigher implements Weigher<byte[]> {

    public static final ByteArrayWeigher INSTANCE = new ByteArrayWeigher();

    static final int BYTE_ARRAY_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(byte[].class);


    private ByteArrayWeigher() {
    }

    /**
     * @return weight in bytes the given byte array object takes on the heap
     */
    @Override
    public long weigh(byte[] byteArray) {
        if (byteArray == null) {
            return 0;
        }
        return weight(byteArray.length);
    }

    public long weight(int length) {
        return WeigherHelpers.paddedSize(BYTE_ARRAY_INSTANCE_SIZE
                + length);
    }
}
