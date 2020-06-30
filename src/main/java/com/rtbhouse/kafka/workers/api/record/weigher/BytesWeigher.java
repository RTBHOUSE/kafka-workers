package com.rtbhouse.kafka.workers.api.record.weigher;

import org.apache.kafka.common.utils.Bytes;

public class BytesWeigher implements Weigher<Bytes> {

    public static final BytesWeigher INSTANCE = new BytesWeigher();

    public static final int BYTES_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(Bytes.class);

    /**
     * @return weight in bytes the given Bytes object takes on the heap
     */
    @Override
    public long weigh(Bytes bytes) {
        if (bytes == null) {
            return 0;
        }
        return BYTES_INSTANCE_SIZE - ByteArrayWeigher.BYTE_ARRAY_INSTANCE_SIZE + ByteArrayWeigher.INSTANCE.weigh(bytes.get());
    }
}
