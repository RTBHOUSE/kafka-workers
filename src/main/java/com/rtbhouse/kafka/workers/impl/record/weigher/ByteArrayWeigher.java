package com.rtbhouse.kafka.workers.impl.record.weigher;

import com.rtbhouse.kafka.workers.api.record.Weigher;

public class ByteArrayWeigher implements Weigher<byte[]> {

    public static final ByteArrayWeigher INSTANCE = new ByteArrayWeigher();

    private ByteArrayWeigher() {
    }

    @Override
    public long weight(byte[] byteArray) {
        return byteArray.length;
    }
}
