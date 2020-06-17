package com.rtbhouse.kafka.workers.api.record.weigher;

public class ByteArrayWeigher implements Weigher<byte[]> {

    public static final ByteArrayWeigher INSTANCE = new ByteArrayWeigher();

    private ByteArrayWeigher() {
    }

    @Override
    public long weight(byte[] byteArray) {
        return byteArray.length;
    }
}
