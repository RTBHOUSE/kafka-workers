package com.rtbhouse.kafka.workers.api.record.weigher;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

public class BaseRecordWeigher<K, V> implements RecordWeigher<K, V> {

    private static final int RECORD_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(WorkerRecord.class, "(WorkerRecord)")
            + WeigherHelpers.estimateInstanceSize(RecordHeaders.class, "(RecordHeaders)");

    private static final int RECORD_HEADER_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(RecordHeader.class, "(RecordHeader)");

    private final Weigher<K> keyWeigher;

    private final Weigher<V> valueWeigher;

    public BaseRecordWeigher(Weigher<K> keyWeigher, Weigher<V> valueWeigher) {
        this.keyWeigher = keyWeigher;
        this.valueWeigher = valueWeigher;
    }

    @Override
    public long weight(WorkerRecord<K, V> record) {
        // we don't care about padding
        return RECORD_INSTANCE_SIZE
                //TODO: Add only internal array sizes (in bytes) for key/value
                + keyWeigher.weight(record.key())
                + valueWeigher.weight(record.value())
                + weight(record.headers());
    }

    private long weight(Headers headers) {
        long size = 0;
        for (Header header : headers) {
            size += RECORD_HEADER_INSTANCE_SIZE
                    //TODO: Add only internal array sizes (in bytes) for key/value
                    + StringWeigher.INSTANCE.weight(header.key())
                    + ByteArrayWeigher.INSTANCE.weight(header.value());
        }
        return size;
    }

    public static void main(String[] args) {
        System.out.println(RECORD_INSTANCE_SIZE);
    }

}
