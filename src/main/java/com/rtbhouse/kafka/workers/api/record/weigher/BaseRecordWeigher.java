package com.rtbhouse.kafka.workers.api.record.weigher;

import java.nio.ByteBuffer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

/**
 * This base class can be helpful in implementing a {@link RecordWeigher}.
 * Its {@link BaseRecordWeigher#BaseRecordWeigher(Weigher, Weigher) constructor} takes weighers for keys and values.
 * Using {@link StringWeigher}, {@link ByteArrayWeigher} classes is recommended there.
 */
public class BaseRecordWeigher<K, V> implements RecordWeigher<K, V> {

    private static final int OBJECT_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(Object.class);

    private static final int RECORD_HEADERS_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(RecordHeaders.class);

    private static final int RECORD_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(WorkerRecord.class)
            // subtract fields which size will be added separately
            - OBJECT_INSTANCE_SIZE // record.key
            - OBJECT_INSTANCE_SIZE // record.value
            - StringWeigher.STRING_INSTANCE_SIZE // record.topic
            - WeigherHelpers.estimateInstanceSize(Headers.class); // record.headers

    private static final int RECORD_HEADER_INSTANCE_SIZE = WeigherHelpers.estimateInstanceSize(RecordHeader.class)
            // subtract fields which size will be added separately
            - StringWeigher.STRING_INSTANCE_SIZE // key
            - WeigherHelpers.estimateInstanceSize(ByteBuffer.class) // valueBuffer (null)
            - ByteArrayWeigher.BYTE_ARRAY_INSTANCE_SIZE; // value

    private final Weigher<K> keyWeigher;

    private final Weigher<V> valueWeigher;

    public BaseRecordWeigher(Weigher<K> keyWeigher, Weigher<V> valueWeigher) {
        this.keyWeigher = keyWeigher;
        this.valueWeigher = valueWeigher;
    }

    @Override
    public long weigh(WorkerRecord<K, V> record) {
        return RECORD_INSTANCE_SIZE
                + keyWeigher.weigh(record.key())
                + valueWeigher.weigh(record.value())
                + StringWeigher.INSTANCE.weigh(record.topic())
                + weigh(record.headers());
    }

    private long weigh(Headers headers) {
        long size = RECORD_HEADERS_INSTANCE_SIZE;
        for (Header header : headers) {
            size += RECORD_HEADER_INSTANCE_SIZE
                    + StringWeigher.INSTANCE.weigh(header.key())
                    // calling header.value() here may have impact on memory usage and performance as
                    // it replaces ByteBuffer with byte[] inside RecordHeader
                    + ByteArrayWeigher.INSTANCE.weigh(header.value());
        }
        return size;
    }
}
