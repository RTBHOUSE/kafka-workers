package com.rtbhouse.kafka.workers.api.record.weigher;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

/**
 * Returns {@link WorkerRecord#serializedSize()} as record's weight.
 */
public class SimpleRecordWeigher<K, V> implements RecordWeigher<K, V> {

    @Override
    public long weigh(WorkerRecord<K, V> record) {
        return record.serializedSize();
    }
}
