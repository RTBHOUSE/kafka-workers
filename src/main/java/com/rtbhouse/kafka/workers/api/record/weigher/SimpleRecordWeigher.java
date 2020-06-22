package com.rtbhouse.kafka.workers.api.record.weigher;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

public class SimpleRecordWeigher<K, V> implements RecordWeigher<K, V> {

    @Override
    public long weigh(WorkerRecord<K, V> record) {
        return record.serializedSize();
    }
}
