package com.rtbhouse.kafka.workers.impl.record;

import com.rtbhouse.kafka.workers.api.record.Weigher;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

public class SimpleRecordWeigher<K, V> implements Weigher<WorkerRecord<K, V>> {

    @Override
    public long weight(WorkerRecord<K, V> record) {
        return record.serializedSize();
    }
}
