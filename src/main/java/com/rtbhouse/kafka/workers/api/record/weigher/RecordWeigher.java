package com.rtbhouse.kafka.workers.api.record.weigher;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

public interface RecordWeigher<K, V> extends Weigher<WorkerRecord<K, V>> {
}
