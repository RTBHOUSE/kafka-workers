package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

public interface RecordRetainingAction<K, V> {

    WorkerRecord<K, V> getWorkerRecord();

}
