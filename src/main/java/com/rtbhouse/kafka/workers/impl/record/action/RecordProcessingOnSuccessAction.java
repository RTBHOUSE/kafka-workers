package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

public interface RecordProcessingOnSuccessAction<K, V> {

    void handleSuccess(WorkerRecord<K, V> record);
}
