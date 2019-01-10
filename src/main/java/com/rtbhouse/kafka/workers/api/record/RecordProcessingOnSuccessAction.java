package com.rtbhouse.kafka.workers.api.record;

public interface RecordProcessingOnSuccessAction<K, V> {

    void handleSuccess(WorkerRecord<K, V> record);
}
