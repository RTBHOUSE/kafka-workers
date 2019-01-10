package com.rtbhouse.kafka.workers.api.record;

public interface RecordProcessingOnFailureAction<K, V> {

    enum FailureActionName {
        SHUTDOWN,
        SKIP,
        FALLBACK_TOPIC;

        public static FailureActionName of(String name) {
            return valueOf(name.toUpperCase());
        }
    }

    void handleFailure(WorkerRecord<K, V> record, Exception exception);
}
