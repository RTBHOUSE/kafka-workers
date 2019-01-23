package com.rtbhouse.kafka.workers.api.record.action;

public enum FailureActionName {
    SHUTDOWN,
    SKIP,
    FALLBACK_TOPIC;

    public static FailureActionName of(String name) {
        return valueOf(name.toUpperCase());
    }
}
