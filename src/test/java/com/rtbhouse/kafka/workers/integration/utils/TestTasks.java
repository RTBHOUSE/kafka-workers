package com.rtbhouse.kafka.workers.integration.utils;

import java.util.concurrent.TimeUnit;

import com.rtbhouse.kafka.workers.api.task.WorkerTask;

public class TestTasks {

    public static <K, V> WorkerTask<K, V> createNoopTask() {
        return (record, observer) -> {
            observer.onSuccess();
        };
    }

    public static <K, V> WorkerTask<K, V> createInterruptibleTask() {
        return (record, observer) -> {
            try {
                TimeUnit.DAYS.sleep(1);
            } catch (InterruptedException e) {
                observer.onFailure(e);
            }
        };
    }

    public static <K, V> WorkerTask<K, V> createNotInterruptibleTask() {
        return (record, observer) -> {
            while (true) {
            }
        };
    }
}
