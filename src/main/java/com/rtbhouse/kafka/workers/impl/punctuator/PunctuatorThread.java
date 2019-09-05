package com.rtbhouse.kafka.workers.impl.punctuator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.impl.AbstractWorkersThread;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

public class PunctuatorThread<K, V> extends AbstractWorkersThread {

    private static final Logger logger = LoggerFactory.getLogger(PunctuatorThread.class);

    private final List<WorkerThread<K, V>> threads;

    public PunctuatorThread(
            WorkersConfig config,
            WorkersMetrics metrics,
            KafkaWorkersImpl<K, V> workers,
            List<WorkerThread<K, V>> threads) {
        super("punctuator-thread", config, metrics, workers);
        this.threads = threads;
    }

    @Override
    public void init() {
    }

    @Override
    public void process() throws InterruptedException {
        for (WorkerThread<K, V> thread : threads) {
            if (thread.shouldPunctuateNow()) {
                thread.notifyThread();
            }
        }
        Thread.sleep(config.getLong(WorkersConfig.PUNCTUATOR_INTERVAL_MS));
    }

    @Override
    public void close() {
    }

}
