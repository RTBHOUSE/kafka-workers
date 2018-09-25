package com.rtbhouse.kafka.workers.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownListenerThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ShutdownListenerThread.class);

    private final KafkaWorkersImpl<?, ?> workers;

    private volatile boolean shutdown = false;

    public ShutdownListenerThread(KafkaWorkersImpl<?, ?> workers) {
        super("shutdown-thread");
        this.workers = workers;
    }

    @Override
    public void run() {
        try {
            waitForShutdown();
        } catch (InterruptedException e) {
            logger.error("InterruptedException: {}", e);
        }
        workers.close();
    }

    public synchronized void shutdown() {
        shutdown = true;
        notifyAll();
    }

    private synchronized void waitForShutdown() throws InterruptedException {
        while (!shutdown) {
            logger.info("waiting for being notify to shutdown");
            wait();
        }
    }

}
