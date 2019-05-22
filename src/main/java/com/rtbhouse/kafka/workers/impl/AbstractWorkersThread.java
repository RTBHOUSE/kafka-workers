package com.rtbhouse.kafka.workers.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;

public abstract class AbstractWorkersThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(AbstractWorkersThread.class);

    private final String name;
    protected final WorkersConfig config;
    protected final WorkersMetrics metrics;
    protected final KafkaWorkersImpl<?, ?> workers;

    protected volatile boolean shutdown = false;
    private volatile WorkersException exception;

    protected volatile boolean stopped = false;

    public AbstractWorkersThread(String name, WorkersConfig config, WorkersMetrics metrics, KafkaWorkersImpl<?, ?> workers) {
        super(name);
        this.name = name;
        this.config = config;
        this.metrics = metrics;
        this.workers = workers;
    }

    public abstract void init();

    public abstract void process() throws InterruptedException;

    public abstract void close();

    // used to shutdown current thread internally because of failure (see RecordStatusObserver)
    public void shutdown(WorkersException exception) {
        if (exception != null) {
            this.exception = exception;
        }
        this.shutdown = true;
    }

    // used to shutdown current thread by KafkaWorkers in case of shutdown caused by other thread or app shutdown
    public final void shutdown() {
        shutdown(null);
    }

    @Override
    public final void run() {
        logger.info("thread {} started", name);
        Thread.currentThread().setName(name);
        try {
            init();
            while (!shutdown) {
                process();
            }
            if (exception != null) {
                throw new WorkersException(exception);
            }
        } catch (Throwable e) {
            logger.error("Thread shuts down KafkaWorkers", e);
            workers.shutdown(wrapIfNeeded(e));
        } finally {
            stopped = true;
        }
        logger.info("thread {} stopped", name);
    }

    private WorkersException wrapIfNeeded(Throwable e) {
        return (e instanceof WorkersException) ? (WorkersException) e : new WorkersException(e);
    }

}
