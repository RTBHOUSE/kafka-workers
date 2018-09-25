package com.rtbhouse.kafka.workers.api;

/**
 * A callback interface which could be used to implement custom actions for {@link KafkaWorkers} instance shutdown.
 */
public interface ShutdownCallback {

    /**
     * A callback method which could be implemented to provide asynchronous handling of {@link KafkaWorkers} shutdown.
     * This method will be called when user closes {@link KafkaWorkers} instance by {@link KafkaWorkers#shutdown()} call
     * but also when any uncaught exception is thrown in any of background threads including records processing.
     *
     * @param exception
     *            The exception thrown in any of background threads, or null if the instance is closed gracefully.
     *
     */
    void onShutdown(WorkersException exception);

}
