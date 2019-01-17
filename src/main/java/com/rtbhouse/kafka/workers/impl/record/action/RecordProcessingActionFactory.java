package com.rtbhouse.kafka.workers.impl.record.action;

import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.action.FailureActionName;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

public class RecordProcessingActionFactory<K, V> {

    private final WorkersConfig config;
    private final WorkersMetrics metrics;
    private final OffsetsState offsetsState;
    private final WorkerThread<K, V> workerThread;
    private final Supplier<KafkaProducer<K, V>> fallbackTopicKafkaProducerSupplier;

    public RecordProcessingActionFactory(WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState, WorkerThread<K, V> workerThread) {
        this.config = config;
        this.metrics = metrics;
        this.offsetsState = offsetsState;
        this.workerThread = workerThread;
        this.fallbackTopicKafkaProducerSupplier = FallbackTopicKafkaProducerSupplier.getInstance(config);
    }

    public RecordProcessingOnSuccessAction<K, V> createSuccessAction() {
        return new MarkRecordProcessed<>(config, metrics, offsetsState);
    }

    public RecordProcessingOnFailureAction<K, V> createFailureAction() {
        FailureActionName actionName = config.getFailureActionName();
        RecordProcessingOnFailureAction<K, V> innerAction;

        switch (actionName) {
            case SHUTDOWN:
                innerAction = new ShutdownWorkerThread<>(config, metrics, offsetsState, workerThread);
                break;
            case SKIP:
                innerAction = new SkipRecordProcessing<>(config, metrics, offsetsState);
                break;
            case FALLBACK_TOPIC:
                KafkaProducer<K, V> kafkaProducer = fallbackTopicKafkaProducerSupplier.get();
                innerAction = new SendToFallbackTopic<>(config, metrics, offsetsState, kafkaProducer);
                break;
            default:
                throw new IllegalStateException(String.format("Action name [%s] not supported", actionName.name()));
        }

        return new LoggingFailureAction<>(config, metrics, offsetsState, actionName, innerAction);
    }

}
