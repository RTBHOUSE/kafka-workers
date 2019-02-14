package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.record.action.FailureActionName;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.function.Supplier;

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

    public RecordProcessingOnSuccessAction createSuccessAction(WorkerRecord<K, V> workerRecord) {
        return new MarkRecordProcessedAction<>(workerRecord, config, metrics, offsetsState);
    }

    public RecordProcessingOnFailureAction createFailureAction(WorkerRecord<K, V> workerRecord) {
        FailureActionName actionName = config.getFailureActionName();
        RecordProcessingOnFailureAction innerAction;

        switch (actionName) {
            case SHUTDOWN:
                innerAction = new ShutdownWorkerThreadAction<>(workerRecord, config, metrics, offsetsState, workerThread);
                break;
            case SKIP:
                innerAction = new SkipRecordProcessingAction<>(workerRecord, config, metrics, offsetsState);
                break;
            case FALLBACK_TOPIC:
                KafkaProducer<K, V> kafkaProducer = fallbackTopicKafkaProducerSupplier.get();
                innerAction = new SendToFallbackTopicAction<>(workerRecord, config, metrics, offsetsState, kafkaProducer);
                break;
            default:
                throw new IllegalStateException(String.format("Action name [%s] not supported", actionName.name()));
        }

        return new LoggingFailureAction(actionName, innerAction);
    }

}
