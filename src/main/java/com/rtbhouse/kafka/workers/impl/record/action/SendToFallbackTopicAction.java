package com.rtbhouse.kafka.workers.impl.record.action;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.RecordProcessingActionException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

import static com.rtbhouse.kafka.workers.api.WorkersConfig.RECORD_PROCESSING_FALLBACK_TOPIC;

public class SendToFallbackTopicAction<K, V> extends BaseAction<K, V> implements RecordRetainingAction<K, V>, RecordProcessingOnFailureAction {

    private final WorkerRecord<K, V> workerRecord;
    private final KafkaProducer<K, V> kafkaProducer;
    private final String fallbackTopic;

    public SendToFallbackTopicAction(WorkerRecord<K, V> workerRecord, WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState,
                                     KafkaProducer<K, V> kafkaProducer) {
        super(workerRecord, config, metrics, offsetsState);
        this.workerRecord = workerRecord;
        this.fallbackTopic = fallbackTopic();
        this.kafkaProducer = kafkaProducer;
    }

    private String fallbackTopic() {
        return config.getString(RECORD_PROCESSING_FALLBACK_TOPIC);
    }

    @Override
    public void handleFailure(Exception exception) {
        try {
            var producerRecord = new ProducerRecord<>(fallbackTopic, null,
                    workerRecord.key(), workerRecord.value(), workerRecord.headers());
            //TODO: send async
            kafkaProducer.send(producerRecord).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RecordProcessingActionException(e);
        }
        markRecordProcessed();
    }

    @Override
    public WorkerRecord<K, V> getWorkerRecord() {
        return workerRecord;
    }
}
