package com.rtbhouse.kafka.workers.impl.record.action;

import static com.rtbhouse.kafka.workers.api.WorkersConfig.RECORD_PROCESSING_FALLBACK_TOPIC;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.RecordProcessingActionException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class SendToFallbackTopicAction<K, V> extends BaseAction<K, V> implements RecordProcessingOnFailureAction<K, V> {

    private final KafkaProducer<K, V> kafkaProducer;

    private final String fallbackTopic;

    public SendToFallbackTopicAction(WorkersConfig config, WorkersMetrics metrics, OffsetsState offsetsState,
                                     KafkaProducer<K, V> kafkaProducer) {
        super(config, metrics, offsetsState);
        this.fallbackTopic = fallbackTopic();
        this.kafkaProducer = kafkaProducer;
    }

    private String fallbackTopic() {
        return config.getString(RECORD_PROCESSING_FALLBACK_TOPIC);
    }

    @Override
    public void handleFailure(WorkerRecord<K, V> record, Exception exception) {
        try {
            var producerRecord = new ProducerRecord<>(fallbackTopic, null,
                    record.key(), record.value(), record.headers());
            //TODO: send async
            kafkaProducer.send(producerRecord).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RecordProcessingActionException(e);
        }
        markRecordProcessed(record);
    }
}
