package com.rtbhouse.kafka.workers.impl.consumer;

import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.impl.errors.FailedCommitException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class OffsetCommitCallbackImpl implements OffsetCommitCallback {

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommitCallbackImpl.class);

    private final ConsumerThread<?, ?> consumerThread;
    private final OffsetsState offsetsState;
    private final WorkersMetrics metrics;

    public OffsetCommitCallbackImpl(
            ConsumerThread<?, ?> consumerThread,
            OffsetsState offsetsState,
            WorkersMetrics metrics) {
        this.consumerThread = consumerThread;
        this.offsetsState = offsetsState;
        this.metrics = metrics;
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            logger.error("commit failed, offsets: {} exception: {}", offsets, exception);
            consumerThread.shutdown(new FailedCommitException(exception));
        } else {
            logger.debug("commit succeeded, offsets: {}", offsets);
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition partition = entry.getKey();
                long offset = entry.getValue().offset();
                metrics.recordSensor(WorkersMetrics.COMMITTED_OFFSET_METRIC, partition, offset);
            }
            offsetsState.removeCommitted(offsets);
        }
    }

}
