package com.rtbhouse.kafka.workers.impl.consumer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.impl.errors.FailedCommitException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;

public class OffsetCommitCallbackImpl implements OffsetCommitCallback {

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommitCallbackImpl.class);

    private final WorkersConfig config;
    private final ConsumerThread<?, ?> consumerThread;
    private final OffsetsState offsetsState;
    private final WorkersMetrics metrics;

    private final AtomicInteger failuresInRow = new AtomicInteger();

    public OffsetCommitCallbackImpl(
            WorkersConfig config,
            ConsumerThread<?, ?> consumerThread,
            OffsetsState offsetsState,
            WorkersMetrics metrics) {
        this.config = config;
        this.consumerThread = consumerThread;
        this.offsetsState = offsetsState;
        this.metrics = metrics;
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            if (exception instanceof RetriableCommitFailedException) {
                final int maxFailuresInRow = config.getInt(WorkersConfig.CONSUMER_MAX_RETRIABLE_FAILURES);
                final int failureNum = failuresInRow.incrementAndGet();
                if (failureNum <= maxFailuresInRow) {
                    logger.warn("retriable commit failed exception: {}, offsets: {}, failureNum: {}/{}",
                            exception, offsets, failureNum, maxFailuresInRow);
                } else {
                    logger.error("retriable commit failed exception: {}, offsets: {}, failureNum: {}/{}",
                            exception, offsets, failureNum, maxFailuresInRow);
                    consumerThread.shutdown(new FailedCommitException(exception));
                }
            } else {
                logger.error("commit failed exception: {}, offsets: {}", exception, offsets);
                consumerThread.shutdown(new FailedCommitException(exception));
            }
        } else {
            logger.debug("commit succeeded, offsets: {}", offsets);
            failuresInRow.set(0);
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition partition = entry.getKey();
                long offset = entry.getValue().offset();
                metrics.recordSensor(WorkersMetrics.COMMITTED_OFFSET_METRIC, partition, offset);
            }
            offsetsState.removeCommitted(offsets);
        }
    }

}
