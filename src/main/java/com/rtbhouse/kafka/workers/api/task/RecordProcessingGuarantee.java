package com.rtbhouse.kafka.workers.api.task;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.observer.StatusObserver;

/**
 * Used to describe record processing guarantee through behaviour of {@link StatusObserver} implementation.
 *
 */
public enum RecordProcessingGuarantee {
    /**
     * No record processing guarantee, potential failures don't cause message retransmission
     */
    NONE,

    /**
     * At least once guarantee, potential failures cause {@link KafkaWorkers#shutdown()} and message retransmission
     * upon restart
     */
    AT_LEAST_ONCE;


    public static RecordProcessingGuarantee fromString(String string) {
        if (string != null) {
            return RecordProcessingGuarantee.valueOf(string.toUpperCase());
        }
        throw new IllegalArgumentException();
    }
}
