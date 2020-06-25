package com.rtbhouse.kafka.workers.api.record.weigher;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

/**
 * The main interface for calculating {@link WorkerRecord} weight (size in bytes it takes on the heap).
 * <p>
 * It allows to measure sizes of the input queues. When a size limit for some queue is exceeded then
 * the corresponding kafka partition is paused. The easiest way to implement custom {@link RecordWeigher}
 * is by extending {@link BaseRecordWeigher} class.
 * <p>
 * A specific implementation can by provided through
 * {@value com.rtbhouse.kafka.workers.api.WorkersConfig#RECORD_WEIGHER_CLASS} config parameter
 * ({@link SimpleRecordWeigher} is a default value).
 */
public interface RecordWeigher<K, V> extends Weigher<WorkerRecord<K, V>> {

    /**
     * @return weight in bytes the given record takes on the heap
     */
    @Override
    long weigh(WorkerRecord<K, V> record);
}
