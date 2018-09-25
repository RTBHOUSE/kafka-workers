package com.rtbhouse.kafka.workers.impl.queues;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

public class RecordsQueue<K, V> extends ConcurrentLinkedQueue<WorkerRecord<K, V>> {

    private static final long serialVersionUID = 1L;

}
