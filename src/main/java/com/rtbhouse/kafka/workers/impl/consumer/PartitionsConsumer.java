package com.rtbhouse.kafka.workers.impl.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public interface PartitionsConsumer {

    void accept(Collection<TopicPartition> topicPartitions) throws InterruptedException;

}
