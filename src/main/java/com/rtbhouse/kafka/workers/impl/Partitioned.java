package com.rtbhouse.kafka.workers.impl;

import java.util.Collection;

import org.apache.kafka.common.TopicPartition;

public interface Partitioned {

    void register(Collection<TopicPartition> partitions) throws InterruptedException;

    void unregister(Collection<TopicPartition> partitions) throws InterruptedException;

}
