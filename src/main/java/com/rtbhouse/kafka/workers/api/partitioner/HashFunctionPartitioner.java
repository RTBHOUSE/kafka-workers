package com.rtbhouse.kafka.workers.api.partitioner;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.nio.charset.Charset;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

public class HashFunctionPartitioner<K, V> implements WorkerPartitioner<K, V> {

    public static <K, V> HashFunctionPartitioner<K, V> newMurmur2Partitioner(int subpartitionCount) {
        return new HashFunctionPartitioner<>(
                HashFunctionPartitioner::murmur2Hash,
                subpartitionCount);
    }

    private static <K, V> long murmur2Hash(ConsumerRecord<K, V> record) {
        String keyWithPartition = record.key().toString() + ":" + record.partition();
        return Utils.toPositive(Utils.murmur2(keyWithPartition.getBytes(Charset.forName("UTF-8"))));
    }

    private final int subpartitionCount;

    private final Function<ConsumerRecord<K, V>, Long> hashFunction;

    public HashFunctionPartitioner(Function<ConsumerRecord<K, V>, Long> hashFunction, int subpartitionCount) {
        this.hashFunction = hashFunction;
        this.subpartitionCount = subpartitionCount;
        verify();
    }

    private void verify() {
        checkNotNull(hashFunction, "hash function cannot be null");
        checkState(subpartitionCount > 0, "subpartitionCount [%s] has to be positive", subpartitionCount);
    }

    @Override
    public int subpartition(ConsumerRecord<K, V> consumerRecord) {
        return (int) (hashFunction.apply(consumerRecord) % subpartitionCount);
    }

    @Override
    public int count(TopicPartition topicPartition) {
        return subpartitionCount;
    }

}
