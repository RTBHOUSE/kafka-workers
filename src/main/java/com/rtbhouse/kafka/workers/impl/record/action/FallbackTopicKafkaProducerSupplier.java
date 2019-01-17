package com.rtbhouse.kafka.workers.impl.record.action;

import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.google.common.base.Suppliers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;

public class FallbackTopicKafkaProducerSupplier<K, V> implements Supplier<KafkaProducer<K, V>> {

    private static volatile FallbackTopicKafkaProducerSupplier INSTANCE;

    private final Supplier<KafkaProducer<K, V>> supplier;

    public static <K, V> FallbackTopicKafkaProducerSupplier<K, V> getInstance(WorkersConfig config) {
        if (INSTANCE == null) {
            synchronized (FallbackTopicKafkaProducerSupplier.class) {
                INSTANCE = new FallbackTopicKafkaProducerSupplier(config);
            }
        }
        //noinspection unchecked
        return INSTANCE;
    }

    private FallbackTopicKafkaProducerSupplier(WorkersConfig config) {
        this.supplier = Suppliers.memoize(() -> createKafkaProducer(config));
    }

    private KafkaProducer<K, V> createKafkaProducer(WorkersConfig config) {
        return new KafkaProducer<>(config.getFallbackKafkaProducerConfigs());
    }

    @Override
    public KafkaProducer<K, V> get() {
        return supplier.get();
    }
}
