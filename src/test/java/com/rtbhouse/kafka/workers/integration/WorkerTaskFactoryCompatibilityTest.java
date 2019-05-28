package com.rtbhouse.kafka.workers.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.integration.utils.KafkaServerRule;
import com.rtbhouse.kafka.workers.integration.utils.RequiresKafkaServer;
import com.rtbhouse.kafka.workers.integration.utils.TestProperties;
import com.rtbhouse.kafka.workers.integration.utils.ZookeeperUtils;

@RequiresKafkaServer
public class WorkerTaskFactoryCompatibilityTest {

    private static final String TOPIC = "topic";

    private static final Properties SERVER_PROPERTIES = TestProperties.serverProperties();
    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            StringDeserializer.class, StringDeserializer.class, TOPIC);
    private static final Properties PRODUCER_PROPERTIES = TestProperties.producerProperties(
            StringSerializer.class, StringSerializer.class);

    @Rule
    public KafkaServerRule kafkaServerRule = new KafkaServerRule(SERVER_PROPERTIES);

    private KafkaProducer<String, String> producer;

    @Before
    public void before() throws InterruptedException {
        ZookeeperUtils.createTopics(kafkaServerRule.getZookeeperConnectString(), 1, 1, TOPIC);
        producer = new KafkaProducer<>(PRODUCER_PROPERTIES);
    }

    @Test
    public void shouldCreateTaskWithSubpartition() throws InterruptedException {
        producer.send(new ProducerRecord<>(TOPIC, "test"));

        WorkerTaskFactory<byte[], byte[]> taskFactory = spy(new TestTaskFactory());
        CountDownLatch latch = new CountDownLatch(1);

        KafkaWorkers<byte[], byte[]> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                taskFactory,
                exception -> latch.countDown());

        kafkaWorkers.start();

        assertThat(latch.await(10, TimeUnit.SECONDS)).isFalse();

        // Verify if default implementation provides backwards compatibility.
        verify(taskFactory, atLeastOnce()).createTask(any(), any());
        verify(taskFactory, atLeastOnce()).createTask(any());

        kafkaWorkers.shutdown();
    }

    private static class TestTaskFactory implements WorkerTaskFactory<byte[], byte[]> {

        @Override
        public WorkerTask<byte[], byte[]> createTask(WorkersConfig config) {
            return new TestTask();
        }
    }


    private static class TestTask implements WorkerTask<byte[], byte[]> {
        @Override
        public void process(WorkerRecord<byte[], byte[]> record, RecordStatusObserver observer) {
            observer.onSuccess();
        }
    }
}
