package com.rtbhouse.kafka.workers.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.ShutdownCallback;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingFailureException;
import com.rtbhouse.kafka.workers.integration.utils.KafkaServerRule;
import com.rtbhouse.kafka.workers.integration.utils.RequiresKafkaServer;
import com.rtbhouse.kafka.workers.integration.utils.TestProperties;
import com.rtbhouse.kafka.workers.integration.utils.ZookeeperUtils;

@RequiresKafkaServer
public class RecordStatusObserverTest {

    private static final String TOPIC = "topic";

    private static final Properties SERVER_PROPERTIES = TestProperties.serverProperties();
    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            StringDeserializer.class, StringDeserializer.class, TOPIC);
    static {
        WORKERS_PROPERTIES.put(WorkersConfig.CONSUMER_POLL_TIMEOUT_MS, 100L);
        WORKERS_PROPERTIES.put(WorkersConfig.CONSUMER_COMMIT_INTERVAL_MS, 100L);
        WORKERS_PROPERTIES.put(WorkersConfig.CONSUMER_PROCESSING_TIMEOUT_MS, 100L);
        WORKERS_PROPERTIES.put(WorkersConfig.WORKER_SLEEP_MS, 100L);
    }
    private static final Properties PRODUCER_PROPERTIES = TestProperties.producerProperties(
            StringSerializer.class, StringSerializer.class);

    @Rule
    public KafkaServerRule kafkaServerRule = new KafkaServerRule(SERVER_PROPERTIES);

    private KafkaProducer<String, String> producer;

    @Before
    public void before() throws Exception {
        ZookeeperUtils.createTopics(kafkaServerRule.getZookeeperConnectString(), 1, 1, TOPIC);
        producer = new KafkaProducer<>(PRODUCER_PROPERTIES);
    }

    @After
    public void after() {
        producer.close();
    }

    @Test
    public void shouldShutdownKafkaWorkers() throws Exception {
        // given
        producer.send(new ProducerRecord<>(TOPIC, "test")).get();
        WORKERS_PROPERTIES.put(WorkersConfig.WORKER_PROCESSING_GUARANTEE, "at_least_once");

        AtomicReference<Throwable> exceptionToCheck = new AtomicReference<>(null);
        CountDownLatch latch = new CountDownLatch(1);
        KafkaWorkers<String, String> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TestTaskFactory(),
                new ShutdownCallback() {
                    @Override
                    public void onShutdown(WorkersException exception) {
                        exceptionToCheck.set(exception.getCause());
                        latch.countDown();
                    }
                });

        // when
        kafkaWorkers.start();

        //then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(exceptionToCheck.get()).isNotNull();
        assertThat(exceptionToCheck.get()).isExactlyInstanceOf(ProcessingFailureException.class);
        assertThat(exceptionToCheck.get().getCause()).isExactlyInstanceOf(RuntimeException.class);
        assertThat(exceptionToCheck.get().getCause().getMessage()).isEqualToIgnoringCase("Test");

        kafkaWorkers.shutdown();
    }

    @Test
    public void shouldSkipRecord() throws Exception {
        // given
        producer.send(new ProducerRecord<>(TOPIC, "test")).get();
        WORKERS_PROPERTIES.put(WorkersConfig.WORKER_PROCESSING_GUARANTEE, "none");

        CountDownLatch latch = new CountDownLatch(1);
        KafkaWorkers<String, String> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TestTaskFactory(),
                new ShutdownCallback() {
                    @Override
                    public void onShutdown(WorkersException exception) {
                        latch.countDown();
                    }
                });

        // when
        kafkaWorkers.start();

        //then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isFalse();

        kafkaWorkers.shutdown();
    }

    private static class TestTask implements WorkerTask<String, String> {

        @Override
        public void process(WorkerRecord<String, String> record, RecordStatusObserver observer) {
            observer.onFailure(new RuntimeException("Test"));
        }

    }

    private static class TestTaskFactory implements WorkerTaskFactory<String, String> {

        @Override
        public TestTask createTask(WorkersConfig config) {
            return new TestTask();
        }

    }

}
