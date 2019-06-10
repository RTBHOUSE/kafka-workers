package com.rtbhouse.kafka.workers.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.observer.StatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.integration.utils.KafkaServerRule;
import com.rtbhouse.kafka.workers.integration.utils.RequiresKafkaServer;
import com.rtbhouse.kafka.workers.integration.utils.TestProperties;
import com.rtbhouse.kafka.workers.integration.utils.ZookeeperUtils;

@RequiresKafkaServer
public class PunctuateTest {

    private static final Logger logger = LoggerFactory.getLogger(PunctuateTest.class);

    private static final String TOPIC = "topic";
    private static final int RECORDS_COUNT = 100;

    private static final Properties SERVER_PROPERTIES = TestProperties.serverProperties();

    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            StringDeserializer.class, StringDeserializer.class, TOPIC);
    static {
        WORKERS_PROPERTIES.put(WorkersConfig.PUNCTUATOR_INTERVAL_MS, 100L);
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
    public void after() throws IOException {
        producer.close();
    }

    @Test
    public void shouldRunPunctuate() throws Exception {

        // given
        for (int i = 0; i < RECORDS_COUNT; i++) {
            producer.send(new ProducerRecord<>(TOPIC, 0, null, "key_" + i, "value_" + i));
        }

        CountDownLatch latch = new CountDownLatch(5);

        KafkaWorkers<String, String> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TestTaskFactory(latch));

        // when
        kafkaWorkers.start();

        // then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

    }

    private static class TestTask implements WorkerTask<String, String> {

        private CountDownLatch latch;

        public TestTask(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void process(WorkerRecord<String, String> record, StatusObserver observer) {
            observer.onSuccess();
        }

        @Override
        public void punctuate(long punctuateTime) {
            logger.info("punctuate(punctuateTime: {})", punctuateTime);
            latch.countDown();
        }
    }

    private static class TestTaskFactory implements WorkerTaskFactory<String, String> {

        private CountDownLatch latch;

        public TestTaskFactory(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public TestTask createTask(WorkersConfig config) {
            return new TestTask(latch);
        }

    }

}
