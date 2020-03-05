package com.rtbhouse.kafka.workers.integration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CANNOT_STOP_THREADS;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CLOSED_GRACEFULLY;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CLOSED_NOT_GRACEFULLY;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.STARTED;
import static com.rtbhouse.kafka.workers.integration.utils.TestTasks.createInterruptibleTask;
import static com.rtbhouse.kafka.workers.integration.utils.TestTasks.createNoopTask;
import static com.rtbhouse.kafka.workers.integration.utils.TestTasks.createNotInterruptibleTask;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_SECOND;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.integration.utils.KafkaServerRule;
import com.rtbhouse.kafka.workers.integration.utils.RequiresKafkaServer;
import com.rtbhouse.kafka.workers.integration.utils.TestProperties;
import com.rtbhouse.kafka.workers.integration.utils.ZookeeperUtils;

@RequiresKafkaServer
public class ShutdownTest {

    private static final String SOME_TOPIC = "some_topic";
    private static final int NUM_PARTITIONS = 4;
    private static final Duration WORKER_SHUTDOWN_TIMEOUT = Duration.ofSeconds(3);

    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            StringDeserializer.class, StringDeserializer.class, SOME_TOPIC);
    static {
        WORKERS_PROPERTIES.put(WorkersConfig.WORKER_THREADS_NUM, NUM_PARTITIONS);
        WORKERS_PROPERTIES.put(WorkersConfig.WORKER_SHUTDOWN_TIMEOUT_MS, WORKER_SHUTDOWN_TIMEOUT.toMillis());
    }

    private static final Properties PRODUCER_PROPERTIES = TestProperties.producerProperties(
            StringSerializer.class, StringSerializer.class);

    @Rule
    public KafkaServerRule kafkaServerRule = new KafkaServerRule(TestProperties.serverProperties());

    private KafkaProducer<String, String> producer;

    @Before
    public void before() {
        producer = new KafkaProducer<>(PRODUCER_PROPERTIES);
    }

    @After
    public void after() {
        producer.close();
    }

    @Test
    public void shouldShutDownWhenThereAreNoInputMessages() throws InterruptedException {

        //given
        topic(SOME_TOPIC, NUM_PARTITIONS, 0);
        KafkaWorkers<String, String> kafkaWorkers = kafkaWorkersStarted(
                config -> createNoopTask()
        );

        //when
        Instant start = Instant.now();
        kafkaWorkers.shutdown(); //blocking
        Duration shutdownTime = elapsedTimeFrom(start);

        //then
        assertThat(kafkaWorkers.getStatus()).isEqualTo(CLOSED_GRACEFULLY);
        assertThat(shutdownTime).isLessThan(WORKER_SHUTDOWN_TIMEOUT.plus(ONE_SECOND));
    }

    @Test
    public void shouldShutDownWhenThreadsCanBeInterrupted() throws InterruptedException {

        //given
        topic(SOME_TOPIC, NUM_PARTITIONS, 5);
        KafkaWorkers<String, String> kafkaWorkers = kafkaWorkersStarted(
                config -> createInterruptibleTask()
        );

        //when
        Instant start = Instant.now();
        kafkaWorkers.shutdown(); //blocking
        Duration shutdownTime = elapsedTimeFrom(start);

        //then
        assertThat(kafkaWorkers.getStatus()).isEqualTo(CLOSED_NOT_GRACEFULLY);
        assertThat(shutdownTime).isBetween(WORKER_SHUTDOWN_TIMEOUT, WORKER_SHUTDOWN_TIMEOUT.multipliedBy(2).plus(ONE_SECOND));
    }

    @Test
    public void shouldShutDownWhenThreadsCannotBeInterrupted() throws InterruptedException {

        //given
        topic(SOME_TOPIC, NUM_PARTITIONS, 5);
        KafkaWorkers<String, String> kafkaWorkers = kafkaWorkersStarted(
                config -> createNotInterruptibleTask()
        );

        //when
        Instant start = Instant.now();
        kafkaWorkers.shutdown(); //blocking
        Duration shutdownTime = elapsedTimeFrom(start);

        //then
        assertThat(kafkaWorkers.getStatus()).isEqualTo(CANNOT_STOP_THREADS);
        assertThat(shutdownTime).isBetween(
                WORKER_SHUTDOWN_TIMEOUT.multipliedBy(2),
                WORKER_SHUTDOWN_TIMEOUT.multipliedBy(2).plus(ONE_SECOND)
        );
    }

    private void topic(String topic, int numPartitions, int numMessagesPerPartition) throws InterruptedException {
        checkArgument(numPartitions >= 1);
        checkArgument(numMessagesPerPartition >= 0);

        ZookeeperUtils.createTopics(kafkaServerRule.getZookeeperConnectString(), numPartitions, 1, topic);
        for (int partition = 0; partition < numPartitions; partition++) {
            for (int i = 0; i < numMessagesPerPartition; i++) {
                producer.send(new ProducerRecord<>(SOME_TOPIC, partition, null,
                        "key_" + partition, "value_" + i));
            }
        }
        producer.flush();
    }

    private <K, V> KafkaWorkers<K, V> kafkaWorkersStarted(WorkerTaskFactory<K, V> taskFactory) throws InterruptedException {
        WorkersConfig workersConfig = new WorkersConfig(WORKERS_PROPERTIES);
        KafkaWorkers<K, V> kafkaWorkers = new KafkaWorkers<>(
                workersConfig,
                taskFactory
        );
        kafkaWorkers.start();

        await().atMost(FIVE_SECONDS).until(() -> kafkaWorkers.getStatus() == STARTED);

        //wait to allow threads to start processing
        SECONDS.sleep(10);

        return kafkaWorkers;
    }

    private Duration elapsedTimeFrom(Instant start) {
        return Duration.between(start, Instant.now());
    }
}
