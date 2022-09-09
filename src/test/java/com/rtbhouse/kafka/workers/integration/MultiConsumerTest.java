package com.rtbhouse.kafka.workers.integration;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.ShutdownCallback;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.weigher.StringWeigher;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.integration.utils.KafkaServerRule;
import com.rtbhouse.kafka.workers.integration.utils.KafkaUtils;
import com.rtbhouse.kafka.workers.integration.utils.RequiresKafkaServer;
import com.rtbhouse.kafka.workers.integration.utils.TestProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CANNOT_STOP_THREADS;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CLOSED_GRACEFULLY;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.CLOSED_NOT_GRACEFULLY;
import static com.rtbhouse.kafka.workers.api.KafkaWorkers.Status.STARTED;
import static com.rtbhouse.kafka.workers.integration.utils.TestTasks.createInterruptibleTask;
import static com.rtbhouse.kafka.workers.integration.utils.TestTasks.createNoopTask;
import static com.rtbhouse.kafka.workers.integration.utils.TestTasks.createNotInterruptibleTask;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_SECOND;
import static org.awaitility.Durations.TEN_SECONDS;

@RequiresKafkaServer
public class MultiConsumerTest {

    private static final String SOME_TOPIC = "some_topic";
    private static final int NUM_PARTITIONS = 4;
    private static final Duration WORKER_SHUTDOWN_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration CONSUMER_PROCESSING_TIMEOUT = Duration.ofSeconds(30);

    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            StringDeserializer.class, StringDeserializer.class,
            StringWeigher.class, StringWeigher.class,
            SOME_TOPIC);
    static {
        WORKERS_PROPERTIES.put(WorkersConfig.WORKER_THREADS_NUM, NUM_PARTITIONS);
        WORKERS_PROPERTIES.put(WorkersConfig.WORKER_SHUTDOWN_TIMEOUT_MS, WORKER_SHUTDOWN_TIMEOUT.toMillis());
        WORKERS_PROPERTIES.put(WorkersConfig.CONSUMER_PROCESSING_TIMEOUT_MS, CONSUMER_PROCESSING_TIMEOUT.toMillis());
        WORKERS_PROPERTIES.put(WorkersConfig.CONSUMER_THREADS_NUM, 2);
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
    public void shouldShutDownWhenThereAreNoInputMessages() throws Exception {

        //given
        topic(SOME_TOPIC, NUM_PARTITIONS, 10);
        KafkaWorkers<String, String> kafkaWorkers = kafkaWorkersStarted(
                config -> (record, observer) -> {
                    if (record.partition() == 0) {
                        observer.onFailure(new RuntimeException("partition 0"));
                    } else {
                        observer.onSuccess();
                    }
                }
        );

    }


    private Duration runWithTimeout(Runnable runnable, Duration timeout) throws Exception {
        Instant start = Instant.now();
        CompletableFuture.runAsync(runnable).get(timeout.toMillis(), MILLISECONDS);
        return elapsedTimeFrom(start);
    }

    private void topic(String topic, int numPartitions, int numMessagesPerPartition) {
        checkArgument(numPartitions >= 1);
        checkArgument(numMessagesPerPartition >= 0);

        KafkaUtils.createTopics(kafkaServerRule.getBootstrapServers(), numPartitions, 1, topic);
        for (int partition = 0; partition < numPartitions; partition++) {
            for (int i = 0; i < numMessagesPerPartition; i++) {
                producer.send(new ProducerRecord<>(SOME_TOPIC, partition, null,
                        "key_" + partition, "value_" + i));
            }
        }
        producer.flush();
    }

    private <K, V> KafkaWorkers<K, V> kafkaWorkersStarted(WorkerTaskFactory<K, V> taskFactory) throws InterruptedException {
        return kafkaWorkersStarted(taskFactory, null);
    }

    private <K, V> KafkaWorkers<K, V> kafkaWorkersStarted(WorkerTaskFactory<K, V> taskFactory,
            ShutdownCallback shutdownCallback) throws InterruptedException {

        WorkersConfig workersConfig = new WorkersConfig(WORKERS_PROPERTIES);
        KafkaWorkers<K, V> kafkaWorkers = new KafkaWorkers<>(
                workersConfig,
                taskFactory,
                shutdownCallback
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
