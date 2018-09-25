package com.rtbhouse.kafka.workers.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
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
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
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
public class MergerTest {

    private static final Logger logger = LoggerFactory.getLogger(MergerTest.class);

    private static final String TOPIC_ONE = "topic1";
    private static final String TOPIC_TWO = "topic2";
    private static final String OUTPUT_TOPIC = "output-topic";

    private static final Properties SERVER_PROPERTIES = TestProperties.serverProperties();

    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            StringDeserializer.class, StringDeserializer.class, TOPIC_ONE, TOPIC_TWO);

    private static final Properties CONSUMER_PROPERTIES = TestProperties.consumerProperties(
            StringDeserializer.class, StringDeserializer.class);

    private static final Properties PRODUCER_PROPERTIES = TestProperties.producerProperties(
            StringSerializer.class, StringSerializer.class);

    @Rule
    public KafkaServerRule kafkaServerRule = new KafkaServerRule(SERVER_PROPERTIES);

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;


    @Before
    public void before() throws Exception {
        ZookeeperUtils.createTopics(kafkaServerRule.getZookeeperConnectString(), 2, 1, TOPIC_ONE, TOPIC_TWO, OUTPUT_TOPIC);
        consumer = new KafkaConsumer<>(CONSUMER_PROPERTIES);
        producer = new KafkaProducer<>(PRODUCER_PROPERTIES);
    }

    @After
    public void after() throws IOException {
        producer.close();
        consumer.close();
    }

    @Test
    public void shouldMergeStreams() throws Exception {

        // given
        producer.send(new ProducerRecord<>(TOPIC_ONE, 0, 10000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_ONE, 0, 50000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_ONE, 0, 70000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_ONE, 0, 130000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_ONE, 0, 150000L, (String) null, (String) null));

        producer.send(new ProducerRecord<>(TOPIC_ONE, 1, 40000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_ONE, 1, 90000L, (String) null, (String) null));

        producer.send(new ProducerRecord<>(TOPIC_TWO, 0, 30000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_TWO, 0, 90000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_TWO, 0, 110000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_TWO, 0, 120000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_TWO, 0, 140000L, (String) null, (String) null));

        producer.send(new ProducerRecord<>(TOPIC_ONE, 1, 20000L, (String) null, (String) null));
        producer.send(new ProducerRecord<>(TOPIC_ONE, 1, 200000L, (String) null, (String) null));

        KafkaWorkers<String, String> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new MergerTaskFactory());

        // when
        kafkaWorkers.start();

        consumer.subscribe(Arrays.asList(TOPIC_ONE, TOPIC_TWO, OUTPUT_TOPIC));

        List<Long> expected = Arrays.asList(10000L, 30000L, 50000L, 70000L, 90000L, 110000L, 120000L, 130000L, 140000L);
        List<Long> actual = new ArrayList<>();
        for (int i = 0; i < 20 && actual.size() < expected.size(); ++i) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                if (OUTPUT_TOPIC.equals(record.topic()) && record.partition() == 0) {
                    actual.add(record.timestamp());
                }
            }
        }
        consumer.close();

        // then
        assertThat(actual).isEqualTo(expected);

        kafkaWorkers.shutdown();
    }

    private static class MergerTask implements WorkerTask<String, String> {

        private KafkaProducer<String, String> taskProducer;

        private Map<TopicPartition, Long> timestamps;

        public MergerTask(Map<TopicPartition, Long> timestamps) {
            this.timestamps = timestamps;
        }

        @Override
        public void init(WorkerSubpartition subpartition, WorkersConfig config) {
            taskProducer = new KafkaProducer<>(PRODUCER_PROPERTIES);
        }

        @Override
        public boolean accept(WorkerRecord<String, String> record) {
            timestamps.put(record.topicPartition(), record.timestamp());

            TopicPartition otherPartition = new TopicPartition(
                    TOPIC_ONE.equals(record.topic()) ? TOPIC_TWO : TOPIC_ONE,
                    record.partition());

            boolean result = timestamps.get(otherPartition) != null && timestamps.get(otherPartition) >= record.timestamp();
            logger.info("accept(partition: {}, timestamp: {}): {}", record.partition(), record.timestamp(), result);
            return result;
        }

        @Override
        public void process(WorkerRecord<String, String> record, RecordStatusObserver observer) {
            logger.info("process(partition: {}, timestamp: {})", record.partition(), record.timestamp());

            Future<RecordMetadata> future = taskProducer.send(new ProducerRecord<>(
                    OUTPUT_TOPIC,
                    record.partition(),
                    record.timestamp(),
                    record.key(),
                    record.value()));

            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                observer.onFailure(new ProcessingFailureException("could not send " + record, e));
            }

            observer.onSuccess();
        }

        @Override
        public void close() {
            logger.info("close producer");
            taskProducer.close();
        }

    }

    private static class MergerTaskFactory implements WorkerTaskFactory<String, String> {

        private Map<TopicPartition, Long> timestamps = new ConcurrentHashMap<>();

        @Override
        public MergerTask createTask(WorkersConfig config) {
            return new MergerTask(timestamps);
        }

    }

}
