package com.rtbhouse.kafka.workers.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.ShutdownCallback;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.record.weigher.ByteArrayWeigher;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.impl.errors.BadStatusException;
import com.rtbhouse.kafka.workers.integration.utils.KafkaServerRule;
import com.rtbhouse.kafka.workers.integration.utils.RequiresKafkaServer;
import com.rtbhouse.kafka.workers.integration.utils.TestProperties;
import com.rtbhouse.kafka.workers.integration.utils.ZookeeperUtils;

@RequiresKafkaServer
public class LifecycleTest {

    private static final String TOPIC = "topic";

    private static final Properties SERVER_PROPERTIES = TestProperties.serverProperties();

    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            ByteArrayDeserializer.class, ByteArrayDeserializer.class,
            ByteArrayWeigher.class, ByteArrayWeigher.class,
            TOPIC);

    @Rule
    public KafkaServerRule kafkaServerRule = new KafkaServerRule(SERVER_PROPERTIES);

    @Before
    public void before() throws Exception {
        ZookeeperUtils.createTopics(kafkaServerRule.getZookeeperConnectString(), 1, 1, TOPIC);
    }

    @Test
    public void shouldThrowExceptionForStartAfterStart() {
        KafkaWorkers<byte[], byte[]> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TaskFactory());

        kafkaWorkers.start();

        assertThatThrownBy(() -> {
            kafkaWorkers.start();
        }).isInstanceOf(BadStatusException.class);

        kafkaWorkers.shutdown();
    }

    @Test
    public void shouldThrowExceptionForStartAfterShutdown() {
        KafkaWorkers<byte[], byte[]> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TaskFactory());

        kafkaWorkers.start();
        kafkaWorkers.shutdown();

        assertThatThrownBy(() -> {
            kafkaWorkers.start();
        }).isInstanceOf(BadStatusException.class);
    }

    @Test
    public void shouldNotThrowExceptionForShutdownAfterShutdown() throws Exception {
        KafkaWorkers<byte[], byte[]> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TaskFactory());

        kafkaWorkers.start();
        kafkaWorkers.shutdown();

        assertThatCode(() -> {
            kafkaWorkers.shutdown();
        }).doesNotThrowAnyException();
    }

    @Test
    public void shouldNotThrowExceptionForShutdownBeforeStart() throws Exception {
        KafkaWorkers<byte[], byte[]> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TaskFactory());

        assertThatCode(() -> {
            kafkaWorkers.shutdown();
            kafkaWorkers.start();
            kafkaWorkers.shutdown();
        }).doesNotThrowAnyException();
    }

    @Test
    public void shouldShutdownWorkers() throws Exception {

        // given
        CountDownLatch latch = new CountDownLatch(1);
        List<WorkersException> exceptions = new ArrayList<>();

        KafkaWorkers<byte[], byte[]> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TaskFactory(),
                new ShutdownCallback() {
                    @Override
                    public void onShutdown(WorkersException exception) {
                        exceptions.add(exception);
                        latch.countDown();
                    }
                });

        // when
        kafkaWorkers.start();
        kafkaWorkers.shutdown();

        // then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(exceptions.size()).isEqualTo(1);
        assertThat(exceptions.get(0)).isNull();
    }

    private static class TaskFactory implements WorkerTaskFactory<byte[], byte[]> {

        @Override
        public WorkerTask<byte[], byte[]> createTask(WorkersConfig config) {
            return new WorkerTask<byte[], byte[]>() {

                @Override
                public void process(WorkerRecord<byte[], byte[]> record, RecordStatusObserver observer) {
                }

            };
        }

    }

}
