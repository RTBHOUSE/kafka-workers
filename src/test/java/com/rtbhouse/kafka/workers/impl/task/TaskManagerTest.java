package com.rtbhouse.kafka.workers.impl.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.partitioner.RoundRobinPartitioner;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;

@RunWith(MockitoJUnitRunner.class)
public class TaskManagerTest {

    private static final int WORKER_THREADS_NUM = 3;

    @Mock
    private WorkersConfig config;

    @Mock
    private WorkersMetrics metrics;

    @Mock
    private KafkaWorkersImpl<byte[], byte[]> workers;

    @Mock
    private OffsetsState offsetsState;

    @Mock
    private QueuesManager<byte[], byte[]> queueManager;

    @Test
    public void shouldRebalanceTasks() throws InterruptedException {

        // given
        WorkerRecord<byte[], byte[]> record = new WorkerRecord<>(new ConsumerRecord<>("topic", 0, 0L, null, null), 0);
        when(queueManager.peek(any())).thenReturn(record);

        WorkerTaskFactory<byte[], byte[]> taskFactory = new TaskFactory();
        SubpartitionSupplier<byte[], byte[]> subpartitionSupplier = new SubpartitionSupplier<>(new RoundRobinPartitioner<>(10));
        List<WorkerThread<byte[], byte[]>> threads = new ArrayList<>();

        TaskManager<byte[], byte[]> taskManager = new TaskManager<>(
                config, metrics, workers, taskFactory, subpartitionSupplier, threads, offsetsState);

        for (int i = 0; i < WORKER_THREADS_NUM; i++) {
            threads.add(new WorkerThread<>(i, config, metrics, workers, taskManager, queueManager));
        }
        ExecutorService executorService = Executors.newFixedThreadPool(WORKER_THREADS_NUM);
        for (WorkerThread<byte[], byte[]> workerThread : threads) {
            executorService.submit(workerThread);
        }

        // when
        assertThatCode(() -> {
            taskManager.register(Arrays.asList(new TopicPartition("topic", 0)));
        }).doesNotThrowAnyException();

        // then
        assertThat(threads.get(0).getTasksCount()).isEqualTo(4);
        assertThat(threads.get(1).getTasksCount()).isEqualTo(3);
        assertThat(threads.get(2).getTasksCount()).isEqualTo(3);

        // sleep to let thread to run tasks and process some records before next rebalance
        Thread.sleep(1000);

        // when
        assertThatCode(() -> {
            taskManager.register(Arrays.asList(new TopicPartition("topic", 0), new TopicPartition("topic", 1)));
        }).doesNotThrowAnyException();

        // then
        assertThat(threads.get(0).getTasksCount()).isEqualTo(7);
        assertThat(threads.get(1).getTasksCount()).isEqualTo(7);
        assertThat(threads.get(2).getTasksCount()).isEqualTo(6);

        executorService.shutdown();
    }

    private static class TaskFactory implements WorkerTaskFactory<byte[], byte[]> {

        @Override
        public WorkerTask<byte[], byte[]> createTask(WorkersConfig config) {
            return (record, observer) -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new WorkersException(e);
                }
            };
        }

    }

}
