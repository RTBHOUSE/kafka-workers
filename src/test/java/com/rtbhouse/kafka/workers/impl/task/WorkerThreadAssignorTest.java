package com.rtbhouse.kafka.workers.impl.task;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.impl.task.SpreadSubpartitionsWorkerThreadAssignor;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;
import com.rtbhouse.kafka.workers.impl.task.WorkerThreadAssignor;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class WorkerThreadAssignorTest {

    @Test
    public void shouldSpreadSubpartitionsAsFairAsPossible() {
        final WorkerThreadAssignor<Object, Object> workerThreadAssignor = new SpreadSubpartitionsWorkerThreadAssignor<>();

        final WorkerThread<Object, Object> workerThread1 = Mockito.mock(WorkerThread.class);
        final WorkerThread<Object, Object> workerThread2 = Mockito.mock(WorkerThread.class);

        // when
        final Map<WorkerThread<Object, Object>, List<WorkerSubpartition>> assignments = workerThreadAssignor.assign(List.of(
                getSubpartition("topic_a", 0, 0),
                getSubpartition("topic_b", 0, 0),
                getSubpartition("topic_a", 0, 1),
                getSubpartition("topic_b", 0, 1),
                getSubpartition("topic_a", 0, 2),
                getSubpartition("topic_b", 0, 2)
        ), List.of(workerThread1, workerThread2));

        // then
        Assertions.assertThat(assignments).containsValues(
                List.of(
                        getSubpartition("topic_a", 0, 0),
                        getSubpartition("topic_a", 0, 2),
                        getSubpartition("topic_b", 0, 1)
                ),
                List.of(
                        getSubpartition("topic_a", 0, 1),
                        getSubpartition("topic_b", 0, 0),
                        getSubpartition("topic_b", 0, 2)
                )
        );
    }

    @Test
    public void shouldSpreadSubpartitionsAsFairAsPossible2() {
        final WorkerThreadAssignor<Object, Object> workerThreadAssignor = new SpreadSubpartitionsWorkerThreadAssignor<>();

        final WorkerThread<Object, Object> workerThread1 = Mockito.mock(WorkerThread.class);
        final WorkerThread<Object, Object> workerThread2 = Mockito.mock(WorkerThread.class);
        final WorkerThread<Object, Object> workerThread3 = Mockito.mock(WorkerThread.class);

        // when
        final Map<WorkerThread<Object, Object>, List<WorkerSubpartition>> assignments = workerThreadAssignor.assign(List.of(
                getSubpartition("topic_a", 0, 3),
                getSubpartition("topic_a", 0, 1),
                getSubpartition("topic_a", 0, 0),
                getSubpartition("topic_a", 0, 2),
                getSubpartition("topic_a", 1, 0),
                getSubpartition("topic_a", 1, 2),
                getSubpartition("topic_a", 1, 1),
                getSubpartition("topic_a", 1, 3),
                getSubpartition("topic_b", 1, 3),
                getSubpartition("topic_b", 1, 1),
                getSubpartition("topic_b", 1, 0),
                getSubpartition("topic_b", 1, 2)
        ), List.of(workerThread1, workerThread2, workerThread3));

        // then
        Assertions.assertThat(assignments).containsValues(
                List.of(
                        getSubpartition("topic_a", 0, 0),
                        getSubpartition("topic_a", 0, 3),
                        getSubpartition("topic_a", 1, 2),
                        getSubpartition("topic_b", 1, 1)
                ),
                List.of(
                        getSubpartition("topic_a", 0, 1),
                        getSubpartition("topic_a", 1, 0),
                        getSubpartition("topic_a", 1, 3),
                        getSubpartition("topic_b", 1, 2)
                ),
                List.of(
                        getSubpartition("topic_a", 0, 2),
                        getSubpartition("topic_a", 1, 1),
                        getSubpartition("topic_b", 1, 0),
                        getSubpartition("topic_b", 1, 3)
                )
        );
    }

    private WorkerSubpartition getSubpartition(String topic, int partition, int subpartition) {
        return WorkerSubpartition.getInstance(new TopicPartition(topic, partition), subpartition);
    }

}