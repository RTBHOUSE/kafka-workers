package com.rtbhouse.kafka.workers.impl.task;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.AbstractWorkersThread;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;

public class WorkerThread<K, V> extends AbstractWorkersThread {

    public static final Logger logger = LoggerFactory.getLogger(WorkerThread.class);

    private final int workerId;
    private final TaskManager<K, V> taskManager;
    private final QueuesManager<K, V> queueManager;
    private final OffsetsState offsetsState;
    private final List<WorkerTaskImpl<K, V>> tasks = new ArrayList<>();

    private volatile boolean waiting = false;

    public WorkerThread(
            int workerId,
            WorkersConfig config,
            WorkersMetrics metrics,
            KafkaWorkersImpl<K, V> workers,
            TaskManager<K, V> taskManager,
            QueuesManager<K, V> queueManager,
            OffsetsState offsetsState) {
        super("worker-thread-" + workerId, config, metrics, workers);
        this.workerId = workerId;
        this.taskManager = taskManager;
        this.queueManager = queueManager;
        this.offsetsState = offsetsState;
    }

    @Override
    public void init() {
        // tasks are initialized by TaskManager during task registration
    }

    @Override
    public void process() throws InterruptedException {
        int accepting = 0, accepted = 0;
        // gets tasks to process or blocks current thread in two cases:
        // 1. all assigned tasks have empty internal queues without any records to process (to avoid busy waiting)
        // 2. there are not any tasks assigned (e.g. because of tasks rebalance)
        for (WorkerTaskImpl<K, V> task : getTasksToProcess()) {
            WorkerRecord<K, V> peekRecord = queueManager.peek(task.subpartition());
            if (peekRecord == null) {
                throw new WorkersException("peekRecord is null");
            }
            accepting++;
            if (task.accept(peekRecord)) {
                accepted++;
                WorkerRecord<K, V> pollRecord = queueManager.poll(task.subpartition());
                if (pollRecord == null || !pollRecord.equals(peekRecord)) {
                    throw new WorkersException("peekRecord and pollRecord are different");
                }
                try {
                    task.process(pollRecord, new RecordStatusObserverImpl(pollRecord, this, offsetsState, metrics));
                } catch (Exception e) {
                    logger.error("Exception when processing record: {}", pollRecord.toString());
                    throw e;
                }
            }
        }
        if (accepted == 0) {
            logger.debug("goes to sleep for {} ms because from {} peek records {} is accepted",
                    config.getLong(WorkersConfig.WORKER_SLEEP_MS), accepting, accepted);
            // all records are not accepted to process so thread goes to sleep (again to avoid busy waiting)
            Thread.sleep(config.getLong(WorkersConfig.WORKER_SLEEP_MS));
        }
    }

    @Override
    public void close() {
        for (WorkerTaskImpl<K, V> task : tasks) {
            task.close();
        }
    }

    @Override
    public synchronized void shutdown(WorkersException exception) {
        super.shutdown(exception);
        // in case of shutdown we do not want to block thread any more
        notifyAll();
    }

    public int getWorkerId() {
        return workerId;
    }

    public void clearTasks() {
        tasks.clear();
    }

    public void addTask(WorkerTaskImpl<K, V> task) {
        tasks.add(task);
    }

    public int getTasksCount() {
        return tasks.size();
    }

    public boolean isStopped() {
        return waiting || closed;
    }

    public synchronized void notifyThread() {
        for (WorkerTaskImpl<K, V> task : tasks) {
            if (queueManager.peek(task.subpartition()) != null) {
                waiting = false;
                // wakes thread up because at least one record was pushed to process
                notifyAll();
            }
        }
    }

    private synchronized List<WorkerTaskImpl<K, V>> getTasksToProcess() throws InterruptedException {
        List<WorkerTaskImpl<K, V>> tasksToProcess = new ArrayList<>();
        while (tasksToProcess.isEmpty() && !shutdown) { // in case of shutdown we do not want to block thread
            int queues = 0;
            for (WorkerTaskImpl<K, V> task : tasks) {
                queues++;
                if (queueManager.peek(task.subpartition()) != null) {
                    tasksToProcess.add(task);
                }
            }
            if (tasksToProcess.isEmpty()) {
                logger.debug("waits because all {} queues are empty", queues);
                waiting = true;
                // notifies TaskManager that thread is waiting so possible tasks rebalance could take place now
                taskManager.notifyTaskManager();
                // blocks thread because there are not any tasks/records to process
                wait();
            }
        }
        return tasksToProcess;
    }

}
