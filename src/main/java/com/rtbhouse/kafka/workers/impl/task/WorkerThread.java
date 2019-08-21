package com.rtbhouse.kafka.workers.impl.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.AbstractWorkersThread;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;
import com.rtbhouse.kafka.workers.impl.record.RecordStatusObserverImpl;

public class WorkerThread<K, V> extends AbstractWorkersThread {

    public static final Logger logger = LoggerFactory.getLogger(WorkerThread.class);

    private final int workerId;

    private final long workerSleepMs;
    private final long punctuatorIntervalMs;

    private final TaskManager<K, V> taskManager;
    private final QueuesManager<K, V> queueManager;
    private final List<WorkerTaskImpl<K, V>> tasks = new CopyOnWriteArrayList<>();
    private final Map<WorkerSubpartition, WorkerSubpartition> subpartitions = new HashMap<>();
    private final RecordStatusObserverImpl.Context<K, V> recordStatusObserverContext;

    private volatile boolean waiting = false;
    private volatile long punctuateTime = System.currentTimeMillis();

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

        this.workerSleepMs = config.getLong(WorkersConfig.WORKER_SLEEP_MS);
        this.punctuatorIntervalMs = config.getLong(WorkersConfig.PUNCTUATOR_INTERVAL_MS);

        this.taskManager = taskManager;
        this.queueManager = queueManager;
        this.recordStatusObserverContext = new RecordStatusObserverImpl.Context<>(metrics, config, offsetsState, this);
    }

    @Override
    public void init() {
        // tasks are initialized by TaskManager during task registration
        metrics.addWorkerThreadMetrics(this);
    }

    @Override
    public void process() throws InterruptedException {
        int checkedTasksCount = 0, acceptedTasksCount = 0;
        // gets tasks to process or blocks current thread in two cases:
        // 1. all assigned tasks have empty internal queues without any records to process (to avoid busy waiting)
        // 2. there are not any tasks assigned (e.g. because of tasks rebalance)
        for (WorkerTaskImpl<K, V> task : getTasksToProcess()) {
            WorkerRecord<K, V> peekRecord = queueManager.peek(task.subpartition());
            if (peekRecord == null) {
                throw new WorkersException("peekRecord is null");
            }
            checkedTasksCount++;
            if (task.accept(peekRecord)) {
                acceptedTasksCount++;
                WorkerRecord<K, V> pollRecord = queueManager.poll(task.subpartition());
                if (pollRecord == null || !pollRecord.equals(peekRecord)) {
                    throw new WorkersException("peekRecord and pollRecord are different");
                }

                WorkerSubpartition workerSubpartition = getWorkerSubpartition(pollRecord);
                RecordStatusObserver observer = new RecordStatusObserverImpl<>(pollRecord, workerSubpartition, recordStatusObserverContext);
                task.process(pollRecord, observer);
            }
        }

        if (shouldPunctuateNow()) {
            long currentTime = System.currentTimeMillis();
            for (WorkerTaskImpl<K, V> task : tasks) {
                task.punctuate(currentTime);
            }
            punctuateTime = currentTime;

        } else if (acceptedTasksCount == 0) {
            // all records are not accepted to process so thread goes to sleep (again to avoid busy waiting)

            long sleepMillis = Math.min(workerSleepMs, remainingMsToPunctuate());
            if (sleepMillis > 0) {
                logger.debug("goes to sleep for {} ms because from {} peek records 0 is accepted", sleepMillis, checkedTasksCount);
                Thread.sleep(sleepMillis);
            }
        }
    }

    private WorkerSubpartition getWorkerSubpartition(WorkerRecord<K, V> record) {
        WorkerSubpartition subpartition = new WorkerSubpartition(record.topic(), record.partition(), record.subpartition());
        // Keep only one copy of the given subpartition in memory
        return subpartitions.computeIfAbsent(subpartition, s -> s);
    }

    @Override
    public void close() {
        for (WorkerTaskImpl<K, V> task : tasks) {
            task.close();
        }
        metrics.removeWorkerThreadMetrics(this);
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

    public boolean isNotRunning() {
        return waiting || stopped;
    }

    public synchronized void notifyThread() {
        for (WorkerTaskImpl<K, V> task : tasks) {
            if (queueManager.peek(task.subpartition()) != null) {
                waiting = false;
                // wakes thread up because at least one record was pushed to process
                notifyAll();
                break;
            }
        }
        if (shouldPunctuateNow()) {
            waiting = false;
            // wakes thread up because should punctuate tasks
            notifyAll();
        }
    }

    public boolean shouldPunctuateNow() {
        if (tasks.size() > 0 && remainingMsToPunctuate() <= 0) {
            return true;
        }
        return false;
    }

    private long remainingMsToPunctuate() {
        long currentTime = System.currentTimeMillis();
        return punctuatorIntervalMs - (currentTime - punctuateTime);
    }

    private synchronized List<WorkerTaskImpl<K, V>> getTasksToProcess() throws InterruptedException {
        List<WorkerTaskImpl<K, V>> tasksToProcess = new ArrayList<>();
        while (tasksToProcess.isEmpty() && !shutdown && !shouldPunctuateNow()) {
            // in case of shutdown or punctuate we do not want to block thread
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
