# kafka-workers

Kafka Workers is a client library which unifies records consuming from Kafka and processing them by user-defined WorkerTasks. It provides:
 - higher level of distribution because of sub-partitioning defined by **WorkerPartitioner**,
 - tighter control of offsets commits to Kafka applied by **RecordStatusObserver**,
 - possibility to pause and resume processing by **WorkerTask** for given partition,
 - at-least-once state and output semantics,
 - backpressure,
 - processing timeouts,
 - handling failures.

## Version

Current version is **1.0.10**

## Requirements

You need Java 11 and at least Apache Kafka 2.0 to use this library.

## Installation

Releases are distributed on Maven central:

```xml
<dependency>
    <groupId>com.rtbhouse</groupId>
    <artifactId>kafka-workers</artifactId>
    <version>1.0.10</version>
</dependency>
```

## Usage

To use Kafka Workers you should implement the following interfaces:

```java
public interface WorkerTask<K, V> {

    void init(WorkerSubpartition subpartition, WorkersConfig config);

    boolean accept(WorkerRecord<K, V> record);

    void process(WorkerRecord<K, V> record, RecordStatusObserver observer);

    void punctuate(long punctuateTime);

    void close();
}
```
User-defined task which is associated with one of WorkerSubpartitions. The most crucial are: accept() and process() methods. The first one checks if given WorkerRecord could be polled from internal WorkerSubpartition's queue peek and passed to process method. The second one processes just polled WorkerRecord from given WorkerSubpartition's internal queue. Processing could be done synchronously or asynchronously but in both cases one of the RecordStatusObserver's methods onSuccess() or onFailure() has to be called. Not calling any of these methods for configurable amount of time will be considered as a failure. Additionally, punctuate() method allows to do maintenance tasks every configurable amount of time independently if there are records to process or not. All the methods: accept(), process() and punctuate() are executed in a single thread so synchronization is not necessary.

```java
public interface WorkerPartitioner<K, V> {

    int subpartition(ConsumerRecord<K, V> consumerRecord);

    int count(TopicPartition topicPartition);
}
```
User-defined partitioner is used for additional sub-partitioning which could give better distribution of processing. It means that stream of records from one TopicPartition could be reordered during processing but records with the same WorkerSubpartition remain ordered to each other. It leads also to a bit more complex offsets committing policy which is provided by Kafka Workers to ensure at-least-once delivery.

Usage example:
```java

    Properties properties = new Properties();
    properties.setProperty("consumer.topics", "my-topic");
    properties.setProperty("consumer.kafka.bootstrap.servers", "localhost:9192");
    properties.setProperty("consumer.kafka.group.id", "my-workers");
    properties.setProperty("consumer.kafka.key.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
    properties.setProperty("consumer.kafka.value.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");

    KafkaWorkers<String, String> kafkaWorkers = new KafkaWorkers<>(
        new WorkersConfig(properties),
        new MyWorkerTaskFactory<>(),
        new MyWorkerPartitioner<>(),
        new MyShutdownCallback());

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaWorkers::shutdown));
    kafkaWorkers.start();
 }
```

## Configuration

<table class="data-table"><tbody>
<tr>
   <th>Name</th>
   <th>Description</th>
   <th>Type</th>
   <th>Default</th>
</tr>
<tr>
   <td>consumer.topics</td>
   </td>
   <td>A list of kafka topics read by ConsumerThread.</td>
   </td>
   <td>list</td>
   </td>
   <td></td>
</tr>
<tr>
   <td>consumer.commit.interval.ms</td>
   </td>
   <td>The frequency in milliseconds that the processed offsets are committed to Kafka.</td>
   </td>
   <td>long</td>
   </td>
   <td>10000</td>
   </td>
</tr>
<tr>
   <td>consumer.processing.timeout.ms</td>
   </td>
   <td>The timeout in milliseconds for record to be successfully processed.</td>
   </td>
   <td>long</td>
   </td>
   <td>300000</td>
   </td>
</tr>
<tr>
   <td>consumer.poll.timeout.ms</td>
   </td>
   <td>The time in milliseconds spent waiting in poll if data is not available in the buffer. If 0, returns immediately with any records that are available currently in the buffer, else returns empty.</td>
   </td>
   <td>long</td>
   </td>
   <td>1000</td>
   </td>
</tr>
<tr>
   <td>consumer.commit.retries</td>
   </td>
   <td>The number of retries in case of retriable commit failed exception.</td>
   </td>
   <td>int</td>
   </td>
   <td>3</td>
   </td>
</tr>
<tr>
   <td>consumer.kafka</td>
   </td>
   <td>Should be used as a prefix for internal kafka consumer configuration. Usage example:

    consumer.kafka.bootstrap.servers = localhost:9192
    consumer.kafka.group.id = my-workers
    consumer.kafka.key.deserializer = org.apache.kafka.common.serialization.BytesDeserializer
    consumer.kafka.value.deserializer = org.apache.kafka.common.serialization.BytesDeserializer
   </td>
   </td>
   <td></td>
   </td>
   <td></td>
</tr>
<tr>
   <td>worker.threads.num</td>
   </td>
   <td>The number of WorkerThreads per one Kafka Workers instance.</td>
   </td>
   <td>int</td>
   </td>
   <td>1</td>
   </td>
</tr>
<tr>
   <td>worker.sleep.ms</td>
   </td>
   <td>The time in milliseconds to wait for WorkerThread in case of not accepted tasks.</td>
   </td>
   <td>long</td>
   </td>
   <td>1000</td>
   </td>
</tr>
<tr>
   <td>worker.processing.guarantee</td>
   </td>
   <td>Specifies worker processing guarantees. Possible values:
    <ul>
        <li><code>none</code> - logs and skips records which cause processing failure, thus failures don't cause message retransmission and may result in data loss.</li>
        <li><code>at_least_once</code> - shuts Kafka Workers down on record processing failure, enforces message retransmission upon restart and may cause data duplication.</li>
    </ul>
   </td>
   <td>String</td>
   </td>
   <td><code>at_least_once</code></td>
</tr>
<tr>
   <td>worker.task</td>
   </td>
   <td>Could be used as a prefix for internal task configuration.
   </td>
   </td>
   <td></td>
   </td>
   <td></td>
</tr>
<tr>
   <td>punctuator.interval.ms</td>
   </td>
   <td>The frequency in milliseconds that punctuate method is called.</td>
   </td>
   <td>long</td>
   </td>
   <td>1000</td>
   </td>
</tr>
<tr>
   <td>queue.max.size.bytes</td>
   </td>
   <td>This configuration controls the max size in bytes for single WorkerSubpartition's internal queue.</td>
   </td>
   <td>long</td>
   </td>
   <td>268435456</td>
   </td>
</tr>
<tr>
   <td>queue.total.max.size.bytes</td>
   </td>
   <td>This configuration controls the total max size in bytes for all internal queues.</td>
   </td>
   <td>long</td>
   </td>
   <td>null</td>
   </td>
</tr>
<tr>
   <td>metric.reporters</td>
   </td>
   <td>A list of classes to use as metrics reporters. Implementing the <code>org.apache.kafka.common.metrics.MetricsReporter</code> interface allows plugging in classes that will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.</td>
   </td>
   <td>list</td>
   </td>
   <td>""</td>
   </td>
</tr>
</tbody></table>

## Internals

Internally one Kafka Workers instance launches one consumer thread, one punctuator thread and configurable count of worker threads. Each thread can execute one or more WorkerTasks and each WorkerTask processes WorkerRecords from internal queue associated with given WorkerSubpartition. Kafka Workers ensures by its offsets state that only continuously processed offsets are commited. 

![Kafka Workers Architecture](docs/workers-arch.png)
