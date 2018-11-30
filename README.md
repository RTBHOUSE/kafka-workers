# kafka-workers

Kafka Workers is a client library which unifies records consuming from Kafka and processing them by user-defined WorkerTasks.

It provides:
 - higher level of distribution because of sub-partitioning defined by **WorkerPartitioner**,
 - tighter control of offsets commits to Kafka applied by **RecordStatusObserver**,
 - possibility to pause and resume processing by **WorkerTask** for given partition.

Additionally it supports:
- at-least-once state and output semantics,
- backpressure,
- processing timeouts,
- handling failures.

## Version

Current version is **1.0.2**

## Requirements

You need Java 8 and at least Apache Kafka 2.0 to use this library.

## Installation

Releases are distributed on Maven central:

```xml
<dependency>
    <groupId>com.rtbhouse</groupId>
    <artifactId>kafka-workers</artifactId>
    <version>1.0.2</version>
</dependency>
```

## Usage

To use Kafka Workers you should implement the following interfaces:

```java
public interface WorkerTask<K, V> {
    void init(WorkerSubpartition subpartition, WorkersConfig config);
    boolean accept(WorkerRecord<K, V> record);
    void process(WorkerRecord<K, V> record, RecordStatusObserver observer);
    void close();
}
```
User-defined task which is associated with one of WorkerSubpartitions. The most crucial are: accept() and process() methods. The first one checks if given WorkerRecord could be polled from internal WorkerSubpartition's queue peek and passed to process method. The second one processes just polled WorkerRecord from given WorkerSubpartition's internal queue. Processing could be done synchronously or asynchronously but in both cases one of the RecordStatusObserver's methods onSuccess() or onFailure() has to be called. Not calling any of these methods for configurable amount of time will be considered as a failure.

```java
public interface WorkerPartitioner<K, V> {
    int subpartition(ConsumerRecord<K, V> consumerRecord);
    int count();
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
## Internals

Internally one Kafka Workers instance launches one consumer thread and configurable count of worker threads. Each thread can execute one or more WorkerTasks and each WorkerTask processes WorkerRecords from internal queue associated with given WorkerSubpartition. Kafka Workers ensures by its offsets state that only continuously processed offsets are commited. 

![Kafka Workers Architecture](docs/workers-arch.png)
