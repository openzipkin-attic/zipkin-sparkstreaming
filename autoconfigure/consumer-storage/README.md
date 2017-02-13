# autoconfigure-consumer-storage

## ZipkinStorageConsumerAutoConfiguration

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html)
module built into the [Spark Streaming Job](../../sparkstreaming-job)
which writes spans to a Zipkin `StorageComponent`.  Internally, this
module wraps the [StorageConsumer](../../consumer/storage) to expose
configuration options via properties.

## Usage

In order to connect, you minimally need to set `zipkin.storage.type` to
a configured backend. You also need to set relevant properties for that.

Ex.
```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.storage.type=elasticsearch \
  --zipkin.storage.elasticsearch.hosts=http://127.0.0.1:9200 \
  ...
```

### Configuration

Configuration properties can be set via commandline parameters, system
properties or any other alternative [supported by Spring Boot](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html).

Besides setting storage properties, you can also override the following.
All of the below have a prefix of "zipkin.sparkstreaming.consumer.storage"

Property | Default |Description
--- | --- | ---
fail-fast | true | check storage before submitting the job.

## More Examples

Ex. to connect to a local Elasticsearch service:

```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.storage.type=elasticsearch \
  --zipkin.storage.elasticsearch.hosts=http://127.0.0.1:9200 \
  --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092
```

Ex. to connect to a local MySQL service:

```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.storage.type=mysql \
  --zipkin.storage.mysql.host=127.0.0.1 \
  --zipkin.storage.mysql.username=root \
  --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092
```

Ex. to connect to a local Cassandra service:

```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.storage.type=cassandra \
  --zipkin.storage.cassandra.contact-points=127.0.0.1 \
  --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092
```
