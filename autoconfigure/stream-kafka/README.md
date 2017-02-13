# autoconfigure-stream-kafka

## ZipkinKafkaStreamFactoryAutoConfiguration

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html) 
module built into the [Spark Streaming Job](../../sparkstreaming-job) 
which reads encoded lists of spans from a Kafka topic.  Internally, this
module wraps the [KafkaStreamFactory](../../stream/kafka) to expose
configuration options via properties.

## Usage

In order to connect, you minimally need to set
`zipkin.sparkstreaming.stream.kafka.bootstrap-servers` or
`zipkin.sparkstreaming.stream.kafka.zookeeper.connect`.

Ex.
```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092 \
  ...
```

### Configuration

Configuration properties can be set via commandline parameters, system
properties or any other alternative [supported by Spring Boot](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html).

Besides setting kafka properties, you can also override the following.
All of the below have a prefix of "zipkin.sparkstreaming.stream.kafka"

Property | Default | Description
--- | --- | ---
topic | zipkin | Kafka topic encoded lists of spans are be consumed from.
group-id | zipkin | Consumer group this process is consuming on behalf of.
bootstrap-servers | none | Initial set of kafka servers to connect to; others may be discovered. Values are in comma-separated host:port syntax. Ex "host1:9092,host2:9092".
zookeeper.connect | none | Looks up bootstrap-servers from Zookeeper. Values is a connect string (comma-separated host:port with optional suffix) Ex "host1:2181,host2:2181".
zookeeper.session-timeout | 10000 | Session timeout for looking up bootstrap-servers.

## More Examples

Ex. to lookup bootstrap servers using Zookeeper

```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.sparkstreaming.stream.kafka.zookeeper.connect=127.0.0.1:2181
  ...
```
