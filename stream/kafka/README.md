# stream-kafka

## KafkaStream

This stream ingests Spans form a Kafka topic advertised by Zookeeper, using Spark Kafka libraries.

Kafka messages should contain a list of spans in json or TBinaryProtocol big-endian encoding.
Details on message encode is available [here](https://github.com/openzipkin/zipkin/blob/master/zipkin-collector/kafka/README.md#encoding-spans-into-kafka-messages)

## Quick Start
In order to connect, you minimally need to supply Kafka bootstrap servers, or
connect servers used to look them up from Zookeeper.

Ex.
```bash
# to supply your Kafka server directly
java -jar zipkin-sparkstreaming-job.jar --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092
# to lookup your Kafka server with Zookeeper
java -jar zipkin-sparkstreaming-job.jar --zipkin.sparkstreaming.stream.kafka.zookeeper.connect-servers=127.0.0.1:2181
```

## Configuration
KafkaStream can be used as a library, where attributes are set via
`KafkaStreamFactory.Builder`. It is more commonly enabled with Spring via autoconfiguration.

Here are the relevant setting and a short description. All properties
have a prefix of "zipkin.sparkstreaming.stream.kafka"

Property | Default | Description
--- | --- | ---
topic | zipkin | Kafka topic encoded lists of spans are be consumed from.
bootstrap-servers | none | Initial set of kafka servers to connect to; others may be discovered. Values are in comma-separated host:port syntax. Ex "host1:9092,host2:9092".
zookeeper.connect-servers | none | Looks up bootstrap-servers from Zookeeper. Values are in comma-separated host:port syntax. Ex "host1:2181,host2:2181".
zookeeper.connect-suffix | "" | Optional chroot path used as a suffix for connect string.
zookeeper.session-timeout | 10000 | Session timeout for looking up bootstrap-servers.