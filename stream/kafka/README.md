# stream-kafka

## KafkaStreamFactory

This stream ingests Spans form a Kafka topic advertised by Zookeeper,
using Spark Kafka libraries.

Kafka messages should contain a list of spans in json or TBinaryProtocol
big-endian encoding. Details on message encode is available [here](https://github.com/openzipkin/zipkin/blob/master/zipkin-collector/kafka/README.md#encoding-spans-into-kafka-messages)

## Usage

While the `KafkaStreamFactory` can be used directly through the provided
builder interface, most users will likely find more value in the Spring
Boot autoconfiguraton module.  Additional information for using the
module can be found [here](../../autoconfigure/stream-kafka).
