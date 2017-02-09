[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin) [![Build Status](https://circleci.com/gh/openzipkin/zipkin-azure.svg?style=svg)](https://circleci.com/gh/openzipkin/zipkin-sparkstreaming) [![Download](https://api.bintray.com/packages/openzipkin/maven/zipkin-sparkstreaming/images/download.svg) ](https://bintray.com/openzipkin/maven/zipkin-sparkstreaming/_latestVersion)

# zipkin-sparkstreaming
This is a streaming alternative to Zipkin's collector.

Zipkin's collector receives span messages reported by applications, or
via Kafka. It does very little besides storing them for later query, and
there are limited options for downsampling or otherwise.

This project provides a more flexible pipeline, including the ability to
* receive spans from other sources, like files
* perform dynamic sampling, like retain only latent or error traces
* process data in real-time, like reporting or alternate visualization tools
* adjust data, like scrubbing private data or normalizing service names

## Status
Many features are incomplete. Please join us to help complete them.

## Usage
You have to build locally right now. We'll improve this section

```bash
# make the jar
./mvnw clean install
# run local
java -jar ./sparkstreaming-job/target/zipkin-sparkstreaming-job-*.jar
# run remote
java -jar ./sparkstreaming-job/target/zipkin-sparkstreaming-job-*.jar --zipkin.sparkstreaming.sparkMaster=spark://acole:7077
```

## Key Components

### Stream
A stream is a source of json or thrift encoded span messages.

For example, a message stream could be a Kafka topic named "zipkin"

TODO: briefly describe and link to built-in message stream factories

### Adjuster
An adjuster conditionally changes spans sharing the same trace ID.

You can make adjusters to fixup data reported by instrumentation, or to
scrub private data.

TODO: briefly describe and link to built-in adjusters

### Consumer
A consumer is an end-recipient of potentially adjusted traces.

This could be a Zipkin storage component, like Elasticsearch, or another
sink, such as a streaming visualization tool.

TODO: briefly describe and link to built-in consumers
