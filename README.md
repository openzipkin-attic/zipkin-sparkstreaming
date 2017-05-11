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

The quickest way to get started is to fetch the [latest released job](https://search.maven.org/remote_content?g=io.zipkin.sparkstreaming&a=zipkin-sparkstreaming-job&v=LATEST) as a self-contained executable jar. Note that the Zipkin Spark Streaming Job requires minimum JRE 7. For example:

### Download the latest job
The following downloads the latest version using wget:

```bash
wget -O zipkin-sparkstreaming-job.jar 'https://search.maven.org/remote_content?g=io.zipkin.sparkstreaming&a=zipkin-sparkstreaming-job&v=LATEST'
```

### Run the job
You can either run the job in local or cluster mode. Here's an example of each:

```bash
# run local
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.log-level=debug \
  --zipkin.storage.type=elasticsearch \
  --zipkin.storage.elasticsearch.hosts=http://127.0.0.1:9200 \
  --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092
# run in a cluster
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.log-level=debug \
  --zipkin.storage.type=elasticsearch \
  --zipkin.storage.elasticsearch.hosts=http://127.0.0.1:9200 \
  --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092 \
  --zipkin.sparkstreaming.master=spark://127.0.0.1:7077
```

## Key Components

The image below shows the internal architecture of zipkin spark streaming job. StreamFactory is a extensible interface that ingests data from Kafka or any other transport. The filtering step filters spans based on criteria like service name([#33](https://github.com/openzipkin/zipkin-sparkstreaming/issues/33)). The aggregation phase groups the spans by time or trace ID. The adjuster phase is useful for making adjustments to spans that belong to the same trace. For example, the FinagleAdjuster fixes known bugs in the old finagle zipkin tracer. The final consumer stage persists the data to a storage system like ElasticSearch service. 

                                    
                                            ┌────────────────────────────┐   
                                            │           Kafka            │   
                                            └────────────────────────────┘   
                                          ┌────────────────┼────────────────┐
                                          │                ▼                │
                                          │  ┌────────────────────────────┐ │
                                          │  │       StreamFactory        │ │
                                          │  └────────────────────────────┘ │
                                          │                 │               │
                                          │                 ▼               │
                                          │  ┌────────────────────────────┐ │
                                          │  │         Filtering          │ │
                                          │  └────────────────────────────┘ │
                                          │                 │               │
                                          │                 ▼               │
                                          │  ┌────────────────────────────┐ │
                                          │  │        Aggregation         │ │
                                          │  └────────────────────────────┘ │
                                          │                 │               │
                                          │                 ▼               │
                                          │  ┌────────────────────────────┐ │
                                          │  │          Adjuster          │ │
                                          │  └────────────────────────────┘ │
                                          │                 │               │
                                          │                 ▼               │
                                          │  ┌────────────────────────────┐ │
                                          │  │          Consumer          │ │
                                          │  └────────────────────────────┘ │
                                          └─────────────────┼───────────────┘
                                                            ▼                
                                               ┌──────────────────────────┐  
                                               │ Storage (ES, Cassandra)  │  
                                               └──────────────────────────┘  
                                               
### Stream
A stream is a source of json or thrift encoded span messages.

For example, a message stream could be a Kafka topic named "zipkin"

Stream | Description
--- | ---
[Kafka](./stream/kafka) | Ingests spans from a Kafka topic.

### Adjuster
An adjuster conditionally changes spans sharing the same trace ID.

You can make adjusters to fixup data reported by instrumentation, or to
scrub private data. This [example](https://github.com/openzipkin/zipkin-sparkstreaming-example) shows how to add a custom adjuster to the spark job.

Below is the list of prepackaged adjusters.

Adjuster | Description
--- | ---
[Finagle](./adjuster/finagle) | Fixes up spans reported by [Finagle](https://github.com/twitter/finagle/tree/develop/finagle-zipkin).

### Consumer
A consumer is an end-recipient of potentially adjusted spans sharing the
same trace ID.

This could be a Zipkin storage component, like Elasticsearch, or another
sink, such as a streaming visualization tool.

Consumer | Description
--- | ---
[Storage](./consumer/storage) | Writes spans to a Zipkin Storage Component
