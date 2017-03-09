# sparkstreaming-job

This is a Spring Boot Application that launches the spark streaming job.
It primarily uses properties for configuration.

## Usage

To start the job, you need to minimally set properties for a stream
(like kafka) and a consumer (like storage). Built-in options are located
in the [autoconfigure](../autoconfigure) module.

Ex. To receive messages from Kafka and store them into Elasticsearch:
```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.storage.type=elasticsearch \
  --zipkin.storage.elasticsearch.hosts=http://127.0.0.1:9200 \
  --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092
```

### Configuration

Configuration properties can be set via commandline parameters, system
properties or any other alternative [supported by Spring Boot](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html).

Here are the relevant setting and a short description. With the exception
of "zipkin.log-level", properties all have a prefix of "zipkin.sparkstreaming"

Property | Default | Description
--- | --- | ---
zipkin.log-level | info | Logging level for the category "zipkin". Set to debug for details.
master | `local[*]` | The spark master used for this job. `local[*]` means run on-demand w/o connecting to a cluster.
jars | the exec jar | Indicates which jars to distribute to the cluster.
conf | "spark.ui.enabled=false" | Overrides the properties used to create a SparkConf
batch-duration | 10000 | The time interval in millis at which streaming data will be divided into batches

Ex. to manually control spark conf, add properties prefixed with `zipkin.sparkstreaming.conf`:
```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.sparkstreaming.conf.spark.eventLog.enabled=false \
  ...
```
