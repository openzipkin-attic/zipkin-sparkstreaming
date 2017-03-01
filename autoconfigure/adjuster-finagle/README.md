# autoconfigure-adjuster-finagle

## ZipkinFinagleAdjusterAutoConfiguration

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html) 
module built into the [Spark Streaming Job](../../sparkstreaming-job) 
which fixes up spans reported by Finagle applications.  Internally, this
module wraps the [FinagleAdjuster](../../adjuster/finagle) to expose
configuration options via properties.

## Usage

In order to connect, you minimally need to set
`zipkin.sparkstreaming.adjuster.finagle.enabled` to true.

Ex. to enable Finagle adjustment

```bash
java -jar zipkin-sparkstreaming-job.jar \
  --zipkin.sparkstreaming.adjuster.finagle.enabled=true \
  --zipkin.storage.type=elasticsearch \
  --zipkin.storage.elasticsearch.hosts=http://127.0.0.1:9200 \
  --zipkin.sparkstreaming.stream.kafka.bootstrap-servers=127.0.0.1:9092
```

### Configuration

Configuration properties can be set via commandline parameters, system
properties or any other alternative [supported by Spring Boot](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html).

Here are the relevant setting and a short description. Properties all
have a prefix of "zipkin.sparkstreaming.adjuster.finagle"

Property | Default | Description | Fix
--- | --- | --- | ---
apply-timestamp-and-duration | true | Backfill span.timestamp and duration based on annotations. | [Use zipkin-finagle](https://github.com/openzipkin/zipkin-finagle/issues/10)
adjust-issue343 | false | Drops "finagle.flush" annotation, to rectify [finagle memcached bug](https://github.com/twitter/finagle/issues/343). | Use finagle version 6.36.0 or higher
