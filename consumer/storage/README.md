# consumer-storage

## StorageConsumer

This consumer writes spans to a Zipkin `StorageComponent`, such as
Elasticsearch.

## Usage

While the `StorageConsumer` can be used directly through the provided
builder interface, most users will likely find more value in the Spring
Boot autoconfiguraton module.  Additional information for using the
module can be found [here](../../autoconfigure/adjuster-finagle).
