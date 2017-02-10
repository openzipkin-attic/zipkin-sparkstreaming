# adjuster-finagle

## FinagleAdjuster
This fixes up spans reported by [Finagle](https://github.com/twitter/finagle/tree/develop/finagle-zipkin).

Adjustment code should not be added without a tracking issue either in
[Finagle](https://github.com/twitter/finagle/issues) or [Zipkin Finagle](https://github.com/openzipkin/zipkin-finagle/issues).

## Configuration
FinagleAdjuster can be used as a library, where attributes are set via
`FinagleAdjuster.Builder`.

It is more commonly enabled with Spring via autoconfiguration when
"zipkin.sparkstreaming.adjuster.finagle.enabled" is set to "true"

Here are the relevant setting and a short description. Properties all
have a prefix of "zipkin.sparkstreaming.adjuster.finagle"

Attribute | Property | Description | Fix
--- | --- | --- | ---
applyTimestampAndDuration | apply-timestamp-and-duration | Backfill span.timestamp and duration based on annotations. Defaults to true | [Use zipkin-finagle](https://github.com/openzipkin/zipkin-finagle/issues/10)
