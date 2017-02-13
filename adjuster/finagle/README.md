# adjuster-finagle

## FinagleAdjuster
This fixes up spans reported by [Finagle](https://github.com/twitter/finagle/tree/develop/finagle-zipkin).

Adjustment code should not be added without a tracking issue either in
[Finagle](https://github.com/twitter/finagle/issues) or [Zipkin Finagle](https://github.com/openzipkin/zipkin-finagle/issues).

## Usage

While the `FinagleAdjuster` can be used directly through the provided
builder interface, most users will likely find more value in the Spring
Boot autoconfiguraton module.  Additional information for using the
module can be found [here](../../autoconfigure/adjuster-finagle).
