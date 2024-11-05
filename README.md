# Stitch

**Stitch** is a Scala library for elegantly and efficiently composing RPC calls to services. It can:

* automatically batch multiple requests to the same data source
* request data from multiple sources concurrently
* support existing service interfaces

Microservices are often riddled with hand-written code for managing bulk data access. Instead of requiring you to deal with every service's idiosyncratic bulk (or single item) interface, Stitch allows you to program in terms of single items while it handles batching and concurrency for you.

Stitch will feel familiar to anyone who works with [Futures](https://twitter.github.io/finagle/guide/Futures.html) or promises.

## Status

This project is used in production at X.

## Getting started

We are not currently publishing builds of this library, but the repository can be cloned and built locally.

### Developing Stitch

First clone the repository.
Then run the `sbt` wrapper script, which will install [sbt](https://www.scala-sbt.org/) and the appropriate Scala version.

You can now run tests with `./sbt test`, or build a binary with `./sbt assembly`.


## Getting involved

* Website: https://twitter.github.io/stitch
* Source: https://github.com/twitter/stitch
* API reference: https://twitter.github.io/stitch/docs/com/twitter/stitch

Documentation improvements are always welcome, so please send patches
our way.

## Contributing

See [CONTRIBUTING.md](https://github.com/twitter/stitch/blob/master/CONTRIBUTING.md) for more details about how to contribute.
