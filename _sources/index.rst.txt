Stitch
######

**Stitch** is a Scala library for elegantly and efficiently composing RPC calls to services. It can:

* automatically batch multiple requests to the same data source
* request data from multiple sources concurrently
* support existing service interfaces

Microservices are often riddled with hand-written code for managing bulk data access.
Instead of requiring you to deal with every service's idiosyncratic bulk (or single item) interface,
Stitch allows you to program in terms of single items while it handles batching and concurrency for you.

Stitch will feel familiar to anyone who works with `Futures <https://twitter.github.io/finagle/guide/Futures.html>`_.

Contents
********

The below table of contents is in a good order for reading, each document also links to the next one for convenience.

.. toctree::
   :maxdepth: 3

   Setup
   StitchVsFuture
   WhyUseStitch
   BasicExamples
   FailureHandling
   Groups
   ServiceAdapters
   StitchConcepts
   QueryExecution
   Running
   EfficientQueries
   StitchCache
   TwitterLocals
   Recursion
   Advanced
   Arrows
