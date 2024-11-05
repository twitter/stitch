.. _why-use-stitch:

Why use Stitch?
###############

Stitch provides a more convenient interface than raw
bulk APIs, while still retaining flexibility and efficient
execution. In particular, parallel queries to the same service are
batched together transparently, so application logic can work with
individual keys but the query may still be executed efficiently.

There is a decent amount of overhead to perform an RPC. This work can be
reduced by batching calls together. This will result in fewer overall RPCs
with each RPC containing multiple arguments.

Stitch permits services to process data in batches in most cases.
Unfortunately, code for managing batching often mixes with and muddles
the business logic in a service, especially when correlating data between
multiple batch interfaces. Stitch lets you code as if operating on single
items, yet executes on batches transparently.

How does this perform compared to plain Futures? Stitch’s runtime
efficiency is comparable to manually batching using Futures when the cost of
RPCs is included in the benchmark so while there may be some performance loss
by using Stitch locally, the benefits in RPC batching generally make it worth it.
Generally Stitch will be able to optimize things that would be hard to
manually optimize, particularly when talking to multiple backends or when
performing fanouts during the computation.

Stitch is used in >200 different projects, in services serving low traffic
to services serving hundreds of millions of operations per second.
Stitch has a proven track record with many services using it in production
for several years.

An even more performant API for Stitch exists called Arrows which can give
greater performance than Futures or plain Stitch in many cases.
However, it is generally more difficult to work with than plain Stitch.

Next :ref:`basic-examples`
