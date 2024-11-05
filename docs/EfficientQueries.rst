.. _efficient-queries:

Efficient Queries
#################

The basic rule for writing efficient queries is to minimize
dependencies. Independent queries can be executed concurrently
(reducing latency), and independent queries to the same underlying
service can be batched together into a single RPC call.

Like Futures, Stitch queries may be composed in two ways:

- dependent composition, where one query depends on the result of another.
  Such as flatMap, rescue, and for comprehensions.

- concurrent composition, where queries have no interdependencies.
  Such as join, collect, and traverse

As with Futures, concurrently composed Stitch queries may be executed
concurrently (e.g. concurrent calls to underlying services); moreover,
the execution of a Stitch query can "see" all concurrent queries together,
so they may be batched together and deduplicated.

Concurrent composition is preferable wherever possible
because it offers more scope for optimization.

False data dependencies can prevent a Stitch query from achieving
optimal batching performance. False data dependencies occur when a
later part of a Stitch query falsely depends on an earlier part.

.. code-block:: scala

  fooStitch.flatMap { foo =>
    barStitch.flatMap { bar =>
      query(foo)
      ...
    }
  }

the call to `query(foo)` depends on `barStitch` (because it's
inside the `flatMap` of `barStitch`) but doesn't use the result
`bar`. So the execution of `query(foo)` is unnecessarily delayed
until `barStitch` completes, increasing overall latency and possibly
missing opportunities for batching (Futures have the same pitfall).

.. _avoiding-false-dependencies:

Avoiding false dependencies
***************************

You can rewrite the query above with `Stitch.join` to avoid the
false dependency:

.. code-block:: scala

  Stitch.join(fooStitch, barStitch) flatMap { case (foo, bar) =>
    query(foo)
    ...
  }

The arguments to `Stitch.join` are independent, so in general
sub-queries to the same underlying service will be batched together,
e.g.

.. code-block:: scala

 Stitch.join(tweetService.getTweet(id1), tweetService.getTweet(id2))

The same goes for the sub-queries in `Stitch.traverse` and
`Stitch.collect`, e.g.

.. code-block:: scala

   Stitch.traverse(ids) { tweetService.getTweet(_) }

However the details of batching are service-specific, so even independent
queries may not always be batched. Consult the documentation for the
specific :ref:`Service Adapter <service-adapters>` to be sure.

.. _for-expressions:

For-expressions
===============

Watch out especially for false dependencies in `for`-expressions:

.. code-block:: scala

  for {
    foo <- fooStitch
    bar <- barStitch
    q <- query(foo)
  } ...

which are equivalent to nested `flatMap` s but don't indicate the
nesting visually. You can rewrite without the false dependency as follows:

.. code-block:: scala

  for {
    (foo, bar) <- Stitch.join(fooStitch, barStitch)
    q <- query(foo)
  } ...

Next :ref:`stitchcache`
