.. _arrows:

Arrows
######

Stitch enables high performing code without the need to manually handle batching.
Arrows improve on that further. Arrows are even more performant than Stitch due to
several optimizations. Stitch and Arrows can also interoperate with one another.

Where Stitch is a query graph that gets simplified, Arrows can be thought of as a
graph of functions that get applied to create computation plans. Because of this,
you can create an Arrow once and use it many times. The graph of functions gets
applied to inputs without needing to utilize Futures or Stitches in many cases,
this reduces the overhead by making most Arrow code simple function applications.
Arrows also provide a batch interface that reuses buffers over the life of an
Arrow’s execution.

Arrows are implemented in such a way to minimize allocations,
in many cases this means that rather than allocating objects for each input in a
batch, it can be allocated once and re-used for each input. Between reusing Arrows,
reusing buffers, applying functions directly, and reducing allocations by optimizing
for batches, Arrows achieve significant performance gains over plain Stitch.

If Arrows are so much better, then why don’t we use them? Arrows are composed in a
similar way to how you would compose many functions together into a pipeline,
in order to get the largest benefit out of using Arrows they should be defined
ahead of time and reused many times, when doing this, you don’t have access to
`non-local variables <https://en.wikipedia.org/wiki/Non-local_variable>`_,
so any variables you need will have to be passed around through the input and
output types. This is extremely verbose and tedious, while also obscuring the
meaning of the code, particularly when variables are being passed through because
they are needed elsewhere later in the computation. While this approach is
extremely efficient, it can be difficult to work because you have to think more
like how a compiler works to compose code rather than how you would normally
think to compose Futures.

When using Arrows, a pipeline of Arrows are connected together using `.andThen`.
Each Arrow is `.andThen`’ed to the previous Arrow creating a chain of operations
that can be performed. While Stitches are a `Stitch[T]` where `T` is the output type,
Arrows are `Arrow[T, U]` where `T` is the input and `U` is the output.
Arrows behave like functions where once you have an Arrow defined and built up,
you call `.apply(arg: T)` which will return a `Stitch[U]` which can then be run.
Similarly, you can use the batch interface for Arrows using `.traverse(args: Seq[T])`
which will return a `Stitch[Seq[U]]` which can then be run. The returned Stitches are
normal Stitch objects and can be composed with (and interoperate with) the rest of Stitch.

While Arrows and Stitches can interoperate, to get the greatest performance out of Arrows,
sticking to using Arrows instead of Stitches will yield the greatest performance.
This means that using any of:

- `Arrow.apply(T => Stitch[T])` (which is an Arrow.flatMap)
- `Arrow.flatMap(T => Stitch[T])`
- `Arrow.transform(Try[T] => Stitch[T])`
- `Arrow.rescue(f: PartialFunction[Throwable, Stitch[U]])`
- `.applyArrowToSeq`

or any other Arrow function which a user defined function returns a Stitch.
When an Arrow interoperates with a Stitch (such as in the above examples)
it implies a join point, where all Stitches at that join point must be
completed before the Arrow can continue.
This is useful for interoperating with Stitch but can degrade the performance
over using an Arrow. Arrows, like Stitches, also should be composed to
avoid :ref:`false dependencies <efficient-queries>`.

.. _arrow-locals:

Arrow Locals
************

Arrows also have a form of Locals that can follow the execution of an Arrow.
`Arrow.Locals` are let-scoped values which are propagated and accessible to
Arrows within their scope. Unlike normal Locals which can be accessed anywhere
in their scope, `Arrow.Locals` can only be accessed by composing their accessors
with an Arrow, they cannot simply be called from inside the body of an Arrow.map{ },
for example:

.. code-block:: scala

    val local = new Arrow.Local()
    local.let(0)(Arrow.map{_ => local.apply() ...})

The Local isn’t defined where it’s applied above because it’s not composed correctly.
Instead, accessing a Local should be done in the same way that any other Arrow
would be composed. Locals are a feature of Arrows and do not interoperate with
Stitch, so Arrows that interoperate with Stitches won’t propagate Locals into
the Stitch. Similarly, Stitches won’t propagate locals into Arrows.

.. code-block:: scala

    val local = new Arrow.Local()
    local.let(0)(Arrow.join(Arrow.map(_ => ...), local.apply()))

In the above example, the local is defined where it’s applied. `.apply` returns an Arrow
and by joining, andThen-ing, or otherwise connecting it to another Arrow as you would
do with any other Arrow, you can access the value. Here the result would be whatever
the body of .map returns tupled with `0`, so `(..., 0)`.
