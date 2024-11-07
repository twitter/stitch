.. _stitch-concepts:

Stitch Concepts
###############

.. _lazy:

Lazy
****

Most Stitches are lazy. This is an important distinction from Futures.
If you build up a Stitch query but never call `Stitch.run` on it, then it never runs.

Whereas with Futures, you can take an arbitrary future and do `future.ensure(...)`,
and whenever the `future` is finished, the body of `.ensure` will run.
With Stitch, you must be careful to ensure all parts of the computation that you
want to run are part of the Stitch that is passed into `Stitch.run`.

.. code-block:: scala

  val s0 = Stitch.Unit.ensure(println("this will run"))
  val s1 = s0.ensure(println("this won't run"))
  Await.result(Stitch.run(s0))

.. image:: images/LazyDetached.png
  :alt: A graph showing a Stitch with a detached part that never runs

In the above example, `s0` is run but since only `s0` was run and not `s1`,
`sideEffect` never executes. Because of this, it’s important to ensure that the
Stitch that is run contains all computations you want executed.

.. code-block:: scala

  val s0 = Stitch.Unit.ensure(println("this will run"))
  val s1 = s0.ensure(println("this will also run"))
  Await.result(Stitch.run(s1))

.. image:: images/Lazy.png
  :alt: A graph showing a Stitch where all parts are run

While most Stitches are lazily evaluated, :ref:`const` Stitches are evaluated eagerly.
This means that even without a `Stitch.run`, that "`this will run`" will print
in the below example. This comes up when using :ref:`twitter-locals`.

.. code-block:: scala

  val s0 = Stitch.apply(println("this will run"))
  val s1 = s0.ensure(println("this won't run"))



.. _automatic-batching:

Automatic Batching
******************

Stitch will automatically batch calls to the same backend together. This allows Stitch to
enable users to efficiently batch calls and take advantage of batch APIs. Using batch APIs
is often more efficient than making many individual calls to backend services since RPCs
are generally expensive to make and have high overhead, so reducing the number of RPCs but
increasing their size will usually be beneficial.

Stitch automatically batches calls to the same backend together, this means
that whether you talk to 1 or 100 backends, Stitch will handle ensuring that
calls to each backend are batched.

More detail about how automatic batching works is covered in the :ref:`Query Execution section <query-execution>`.

.. _deduplication-non-idempotent-calls:

Deduplication and Non-Idempotent Calls
**************************************

When Stitch batches calls in a :ref:`Group <groups>`, it deduplicates all collected calls using equality.
This is good for idempotent calls where deduplication is ideal but can be a surprise for non-idempotent calls.

.. code-block:: scala

  val globalCounter = new AtomicInteger()
  val globalIncrementGroup = new SeqGroup[Int, Int]{
  override def run(keys: Seq[Int]): Future[Seq[Try[Int]]] = {
    println(s"the batch was: [${keys.mkString(", ")}]")
    Future.value(keys.map(_ => Return(globalCounter.incrementAndGet())))
  }}

  Await.result(Stitch.run(
    Stitch.join(
      Stitch.call(0, globalIncrementGroup),
      Stitch.call(0, globalIncrementGroup),
      Stitch.call(1, globalIncrementGroup),
      Stitch.call(1, globalIncrementGroup)
    )
  )) // result: (1, 1, 2, 2) OR (2, 2, 1, 1)

  globalCounter.get() // result: 2

In the above example, there will be a single batch of size 2 containing the values 0 and 1.
However, this yields unexpected results if it isn't idempotent, this can be worked around
for non-idempotent calls by composing them to avoid this behavior at the expense of losing
batching between these calls.

.. code-block:: scala

  Await.result(Stitch.run(
    Stitch.call(0, globalIncrementGroup)
      .flatMap(v =>
        Stitch.join(
          Stitch.value(v),
          Stitch.call(0, globalIncrementGroup))
  ))) // result: (3, 4) OR (4, 3)

Here we've created a dependency between the first and second call which prevents
them from being batched. This may work for many cases but it won't be ideal since
incorrectly composing a Stitch can result in calls unexpectedly being deduplicated.

The recommended way is to wrap the types so they won't deduplicate, regardless of
how the Stitch is composed. This is done with a wrapper class that doesn't compare
the underlying value when checking for equality, however Stitch will still deduplicate
the wrapper class based on reference equality. For example:

.. code-block:: scala

  class DontDeduplicate[T](val v: T)

  val globalCounter = new AtomicInteger()
  val globalIncrementGroup = new SeqGroup[DontDeduplicate[Int], Int]{
    override def run(
      keys: Seq[DontDeduplicate[Int]]): Future[Seq[Try[Int]]] = {
      println(s"the batch was: [${keys.map(_.v).mkString(", ")}]")
      Future.value(keys.map(_ =>
        Return(globalCounter.incrementAndGet())))
  }
  }

  val refEqual = new DontDeduplicate(0)
  val valEqual = new DontDeduplicate(0)

  Await.result(Stitch.run(
    Stitch.join(
      // reference equality is still deduplicated
      Stitch.call(refEqual, globalIncrementGroup),
      Stitch.call(refEqual, globalIncrementGroup),
      // value equality is no longer deduplicated
      Stitch.call(valEqual, globalIncrementGroup)
    )
  )) // result: (1, 1, 2) OR (2, 2, 1)

The wrapper class adds additional work by allocating these extra objects but will
reliably prevent deduplication while maintaining batching.

Next :ref:`query-execution`
