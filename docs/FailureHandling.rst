.. _failure-handling:

Failure handling
################

Stitch provides `handle` and `rescue` methods on queries (as with
`Futures <https://twitter.github.io/finagle/guide/Futures.html>`_).
Typically result objects in the service API will be converted to
either a successful result or an exception by the service API,
so it's important to handle exceptions, in particular
`stitch.NotFound` for keys which aren't found.

Just as with Futures, `map` and `flatMap` functions are applied
only for successful results; exceptions are passed through. Operations
which combine more than one query (`join`, `traverse`,
`collect`) fail if any of the underlying queries fail (again just as
with Futures).

Here's an example of recovering from a failure by filtering it out:

.. code-block:: scala

  def failEvenNumbers(i: Int): Stitch[Option[Int]] = i match {
    case i if i % 2 == 0 => Stitch.exception(new IllegalArgumentException)
    case i => Stitch.value(Some(i))
  }

  val s = Stitch.traverse(Seq(1, 2, 3, 4)) { i =>
      failEvenNumbers(i)
        .handle { case _: IllegalArgumentException => None }
  }.map(_.flatten)

  Await.result(Stitch.run(s))

Successful results are wrapped in `Some`; failures become `None`;
and we flatten the resulting `Seq[Option[Int]]` into a
`Seq[Int]` containing only the successful results.

.. code-block:: scala

  def failEvenNumbers(i: Int): Stitch[Int] = i match {
    case i if i % 2 == 0 => Stitch.exception(new IllegalArgumentException)
    case i => Stitch.value(i)
  }

  val s = Stitch.traverse(Seq(1, 2, 3, 4)) { i =>
        failEvenNumbers(i).liftToOption()
    }.map(_.flatten)

  Await.result(Stitch.run(s))

We do the same thing again but with `liftToOption` instead which will lift
successful results into `Some(_)` and return `None` for exceptions.

.. code-block:: scala

  val s = Stitch.traverse(Seq(1, 2, 3, 4)) { i =>
        failEvenNumbers(i).liftToTry
    }

  Await.result(Stitch.run(s))

If we want to examine the exceptions we can also do `liftToTry` to get back a `Stitch[Try[Int]]`.

Next: :ref:`groups`
