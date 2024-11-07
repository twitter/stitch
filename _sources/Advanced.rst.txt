.. _advanced:

Advanced
########

The topics covered in this doc are generally beyond the scope of what is needed to utilize Stitch on a day-to-day basis.

.. _ordering:

Ordering
********

Stitch has no ordering guarantees about which keys will end up in which batches to backends.
What that means is that there’s no way to know ahead of time which keys will be with which
other keys in a given RPC. Because the order in which collected calls isn't guaranteed, it's
important to compose your Stitch query so that dependent operations are truly dependent.
If `x` must happen after `y` then they should be composed to ensure that, such as `x.flatMap(y)`
and not depend on what order you think will occur, such as `Stitch.join(x, y)` which has no
guarantee that parts of `x` will run before parts of `y`.

.. code-block:: scala

  val g = new SeqGroup[Int, Int]{
  def run(keys: Seq[Int]): Future[Seq[Try[Int]]] = {
      println(keys)
      Future.value(keys.map(Return(_)))
  }
  override def maxSize = 10
  }
  val s = Stitch.traverse(0 to 99)(v => Stitch.call(v, g))
  Await.result(Stitch.run(s)) // result: Seq(0,1,2,...,97,98,99)

In the above example, an initial thought may be that the batches would be [0-9, 10-19, … 90-99]
however there’s not likely to be the case in the real world. The batches should be assumed to be
random and are just as likely to be [[11, 19, 95, 41, 45, 25, 83, 34, 20, 6], …]. This is due to
keys being stored in a `Set <https://docs.oracle.com/javase/10/docs/api/java/util/HashSet.html>`_
with no guarantees about ordering, and second is that the order in which keys are aggregated
during simplification is not consistent for non-trivial Stitches.

While the key ordering that your backend receives may be essentially random, the ordering that
you get back will be correct. In the above example, even though the inputs are run in arbitrary
batches with no ordering, you will still get back the results in the same order in which they
were passed in.

The below example adds a brief random delay to simulate previous calls finishing at different
times, so the calls to g aren't in a consistent order. By running the below snippet you can
see that the batches are random but the final result is in the correct order.

.. code-block:: scala

  implicit val timer: Timer = new JavaTimer()

  val g = new SeqGroup[Int, Int]{
  def run(keys: Seq[Int]): Future[Seq[Try[Int]]] = {
      println(keys)
      Future.value(keys.map(Return(_)))
  }
  override def maxSize = 10
  }

  val s = Stitch.traverse(0 to 99){v =>
      Stitch.sleep(
          Duration.fromMilliseconds(scala.util.Random.nextInt % 10)
      ).flatMap(_ => Stitch.call(v, g))
  }

  // paste the below in separately due to an issue with the repl

  Await.result(Stitch.run(s)) // result: Seq(0,1,2,...,97,98,99)

You may have noticed that there were likely more than 10 batches, that's because when each
`sleep` finishes, it simplifies the graph, so any keys whose sleep is also finished at that
moment will also be able to be batched, due to small timing differences things aren't perfect,
but it illustrates the point.

.. _bucketed-groups:

Bucketed Groups
***************

In most cases, a simple `MapGroup` or `SeqGroup` are all that will be needed though Stitch
allows for some additional options. For some services, it may be beneficial to keep certain
calls together in the same RPC. If it’s important to keep certain keys together then the
`BucketedSeqGroup` or `BucketedMapGroup` can be used instead.
This allows the implementer to define

.. code-block:: scala

  def bucket(key: Key): B

which will try to put keys in the same bucket together in the same RPC. By default this strategy will split
small buckets to optimize the number of RPCs that are performed, however this is configurable with

.. code-block:: scala

  def packingStrategy: BatchPackingStrategy

.. _runners:

Runners
*******

`Runners` are what power `Groups`. They are pretty deep into the Stitch implementation and
unlikely to ever be touched by people outside of the Stitch maintainers.
However, there are some interesting things that `Runners` do.

Users often will use Groups, but for the most part, a Group delegates almost everything to the Runner.
Runners are responsible for which keys end up in which batches, creating the correct batch sizes,
maintaining the concurrency limits, and invoking the user defined `run` method.

There are several runners that are provided as part of Stitch, `MapRunner` and `SeqRunner` which
were discussed previously in the :ref:`groups` section. There's also a `BucketedMapRunner` and a
`BucketedSeqRunner` which behave as described in the :ref:`bucketed-groups` section.
There's also a non-batched `FutureRunner` which is what powers :ref:`callfuture`

Next :ref:`arrows`
