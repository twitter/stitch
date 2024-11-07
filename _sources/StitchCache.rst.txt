.. _stitchcache:

StitchCache
###########

StitchCache is a cache for storing Stitches.
It allows caching a Stitch query and using the result multiple times.
StitchCache provides a basic caching API, similar to FutureCache.
However, since Stitches are different from Futures, StitchCache is also different from FutureCache.

StitchCache has a `get`, `getOrElseUpdate`, `set`, `evict`, and `size` methods. `get`,
`getOrElseUpdate`, and `set` all return Stitches, the returned Stitch is what is stored in the cache
and may be used for eviction (except for `ValueCache`). Unlike FutureCache, where the passed in Future
can be used to evict, only the Stitch returned by one of these methods can be used to evict.

Stitches passed into a StitchCache become owned by the cache, and Stitches returned from it are owned by the caller.
This means only the owner of the Stitch may run the Stitch, so you must never run the Stitch that is passed into
the cache (not before it’s passed in, not after it’s passed in, never! see :ref:`never-re-runnable`)
since it is owned by the cache. Instead, run the Stitch returned by `get`, `getOrElseUpdate`, or `set`.
The Stitch returned by the cache is safe to run since it’s owned by the caller.
However, the general rules of Stitches still apply, you must :ref:`never rerun <never-re-runnable>`
Stitches returned by the cache, and instead must query the cache for a new Stitch each time.

.. code-block:: scala

  val count = new AtomicInteger()

  val g = new SeqGroup[Int, Int]{
  def run(keys: Seq[Int]): Future[Seq[Try[Int]]] = {
    keys.foreach(_ => count.incrementAndGet())
    Future.value(keys.map(i => Return(i + 1)))
  }}

  val cache: Int => Stitch[Int] =
      StitchCache.fromMap[Int, Int](
          arg => Stitch.call(arg, g),
          new ConcurrentHashMap())

  Await.result(Stitch.run(cache(0))) // result: 1
  Await.result(Stitch.run(cache(0))) // result: 1
  count.get() // result: 1

The first time this is called with `0`, it will make the call to `g`.
This would be as if there is no cache in place. However, for the second one,
it would access the cached value and it wouldn’t need to call `g` at all.

StitchCache also has the ability to deduplicate requests across calls to Stitch.run.
The below example illustrates this by concurrently calling the same key from the
cache in multiple `Stitch.run` calls but the underlying request is only ever sent once.

.. code-block:: scala

  implicit val timer: Timer = new JavaTimer()

  val count = new AtomicInteger()

  val g = new SeqGroup[Int, Int]{
  def run(keys: Seq[Int]): Future[Seq[Try[Int]]] = {
    keys.foreach(_ => count.incrementAndGet())
    // sleep to simulate an RPC, though this isn't really necessary
    Future.sleep(Duration.fromMilliseconds(10))
      .before(Future.value(keys.map(i => Return(i + 1))))
  }}

  val cache: Int => Stitch[Int] =
      StitchCache.fromMap[Int, Int](
          arg => Stitch.call(arg, g),
          new ConcurrentHashMap())

  val concurrentlyRunning = Range(0, 100).map(_ =>
    FuturePool.unboundedPool(Stitch.run(cache(0))).flatten)

  // paste the below in separately due to an issue with the repl

  Await.result(
    Future.collect(concurrentlyRunning) // result: Seq(1, 1, 1, ...)
  )
  count.get() // result: 1

Next :ref:`twitter-locals`
