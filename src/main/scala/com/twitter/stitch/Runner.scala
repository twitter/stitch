package com.twitter.stitch

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.util._
import scala.collection.mutable
import scala.util.control.NonFatal

/** A Runner collects calls on a particular external service to be
 * executed in a batch. (This class should be used only by service
 * adaptor implementations, and in most cases
 * [[com.twitter.stitch.SeqRunner]] or
 * [[com.twitter.stitch.MapRunner]] may be used rather than
 * implementing a Runner from scratch.)
 *
 * Each group of calls has its own runner, and a runner instance is
 * used for only one batch.
 *
 * All access to a runner is single-threaded in the framework.
 */
sealed trait Runner[C, T] {

  /** Adds a pending call, returns a Stitch[T] which simplifies to the
   * result of the call once the call is complete. (Typically the
   * returned Stitch has an underlying `Stitch.future` representing
   * the batched call, with a function mapped over it to extract the
   * result for just this call.)
   */
  def add(call: C): Stitch[T]
}

object Runner {
  /* Memoizes the seq application function for each `ith` position. */
  private[stitch] val ith = Memoize[Int, Seq[Any] => Any] { i => { s => s.apply(i) } }
}

trait FutureRunner[C, T] extends Runner[C, T] {

  /** Issues batched call, returns a Future which completes when the
   * batch completes.
   *
   * It's OK for run() to make multiple calls internally and collect
   * them into a single Future.
   *
   * It's important that the returned Future be
   * "outermost", i.e. satisfied only when all the Futures returned
   * via add() are satisfied. Otherwise we might run the next phase
   * too early and miss available calls.
   */
  def run(): Future[_]
}

trait ArrowFutureRunner[C, T] extends FutureRunner[C, T] {
  def addSeqTry(cs: Seq[Try[C]]): Future[C => Try[T]]
}

/** Runner which executes each call individually, just for testing.
 */
trait IndividualRunner[C, T] extends FutureRunner[C, T] {
  private val calls = new mutable.ArrayBuffer[(C, Promise[T])]()

  def add(c: C): Stitch[T] = {
    val p = Promise[T]()
    calls += ((c, p))
    Stitch.future(p)
  }

  def run(): Future[Unit] =
    Future.join(calls map {
      case (c, p) =>
        p.become(try { run(c) }
        catch { case NonFatal(e) => Future.exception(e) })
        p
    })

  protected def run(c: C): Future[T]
}

/** Runner for calls which take a Seq of keys in arbitrary order and
 * return a function mapping keys to results.
 *
 * Key order is preserved, but keys are deduplicated.
 */
abstract class MapRunner[K, V](label: Any = null) extends ArrowFutureRunner[K, V] {
  private val keySet = new java.util.LinkedHashSet[K]()
  private val p = Promise[K => Try[V]]()
  private val s = Stitch.future(p, label)

  def add(key: K): Stitch[V] = {
    keySet.add(key)
    s.apply(key)
  }

  def addSeqTry(keys: Seq[Try[K]]): Promise[K => Try[V]] = {
    val it = keys.iterator
    while (it.hasNext) {
      it.next() match {
        case Return(k) => keySet.add(k)
        case _ =>
      }
    }
    p
  }

  def run(): Promise[K => Try[V]] = {
    val keys = {
      val iterator = keySet.iterator
      val seq = new mutable.ArrayBuffer[K](keySet.size)
      while (iterator.hasNext) seq += iterator.next()
      seq
    }
    val max = maxSize
    val f =
      try {
        if (keys.size <= max) {
          run(keys)

        } else {
          // i.e. servo.repository.ChunkingStrategy.equalSize
          val chunkCount = math.ceil(keys.size / max.toDouble)
          val chunkSize = math.ceil(keys.size / chunkCount).toInt

          val keyChunks = keys.grouped(chunkSize)

          MapRunner.runBatches(run, keyChunks, maxConcurrency)
        }
      } catch { case NonFatal(e) => Future.exception(e) }

    p.become(f)
    p
  }

  protected def maxSize: Int = Int.MaxValue
  protected def maxConcurrency: Int = Int.MaxValue

  protected def run(calls: Seq[K]): Future[K => Try[V]]

}

object MapRunner {

  /** Given a `run` function, `keyChunks`, and `maxConcurrency`,
   *  calls `run` with each element of `keyChunks` with at most
   *  `maxConcurrency` calls in flight at a time
   *
   * @tparam K key type
   * @tparam V value type
   * @return a future of a function from K => Try[V] which completes after all keyChunks have been run
   */
  @inline def runBatches[K, V](
    run: Seq[K] => Future[K => Try[V]],
    keyChunks: Iterator[mutable.ArrayBuffer[K]],
    maxConcurrency: Int
  ): Future[K => Try[V]] = {
    val indexByKey = new mutable.HashMap[K, Int]

    var i = 0
    val fs = if (maxConcurrency == Int.MaxValue) {
      // avoid overhead from [[AsyncSemaphore]] when the `maxConcurrency` is unbounded
      keyChunks.map { keys =>
        for (key <- keys) {
          indexByKey.put(key, i)
        }
        i += 1
        run(keys)
      }.toSeq
    } else {
      val semaphore = new AsyncSemaphore(maxConcurrency)
      keyChunks.map { keys =>
        for (key <- keys) {
          indexByKey.put(key, i)
        }
        i += 1
        semaphore.acquireAndRun(run(keys))
      }.toSeq
    }

    Future
      .collectToTry(fs)
      .map(fns => {
        key: K => fns(indexByKey(key)).flatMap(_.apply(key))
      })
  }
}

/** Runner for calls which take a Seq of keys in arbitrary order and
 *  return a function mapping keys to results with the batches specified by `bucket`
 *
 *  Keys are deduplicated
 *
 *  BucketedMapRunner works similarly to [[MapRunner]] except that it allows for the user to have
 *  greater control over how batching works.
 *
 *  Using the provided `bucket` function, keys that fall into the same bucket are batched together.
 *  If a bucket is larger than the `maxsize` then the bucket is broken into batches of `maxSize`
 *  or smaller.
 *
 *  A [[BatchPackingStrategy]] is provided to define how to pack the incomplete batches together
 *
 * @tparam K is the key type
 * @tparam V is the returned value type
 * @tparam B is the bucket type, it must define both `.equals()` and `.hashcode()`
 */
abstract class BucketedMapRunner[K, V, B](label: Any = null) extends ArrowFutureRunner[K, V] {
  // keyBuckets accumulates keys in each bucket before they are added to a batch
  private[this] val keyBuckets = mutable.HashMap[B, java.util.HashSet[K]]()
  // count of unique keys in all keyBuckets, this is populated at add time
  private[this] var keyCount = 0

  private[this] val p = Promise[K => Try[V]]()
  private[this] val s = Stitch.future(p, label)

  /** `bucket` is a function which will determine which bucket a given key will be batched with.
   *  `bucket` must always return the same bucket when given the same key
   *  `bucket` must not return `null`
   *  `B` must define both `.equals()` and `.hashcode()`
   *
   *  If `bucket` throws then the keys that were added will fail immediately with the thrown exception
   */
  def bucket(key: K): B

  /** If `bucket` throws then the key that was supposed to be added
   * is not added and instead a `Stitch.exception` is returned
   */
  def add(key: K): Stitch[V] = try {
    val keySet = keyBuckets.getOrElseUpdate(
      // if this throws then a failed stitch is returned and the key is not added to the batch
      bucket(key),
      new java.util.HashSet[K]()
    )
    if (keySet.add(key)) { // true if key was added
      keyCount += 1 // if added, increment keyCount
    }
    s.apply(key)
  } catch {
    case NonFatal(e) => Stitch.exception(e)
  }

  def addSeqTry(keys: Seq[Try[K]]): Promise[K => Try[V]] = {
    keys.foreach {
      case Return(k) => add(k)
      case _ =>
    }
    p
  }

  /** run optimizes the batches by packing the keys into batches based on their buckets
   *
   *  if keyCount <= maxSize then we can skip the packing and just make a single batch
   *  otherwise `packBatches`
   */
  def run(): Promise[K => Try[V]] = {
    val max = maxSize
    val f =
      try {
        if (keyCount <= max) { // since size <= maxSize there is at most 1 batch
          val batch = new mutable.ArrayBuffer[K](keyCount)
          keyBuckets.foreach { case (_, bucket) => bucket.forEach(k => batch += k) }
          run(batch)
        } else {
          val batches = BatchPacking.packBatches(keyCount, keyBuckets, max, packingStrategy)
          val keyChunks = batches.iterator
          MapRunner.runBatches(run, keyChunks, maxConcurrency)
        }
      } catch {
        case NonFatal(e) => Future.exception(e)
      }

    p.become(f)
    p
  }

  protected def maxSize: Int = Int.MaxValue
  protected def maxConcurrency: Int = Int.MaxValue

  /** specifies how to pack incomplete batches  */
  protected def packingStrategy: BatchPackingStrategy

  protected def run(calls: Seq[K]): Future[K => Try[V]]

}

object SeqRunner {

  /** convert the result of a Seq run, `Future[Seq[V]]`,
   *  to the result of a Map run `Future[K => Try[V]]`
   */
  @inline
  private[stitch] def toMap[K, V](
    keys: Seq[K],
    values: Future[Seq[Try[V]]]
  ): Future[K => Try[V]] = {
    values.map { vals =>
      if (vals.size != keys.size) {
        throw new RuntimeException(s"expected ${keys.size} items, got ${vals.size} items")
      } else if (keys.size == 1) { (_: K) =>
        vals.head // _ since there's only 1 key so there's no need to check it
      } else {
        keys.iterator.zip(vals.iterator).toMap
      }
    }
  }
}

/** Runner for calls which take and return a Seq of keys / values.
 *
 *  The size of the input keys seq and output values seq must be equal
 *
 *  This is essentially a thin wrapper around a [[MapRunner]]
 *  that converts the results of an endpoint that returns a Seq
 *  to a Map for use in [[MapRunner]].
 *
 *  Keys are deduplicated under the hood.
 *
 * ex. the input keys `(k1,k1,k2)` will produce a
 *     batched call of `(k1,k2)` resulting in `(v1,v2)`
 *     which will return `(v1, v2, v2)`
 */
abstract class SeqRunner[K, V](label: Any = null) extends ArrowFutureRunner[K, V] { self =>
  // doesn't compile as an anonymous class
  class SeqMapRunner extends MapRunner[K, V](label) {
    def run(keys: Seq[K]): Future[K => Try[V]] =
      SeqRunner.toMap(keys, self.run(keys)) // convert Seq result to Map result
    override def maxSize: Int = self.maxSize
    override def maxConcurrency: Int = self.maxConcurrency
  }

  private val runner = new SeqMapRunner
  def add(key: K): Stitch[V] = runner.add(key)
  def run(): Promise[K => Try[V]] = runner.run()
  def addSeqTry(keys: Seq[Try[K]]): Promise[K => Try[V]] = runner.addSeqTry(keys)
  protected def maxSize: Int = Int.MaxValue
  protected def maxConcurrency: Int = Int.MaxValue

  protected def run(keys: Seq[K]): Future[Seq[Try[V]]]
}

/** Runner for calls which take and return a Seq of keys / values
 *  but batches are created using the provided bucket function and
 *  packing strategy, see [[BucketedMapRunner]] for more details.
 *
 *  The size of the input keys and output values seq must be equal
 *
 *  This is essentially a thin wrapper around a [[BucketedMapRunner]]
 *  that converts the results of an endpoint that returns a Seq
 *  to a Map for use in [[BucketedMapRunner]].
 *
 *  Keys are deduplicated under the hood.
 */
abstract class BucketedSeqRunner[K, V, B](label: Any = null) extends ArrowFutureRunner[K, V] {
  self =>
  // doesn't compile as an anonymous class
  class SeqMapRunner extends BucketedMapRunner[K, V, B](label) {
    def run(keys: Seq[K]): Future[K => Try[V]] =
      SeqRunner.toMap(keys, self.run(keys)) // convert Seq result to Map result
    override def maxSize: Int = self.maxSize
    override def maxConcurrency: Int = self.maxConcurrency
    override def bucket(key: K): B = self.bucket(key)
    override def packingStrategy: BatchPackingStrategy = self.packingStrategy
  }

  private val runner = new SeqMapRunner
  def add(key: K): Stitch[V] = runner.add(key)
  def run(): Promise[K => Try[V]] = runner.run()
  def addSeqTry(keys: Seq[Try[K]]): Promise[K => Try[V]] = runner.addSeqTry(keys)
  protected def maxSize: Int = Int.MaxValue
  protected def maxConcurrency: Int = Int.MaxValue
  protected def bucket(key: K): B
  protected def packingStrategy: BatchPackingStrategy
  protected def run(keys: Seq[K]): Future[Seq[Try[V]]]
}

trait StitchRunner[C, T] extends Runner[C, T] {
  def run(): Unit
}

abstract class StitchSeqRunner[K, V] extends StitchRunner[K, V] { self =>
  private val keys = new mutable.ArrayBuffer[K]()
  private val stitchesByKey = new java.util.HashMap[K, Stitch[V]]

  private val s = Stitch.Ref(Stitch.Incomplete[Seq[V]])

  def add(key: K): Stitch[V] = {
    var stitch = stitchesByKey.get(key)
    if (stitch == null) {
      val i = keys.size
      keys += key
      stitch = s.map(Runner.ith(i).asInstanceOf[Seq[V] => V])
      stitchesByKey.put(key, stitch)
    }
    stitch
  }

  def run(): Unit = {
    s.current = run(keys)
  }

  protected def run(calls: Seq[K]): Stitch[Seq[V]]
}
