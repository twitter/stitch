package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import com.twitter.util.{Duration, Time}
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

/** A single-value asynchronous cache with TTL.  The provider supplies the Stitch when invoked
 * and this class ensures that the provider is not invoked more frequently than the TTL.  If the
 * Stitch fails, that failure is not cached. If more than one value is required, a StitchCache
 * may be more appropriate.
 *
 * This is useful in situations where a call to an external service returns a value that changes
 * infrequently and we need to access that value often, for example asking a service for a list of
 * features that it supports.
 *
 * A non-memoized function like this:
 * {{{
 *   def getData(): Stitch[T] = { ... }
 * }}}
 * can be memoized with a TTL of 1 hour as follows:
 * {{{
 *   import com.twitter.conversions.DurationOps._
 *   import com.twitter.stitch.cache.Refresh
 *   val getData: () => Stitch[T] = Refresh.every(1.hour) { ... }
 * }}}
 */
object Refresh {

  private val empty = (Stitch.Never, Time.Bottom)

  /** Create a memoized Stitch with TTL
   *
   * From Scala:{{{
   *   Refresh.every(1.hour) { ... }
   * }}}
   *
   * From Java: {{{
   *   Refresh.every(Duration.fromSeconds(3600), new Function0<Stitch<T>>() {
   *     @Override
   *     public Stitch<T> apply() {
   *       ...
   *     }
   *   });
   * }}}
   *
   * @param ttl The amount of time between refreshes
   * @param provider A provider function that returns a Stitch query to refresh
   * @return A memoized version of `operation` that will refresh on invocation
   *         if more than `ttl` time has passed since `operation` was last called.
   */
  def every[T](ttl: Duration)(provider: => Stitch[T]): () => Stitch[T] = {
    val emptyT = empty.asInstanceOf[(Stitch[T], Time)]
    val ref = new AtomicReference[(Stitch[T], Time)](emptyT)

    @tailrec def result(): Stitch[T] = ref.get match {
      case tuple @ (cachedValue, lastRetrieved) =>
        val now = Time.now
        if (now < lastRetrieved + ttl)
          cachedValue
        else {
          var nextTuple: (Stitch[T], Time) = null
          val nextStitch: Stitch[T] =
            Stitch.synchronizedRef(
              // since Stitches are lazy, `provider` will only run if the Stitch is run
              Stitch.Unit.before(provider).onFailure(_ => ref.compareAndSet(nextTuple, emptyT)))
          nextTuple = (nextStitch, now)
          if (ref.compareAndSet(tuple, nextTuple)) {
            // successfully updated the saved value
            nextStitch
          } else {
            // value changed, so try again and discard the Stitch we created
            result()
          }
        }
    }
    // Return result, which is a no-arg function that returns a Stitch
    result _
  }
}
