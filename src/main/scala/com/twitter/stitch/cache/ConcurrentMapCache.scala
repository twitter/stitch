package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import com.twitter.stitch.Stitch.{Const, SynchronizedRef}
import java.util.concurrent.ConcurrentMap

/**
 * A [[StitchCache]] backed by a [[ConcurrentMap]]
 *
 * [[ConcurrentMapCache]] guarantees that the passed in Stitches are in
 * the correct form before being stored in the underlying [[ConcurrentMap]].
 *
 * [[ConcurrentMapCache]] only handles ensuring the form of the Stitch is
 * correct, but does not handle eviction.
 *
 * This does not handle evicting failed Stitches from the cache.
 * To build a correct [[StitchCache]] this should be wrapped in an
 * [[EvictingCache]] or [[LazilyEvictingCache]].
 *
 * A reference implementation for caching the results of a query with a
 * [[ConcurrentMap]] can be found at [[StitchCache.fromMap]].
 *
 *
 * @param underlying is a [[ConcurrentMap]] that will be used to store Stitches
 * @return a plain [[StitchCache]] that does not handle evicting failed Stitches
 */
class ConcurrentMapCache[K, V](val underlying: ConcurrentMap[K, Stitch[V]])
    extends StitchCache[K, V] {

  def get(key: K): Option[Stitch[V]] = Option(underlying.get(key))

  def set(key: K, value: Stitch[V]): Stitch[V] = {
    val v = value match {
      // already in the correct form and are thread-safe
      case Const(_) | SynchronizedRef(_) => value

      // not thread-safe so wrap it so it is
      case value => Stitch.synchronizedRef(value)
    }
    underlying.put(key, v)
    v
  }

  def getOrElseUpdate(key: K)(compute: => Stitch[V]): Stitch[V] =
    underlying.computeIfAbsent(
      key,
      _ => {
        compute match {
          // already in the correct form and are thread-safe
          case s @ (Const(_) | SynchronizedRef(_)) => s

          // not thread-safe so wrap it so it is
          case s => Stitch.synchronizedRef(s)
        }
      }
    )

  def evict(key: K, value: Stitch[V]): Boolean =
    underlying.remove(key, value)

  def size: Int = underlying.size()
}
