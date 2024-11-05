package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import com.twitter.stitch.Stitch.{Const, Ref, SynchronizedRef}
import com.twitter.util.Throw

/**
 * Wraps an underlying StitchCache, ensuring that failed Stitches that are set in
 * the cache are evicted later when they are run.
 */
private[stitch] class EvictingCache[K, V](underlying: StitchCache[K, V])
    extends StitchCacheProxy[K, V](underlying) {

  private def evictOnFailure(k: K, v: Stitch[V]): Stitch[V] = {
    var forwardRef: Stitch[V] = null
    val s: Stitch[V] = v.onFailure(_ => underlying.evict(k, forwardRef))
    forwardRef = s
    forwardRef
  }

  override def set(k: K, v: Stitch[V]): Stitch[V] =
    underlying.set(k, evictOnFailure(k, v))

  override def getOrElseUpdate(k: K)(v: => Stitch[V]): Stitch[V] =
    underlying.getOrElseUpdate(k)(evictOnFailure(k, v))
}

/**
 * Wraps an underlying StitchCache, ensuring that if a failed Stitch
 * is fetched, we evict it the next time it's retrieved and run
 *
 * When doing [[get]], if the cached value is a failure, it will be
 * evicted when read, then the failed result will be returned.
 *
 * When doing [[getOrElseUpdate]], if the cached value is a failure,
 * it will be evicted and updated when it's read.
 *
 * [[LazilyEvictingCache]] is useful for extremely high throughput caches.
 * Since eviction is lazy, a key which failed won't result in an eviction
 * operation unless it's actually accessed again.
 */
private[stitch] class LazilyEvictingCache[K, V](underlying: StitchCache[K, V])
    extends StitchCacheProxy[K, V](underlying) {

  private[this] def hasFailed(s: Stitch[V]): Boolean =
    s match {
      case Const(Throw(_)) | Ref(Const(Throw(_))) | SynchronizedRef(Const(Throw(_))) => true
      case _ => false
    }

  private[this] def invalidateLazily(k: K, s: Stitch[V]): Stitch[V] = {
    s match {
      case s if hasFailed(s) =>
        evict(k, s)
        s // returns the result that was returned by the cache, even though it's now evicted
      case s => s
    }
  }

  override def get(k: K): Option[Stitch[V]] =
    underlying.get(k).map(invalidateLazily(k, _))

  override def getOrElseUpdate(k: K)(v: => Stitch[V]): Stitch[V] =
    get(k) match {
      // cached value was not a failure, then return it
      case Some(s) if !hasFailed(s) => s
      // cach miss or was a failure, so update it
      case _ => underlying.getOrElseUpdate(k)(v)
    }
}

object EvictingCache {

  /**
   * Wraps an underlying StitchCache, ensuring that failed Stitches that are set in
   * the cache are evicted later when they are run.
   */
  def apply[K, V](underlying: StitchCache[K, V]): StitchCache[K, V] =
    new EvictingCache[K, V](underlying)

  /**
   * Wraps an underlying StitchCache, ensuring that if a failed Stitch
   * is fetched, we evict it the next time it's retrieved and run
   *
   * If the cached value is a failure, it will be evicted when read,
   * then the failed result will be returned
   */
  def lazily[K, V](underlying: StitchCache[K, V]): StitchCache[K, V] =
    new LazilyEvictingCache[K, V](underlying)
}
