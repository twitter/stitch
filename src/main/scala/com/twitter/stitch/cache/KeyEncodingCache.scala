package com.twitter.stitch.cache

import com.twitter.stitch.Stitch

/** Encodes keys, producing a Cache that takes keys of a different type.
 *
 * Useful for compressing keys, or discarding extra information.  This is
 * especially useful when information is useful for computing a value for the
 * first time, but isn't necessary for demonstrating distinctness.
 *
 * e.g.
 *
 * val fibCache: StitchCache[Int, Int]
 * val fn: (Int, Int, Int, Int) => Int = { case (target, prev, cur, idx) =>
 *   if (idx == target) cur else fn((target, cur, prev + cur, idx + 1))
 * }
 * val memo: (Int, Int, Int, Int) => Int = Cache(fn, new KeyEncodingCache(
 *   new StitchCache[Int, Int],
 *   { case (target: Int, _, _, _) => target }
 * ))
 */
private[stitch] class KeyEncodingCache[K, V, U](encode: K => V, underlying: StitchCache[V, U])
    extends StitchCache[K, U] {
  def get(key: K): Option[Stitch[U]] = underlying.get(encode(key))

  def set(key: K, value: Stitch[U]): Stitch[U] = underlying.set(encode(key), value)

  def getOrElseUpdate(key: K)(compute: => Stitch[U]): Stitch[U] =
    underlying.getOrElseUpdate(encode(key))(compute)

  def evict(key: K, value: Stitch[U]): Boolean = underlying.evict(encode(key), value)

  def size: Int = underlying.size
}
