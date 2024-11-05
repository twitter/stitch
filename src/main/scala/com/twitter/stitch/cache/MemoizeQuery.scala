package com.twitter.stitch.cache

import com.twitter.stitch.Stitch

object MemoizeQuery {

  /**
   * Produces a function which caches a Stitch query in the supplied Cache.
   *
   * Does not guarantee that `fn` will be computed exactly once for each key.
   */
  def apply[A, B](fn: A => Stitch[B], cache: StitchCache[A, B]): A => Stitch[B] =
    new MemoizedFunction(fn, cache)
}

private[stitch] class MemoizedFunction[A, B](fn: A => Stitch[B], cache: StitchCache[A, B])
    extends (A => Stitch[B]) {

  def apply(a: A): Stitch[B] = cache.getOrElseUpdate(a) { fn(a) }
}
