package com.twitter.stitch.cache

import com.twitter.stitch.{Arrow, Stitch}
import com.twitter.util.{Promise, Try}

trait CacheArrow[K, V] {

  /** Wraps an arrow in a StitchCache */
  def wrap(a: Arrow[K, V]): Arrow[K, V]
}

trait CacheAsyncArrow[K, V] {

  /** Wraps an arrow in a StitchCache that returns [[Stitch.NotFound]]
   * if the value isn't in the cache, then adds it the cache
   * asynchronously when the value becomes available.
   */
  def wrapAsync(a: Arrow[K, V]): Arrow[K, V]
}

/**
 * An [[ArrowCache]] provides wrapper methods for [[Arrow]]s so they can be easily used with
 * the `underlying` [[StitchCache]].
 */
case class ArrowCache[K, V](underlying: StitchCache[K, V])
    extends CacheArrow[K, V]
    with CacheAsyncArrow[K, V] {

  def wrap(a: Arrow[K, V]): Arrow[K, V] = {
    val zipAndGetFromCache: Arrow[K, (K, (Boolean, Promise[V], Stitch[V]))] =
      Arrow.zipWithArg(Arrow.identity[K].map { k =>
        var isHit = true
        val p = new Promise[V]
        val s = underlying.getOrElseUpdate(k) { isHit = false; Stitch.callFuture(p) }
        (isHit, p, s)
      })

    val isHit: Arrow.Choice[(K, (Boolean, Promise[V], Stitch[V])), V] =
      Arrow.Choice.ifDefinedAt(
        { case (_, (isHit, _, s)) if isHit => s },
        Arrow.flatMap[Stitch[V], V](identity))

    val runArrow: Arrow[(K, (Boolean, Promise[V], Stitch[V])), Try[V]] =
      Arrow
        .map[(K, (Boolean, Promise[V], Stitch[V])), K] { case (key, _) => key }.andThen(a).liftToTry

    val runAndPopulateCache: Arrow[(K, (Boolean, Promise[V], Stitch[V])), V] =
      Arrow
        .zipWithArg(runArrow)
        .flatMap { // this happens even if `a` fails (due to liftToTry) so we complete the promise in either case
          case ((_, (_, promise, s)), result) =>
            promise.update(result)
            // we need to run `s` to ensure that we respect and execute actions from the underlying StitchCache
            s
        }

    zipAndGetFromCache.andThen(Arrow.choose(isHit, Arrow.Choice.otherwise(runAndPopulateCache)))
  }

  def wrapAsync(a: Arrow[K, V]): Arrow[K, V] = {
    val zipAndGetFromCache: Arrow[K, (K, (Boolean, Promise[V], Stitch[V]))] =
      Arrow.zipWithArg(Arrow.identity[K].map { k =>
        var isHit = true
        val p = new Promise[V]
        val s = underlying.getOrElseUpdate(k) { isHit = false; Stitch.callFuture(p) }
        (isHit, p, s)
      })

    val isHit: Arrow.Choice[(K, (Boolean, Promise[V], Stitch[V])), V] =
      Arrow.Choice.ifDefinedAt(
        { case (_, (isHit, _, s)) if isHit => s },
        Arrow.flatMap[Stitch[V], V](identity))

    val runArrow: Arrow[(K, (Boolean, Promise[V], Stitch[V])), Try[V]] =
      Arrow
        .map[(K, (Boolean, Promise[V], Stitch[V])), K] { case (key, _) => key }.andThen(a).liftToTry

    val runAndPopulateCache: Arrow[(K, (Boolean, Promise[V], Stitch[V])), V] =
      Arrow
        .zipWithArg(runArrow)
        .flatMap { // this happens even if `a` fails (due to liftToTry) so we complete the promise in either case
          case ((_, (_, promise, s)), result) =>
            promise.update(result)
            // we need to run `s` to ensure that we respect and execute actions from the underlying StitchCache
            s
        }

    val runAndReturnNotFound: Arrow[(K, (Boolean, Promise[V], Stitch[V])), V] =
      Arrow.async(runAndPopulateCache).andThen(Arrow.NotFound)

    zipAndGetFromCache.andThen(Arrow.choose(isHit, Arrow.Choice.otherwise(runAndReturnNotFound)))
  }
}
