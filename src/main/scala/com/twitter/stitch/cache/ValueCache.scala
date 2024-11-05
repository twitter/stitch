package com.twitter.stitch.cache

import com.twitter.stitch.{Arrow, Stitch}

/**
 * Wraps an underlying StitchCache so that only stitches which have completed running
 * and are successful get cached. No pending Stitches or failures will be cached.
 *
 * Stitches returned by [[ValueCache.getOrElseUpdate]] and [[ValueCache.set]] can not be
 * used for cache eviction. Instead call [[ValueCache.get]] to get a reference to the Stitch
 * that's stored in the cache to use when calling [[ValueCache.evict]].
 *
 * With other [[StitchCache]]s, as soon as `getOrElseUpdate` or `set` is called, the Stitch
 * is added to the cache. This means that if multiple concurrent Stitches try to access
 * the same key, it's a race to be the first to run the cached Stitch while the remaining
 * Stitches will wait for the result,
 * i.e. other [[StitchCache]] implementations de-duplicate in-flight requests by key.
 *
 * With [[ValueCache]], only successfully completed Stitches are added to the cache.
 * This means that concurrent Stitches will each run independently, then race to write the
 * value back to the cache when it's available.
 * i.e. [[ValueCache()]] does not deduplicate in-flight requests by key.
 *
 * Other [[StitchCache]]s can decrease the number of concurrent requests for a given key by making
 * multiple Stitches dependent on the outcome of 1 Stitch's success. Where with [[ValueCache]],the
 * number of concurrent requests remains the same for a given key keeping each Stitch independent.
 *
 * For example:
 *   Stitches s0, s1, and s2 are all running concurrently, they all call `getOrElseUpdate` for the
 *   same key that isn't in the cache yet. The returned Stitch is run by all 3, but only 1, say s0,
 *   actually runs it, the other 2 are waiting on the result. If the Stitch succeeds then they all
 *   3 succeed, however if the Stitch fails, then all 3 fail
 *
 *   With [[ValueCache]] things would be different. Each Stitch would get back it's own Stitch with
 *   a callback that updates the value in the cache if it succeeds. When the 3 Stitches are run
 *   concurrently, they aren't waiting on each other and are instead all run independently.
 *   Whichever Stitch is completes successfully first will have it's value saved into the cache.
 *   If any of the Stitches fail, the remaining Stitches still have a chance to succeed.
 */
class ValueCache[K, V](underlying: StitchCache[K, V])
    extends StitchCacheProxy[K, V](underlying)
    with CacheArrow[K, V]
    with CacheAsyncArrow[K, V] {

  /** Gets the cached Stitch, or if it isn't added to the cache yet, computes it and
   * returns that value.
   *
   * Only run the Stitch returned by the cache, never run the Stitch
   * that's passed into `getOrElseUpdate`.
   *
   * @note Stitches returned by [[getOrElseUpdate]] can **NOT** be used for eviction.
   *              To evict from a [[ValueCache]] you must first call [[get]] to get a reference to
   *              the stored Stitch
   */
  override def getOrElseUpdate(key: K)(compute: => Stitch[V]): Stitch[V] =
    underlying.get(key) match {
      case Some(v) => v
      case None =>
        // this Stitch can't be used for eviction because we store a `Stitch.Const` containing the result
        // instead of a reference to this stitch itself. This to avoid synchronization overhead.
        compute.onSuccess(result => underlying.getOrElseUpdate(key)(Stitch.value(result)))
    }

  /** Gets the cached Stitch, or if it isn't in the cache, returns
   * [[Stitch.NotFound]], then adds it to the cache asynchronously
   * when the value becomes available.
   *
   * The Stitch returned by getOrElseUpdateAsync can not be used to
   * evict.
   *
   * Only run the Stitch returned by the cache, never run the Stitch
   * that's passed into `getOrElseUpdateAsync`.
   */
  def getOrElseUpdateAsync(key: K)(compute: => Stitch[V]): Stitch[V] =
    get(key).getOrElse {
      Stitch.async(getOrElseUpdate(key)(compute)).flatMap(_ => Stitch.NotFound)
    }

  /** Unconditionally sets a value for a given key
   *
   * Only run the Stitch returned by the cache, never run the Stitch
   * that's passed into `set`.
   *
   * @note Stitches returned by [[set]] can **NOT** be used for eviction.
   *             To evict from a [[ValueCache]] you must first call [[get]] to get a reference to
   *             the stored Stitch.
   */
  override def set(key: K, value: Stitch[V]): Stitch[V] =
    // this Stitch can't be used for eviction because we store a `Stitch.Const` containing the result
    // instead of a reference to this stitch itself. This to avoid synchronization overhead.
    value.onSuccess(result => underlying.set(key, Stitch.value(result)))

  def wrap(a: Arrow[K, V]): Arrow[K, V] = {
    val zipAndGetFromCache: Arrow[K, (K, Option[Stitch[V]])] =
      Arrow.zipWithArg(Arrow.map[K, Option[Stitch[V]]](underlying.get))

    val isHit: Arrow.Choice[(K, Option[Stitch[V]]), V] =
      Arrow.Choice.ifDefinedAt({ case (_, Some(s)) => s }, Arrow.flatMap[Stitch[V], V](identity))

    val zipAndRunArrow: Arrow[(K, Option[Stitch[V]]), (K, V)] =
      Arrow.map[(K, Option[Stitch[V]]), K] { case (key, _) => key }.andThen(Arrow.zipWithArg(a))

    val populateCacheOnSuccess: Arrow[(K, V), V] =
      // flatMap so that we respect and execute actions from the underlying StitchCache
      Arrow.flatMap { case (k, v) => underlying.getOrElseUpdate(k)(Stitch.value(v)) }

    val runAndPopulateCache: Arrow[(K, Option[Stitch[V]]), V] =
      zipAndRunArrow.andThen(populateCacheOnSuccess)

    zipAndGetFromCache.andThen(Arrow.choose(isHit, Arrow.Choice.otherwise(runAndPopulateCache)))
  }

  def wrapAsync(a: Arrow[K, V]): Arrow[K, V] = {
    val zipAndGetFromCache: Arrow[K, (K, Option[Stitch[V]])] =
      Arrow.zipWithArg(Arrow.map[K, Option[Stitch[V]]](underlying.get))

    val isHit: Arrow.Choice[(K, Option[Stitch[V]]), V] =
      Arrow.Choice.ifDefinedAt({ case (_, Some(s)) => s }, Arrow.flatMap[Stitch[V], V](identity))

    val zipAndRunArrow: Arrow[(K, Option[Stitch[V]]), (K, V)] =
      Arrow.map[(K, Option[Stitch[V]]), K] { case (key, _) => key }.andThen(Arrow.zipWithArg(a))

    val populateCache: Arrow[(K, V), V] =
      // flatMap so that we respect and execute actions from the underlying StitchCache
      Arrow.flatMap { case (k, v) => underlying.getOrElseUpdate(k)(Stitch.value(v)) }

    val runAndAsyncPopulateCacheAndReturnNotFound: Arrow[(K, Option[Stitch[V]]), V] =
      Arrow.async(zipAndRunArrow.andThen(populateCache)).andThen(Arrow.NotFound)

    zipAndGetFromCache.andThen(
      Arrow.choose(isHit, Arrow.Choice.otherwise(runAndAsyncPopulateCacheAndReturnNotFound)))
  }
}

object ValueCache {

  /**
   * Wraps an underlying StitchCache so that only stitches which have completed running
   * and are successful get cached. No pending Stitches or failures will be cached.
   */
  def apply[K, V](underlying: StitchCache[K, V]): ValueCache[K, V] =
    new ValueCache(underlying)
}

/**
 * Wraps a [[ValueCache]] to proxy [[getOrElseUpdate]] to [[underlying.getOrElseUpdateAsync]]
 *
 * @see [[AsyncValueCache]]'s companion object and [[ValueCache.getOrElseUpdateAsync]] for more information.
 */
class AsyncValueCache[K, V](underlying: ValueCache[K, V])
    extends StitchCacheProxy[K, V](underlying)
    with CacheArrow[K, V]
    with CacheAsyncArrow[K, V] {
  override def getOrElseUpdate(key: K)(compute: => Stitch[V]): Stitch[V] =
    underlying.getOrElseUpdateAsync(key)(compute)
  override def wrap(a: Arrow[K, V]): Arrow[K, V] = underlying.wrap(a)
  override def wrapAsync(a: Arrow[K, V]): Arrow[K, V] = underlying.wrapAsync(a)
}

object AsyncValueCache {

  /**
   * Wraps an underlying StitchCache in a [[ValueCache]] and then proxies
   * [[ValueCache.getOrElseUpdate]] to [[ValueCache.getOrElseUpdateAsync]].
   * This makes it convenient to use [[StitchCache]]'s convenience functions
   * but with async cache population semantics.
   *
   * @see [[AsyncValueCache]] and [[ValueCache.getOrElseUpdateAsync]] for more information.
   */
  def apply[K, V](underlying: StitchCache[K, V]): AsyncValueCache[K, V] =
    new AsyncValueCache(new ValueCache(underlying))
}
