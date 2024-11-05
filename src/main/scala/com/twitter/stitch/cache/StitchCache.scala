package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import java.util.concurrent.ConcurrentMap

/** StitchCache is used to represent an in-memory, in-process, asynchronous cache.
 *
 * Users __must never__ run a Stitch passed into a cache method,
 * and must instead run the Stitch returned by the cache.
 *
 * It is __never safe to re-run or share Stitches__, so __always
 * query the cache for a new Stitch to run.__
 *
 * Stitches passed into a StitchCache become owned by the cache,
 * and Stitches returned from it are owned by the caller.
 *
 * Every cache operation is atomic.
 *
 * @note Any correct implementation should make sure that Stitches which
 *       fail after being added are evicted. This is usually done by composing a
 *       [[StitchCache]] with an [[EvictingCache]] or [[LazilyEvictingCache]] wrapper.
 *
 * A reference implementation for caching the results of
 * an asynchronous function can be found at [[StitchCache$.standard]].
 */
abstract class StitchCache[K, V] {

  /** Gets the cached Stitch.
   *
   * @return None if a value hasn't been specified for that key yet
   *         Some(stitch) if the value has been specified. Just because
   *         this returns Some(...) doesn't mean that the contained
   *         Stitch has been run.
   */
  def get(key: K): Option[Stitch[V]]

  /** Gets the cached Stitch, or if it isn't added to the cache yet, computes it and
   * returns that value.
   *
   * Only run the Stitch returned by the cache, never run the Stitch
   * that's passed into `getOrElseUpdate`.
   *
   * `compute` must not throw or return `null`.
   */
  def getOrElseUpdate(key: K)(compute: => Stitch[V]): Stitch[V]

  /** Unconditionally sets a value for a given key
   *
   * Only run the Stitch returned by the cache, never run the Stitch
   * that's passed into `set`.
   */
  def set(key: K, value: Stitch[V]): Stitch[V]

  /** Evicts the contents of a `key` if the old value is `value`.
   *
   * Since [[com.twitter.stitch.Stitch]] uses reference equality, you must use the
   * same object reference that was returned by the cache to evict a value.
   *
   * @return true if the key was evicted
   *         false if the key was not evicted
   */
  def evict(key: K, value: Stitch[V]): Boolean

  /** @return the number of queries that are stored in the cache */
  def size: Int
}

/** A proxy for [[StitchCache]]s, useful for wrap-but-modify. */
abstract class StitchCacheProxy[K, V](underlying: StitchCache[K, V]) extends StitchCache[K, V] {
  def get(key: K): Option[Stitch[V]] = underlying.get(key)

  def getOrElseUpdate(key: K)(compute: => Stitch[V]): Stitch[V] =
    underlying.getOrElseUpdate(key)(compute)

  def set(key: K, value: Stitch[V]): Stitch[V] = underlying.set(key, value)

  def evict(key: K, value: Stitch[V]): Boolean = underlying.evict(key, value)

  def size: Int = underlying.size
}

/** StitchCache is used to represent an in-memory, in-process, asynchronous cache.
 *
 * Users must never run a Stitch passed into a cache method,
 * and must instead run the Stitch returned by the cache.
 *
 * It is never safe to re-run or share Stitches, so always
 * query the cache for a new Stitch to run.
 *
 * Stitches passed into a StitchCache become owned by the cache,
 * and Stitches returned from it are owned by the caller.
 */
object StitchCache {

  /** Creates a function which caches the results of `fn` in a [[StitchCache]]
   * backed by a [[java.util.concurrent.ConcurrentMap]].
   *
   * This can be used with `com.github.benmanes.caffeine.cache.Cache` or
   * `com.google.common.cache.Cache` by doing
   * {{{
   *   val cache : com.[...].cache.Cache
   *   fromMap(fn, cache.asMap)
   * }}}
   *
   * Doing `asMap` will ignore any `com.[...].cache.CacheLoader`s on the underlying cache.
   * The `com.[...].cache.CacheLoader`'s `load` method can be provided as `fn` to provide
   * the expected functionality.
   *
   * Both of the below are equivalent. Since the `com.[...].cache.CacheLoader` is ignored
   * after doing `.asMap` it doesn't matter if it's been applied to the `.build(...)` method.
   * As long as the `com.[...].cache.CacheLoader`'s `.load` method is passed into [[fromMap]]
   * then it will behave as expected. However, it's not recommended to pass the `CacheLoader`
   * into `.build(...)` since it can lead to confusion.
   *
   * {{{
   *   val cacheLoader: com.[...].cache.CacheLoader
   *   // a cache without the cacheloader applied
   *   val cache : com.[...].cache.Cache = [...].newBuilder.[...].build()
   *   fromMap(cacheLoader.load, cache.asMap)
   * }}}
   * or (the above is preferred)
   * {{{
   *   val cacheLoader: com.[...].cache.CacheLoader
   *   // a cache with the cacheloader applied, but it's ignored because `.asMap`
   *   // it's not recommended to do this to avoid issues
   *   val cache : com.[...].cache.LoadingCache = [...].newBuilder.[...].build(cacheLoader)
   *   fromMap(cacheLoader.load, cache.asMap)
   * }}}
   *
   * Once the underlying `com.[...].cache.Cache` is wrapped in a [[StitchCache]] then it considered
   * owned by [[StitchCache]] and the underlying `com.[...].cache.Cache` should never be accessed
   * directly. Reading or writing directly to the underlying cache can pollute it with incorrectly
   * formed Stitches which will not be safe to run or result in failed Stitches not being evicted.
   *
   * @param fn a function from K => Stitch[V] that will be called to create a Stitch if its not already
   *           in the cache. This must not throw or return `null`.
   * @param map is a [[ConcurrentMap]] that will be used to store Stitches
   * @return a Function from `K => Stitch[V]` that abstracts away the underlying [[StitchCache]].
   *         This implementation eagerly evicts failed Stitches.
   */
  def fromMap[K, V](fn: K => Stitch[V], map: ConcurrentMap[K, Stitch[V]]): K => Stitch[V] =
    standard(fn, new ConcurrentMapCache(map))

  /** Encodes keys, producing a [[StitchCache]] that takes keys of a different type. */
  def keyEncoded[K, V, U](encode: K => V, cache: StitchCache[V, U]): StitchCache[K, U] =
    new KeyEncodingCache(encode, cache)

  /** Creates a function which caches the results of `fn` in an [[StitchCache]].
   *  Ensures that failed Stitches are evicted eagerly.
   *
   *  For lazy eviction, use the [[LazilyEvictingCache]] wrapper.
   */
  def standard[K, V](fn: K => Stitch[V], cache: StitchCache[K, V]): K => Stitch[V] =
    MemoizeQuery(fn, new EvictingCache(cache))
}
