package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import com.twitter.stitch.cache.testable.TestableStitchCache
import com.twitter.stitch.helpers.AwaitHelpers
import org.scalatest.funsuite.AnyFunSuite

class LazilyEvictingCacheTest extends AnyFunSuite with AwaitHelpers {
  val key = "key"
  val value = "value"

  case class MockException() extends Exception()

  test("LazilyEvictingCache should evict on failed Stitches for get") {
    val cache = new TestableStitchCache[String, String]
    val sCache = new LazilyEvictingCache(cache)

    val e = Stitch.exception(MockException())
    sCache.set(key, e)
    assert(sCache.get(key).contains(e))
    assert(cache.evictCalls == 1)
    assert(cache.get(key).isEmpty)
  }

  test("LazilyEvictingCache should keep completed Stitches for get") {
    val cache = new TestableStitchCache[String, String]
    val sCache = new LazilyEvictingCache(cache)

    val s = sCache.set(key, Stitch.value(value))
    assert(sCache.get(key).contains(s))
    assert(sCache.get(key).contains(s)) // still contains s
  }

  test("LazilyEvictingCache should evict on failed Stitches for getOrElseUpdate") {
    val cache = new TestableStitchCache[String, String]
    val sCache = new LazilyEvictingCache(cache)

    val v = Stitch.value(value)
    val e = Stitch.exception(MockException())
    sCache.set(key, e)

    val s = sCache.getOrElseUpdate(key)(v)
    assert(cache.getCalls == 1)
    assert(
      cache.getOrElseUpdateComputeResult.contains(v)
    ) // exception found so its evicted and updated with v
    assert(cache.getOrElseUpdateCalls == 1)
    assert(cache.evictCalls == 1)
    assert(s == v)
    assert(sCache.get(key).contains(v))
  }

  test("LazilyEvictingCache should keep satisfied Stitches for getOrElseUpdate") {
    val cache = new TestableStitchCache[String, String]
    val sCache = new LazilyEvictingCache(cache)

    val v = Stitch.value("other")
    val s = Stitch.value(value)
    sCache.set(key, s)

    val goeu = sCache.getOrElseUpdate(key)(v)
    assert(cache.getCalls == 1)
    assert(cache.getOrElseUpdateComputeResult.isEmpty)
    assert(cache.getOrElseUpdateCalls == 0)
    assert(cache.evictCalls == 0)
    assert(s == goeu)
    assert(sCache.get(key).contains(s))
  }
}
