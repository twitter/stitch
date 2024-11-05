package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import com.twitter.stitch.cache.testable.TestableStitchCache
import com.twitter.stitch.helpers.AwaitHelpers
import com.twitter.util.Promise
import org.scalatest.funsuite.AnyFunSuite

class EvictingCacheTest extends AnyFunSuite with AwaitHelpers {
  val key = "key"
  val value = "value"

  case class MockException() extends Exception()

  test("EvictingCache should evict a failed Stitch once it's run for set") {
    val cache = new TestableStitchCache[String, String]
    val sCache = new EvictingCache(cache)

    val s = sCache.set(key, Stitch.exception(MockException()))
    assert(cache.setCalls == 1)
    assert(cache.evictCalls == 0)
    assert(sCache.get(key).nonEmpty)
    intercept[MockException](await(s))
    assert(cache.setCalls == 1)
    assert(cache.evictCalls == 1)
    assert(sCache.get(key).isEmpty)
  }

  test("EvictingCache should keep completed Stitches for set") {
    val cache = new TestableStitchCache[String, String]
    val sCache = new EvictingCache(cache)

    val p = Promise[String]()
    val s = sCache.set(key, Stitch.callFuture(p))
    val c = Stitch.run(s)
    assert(cache.setCalls == 1)
    p.setValue(value)
    awaitFuture(c)
    assert(cache.evictCalls == 0)
  }

  test("EvictingCache should evict a failed Stitch once its run for getOrElseUpdate") {
    val cache = new TestableStitchCache[String, String]
    val sCache = new EvictingCache(cache)

    val s = sCache.getOrElseUpdate(key)(Stitch.exception(MockException()))
    assert(cache.getOrElseUpdateCalls == 1)
    assert(cache.evictCalls == 0)
    assert(sCache.get(key).nonEmpty)
    intercept[MockException](await(s))
    assert(cache.getOrElseUpdateCalls == 1)
    assert(cache.evictCalls == 1)
    assert(sCache.get(key).isEmpty)
  }

  test("EvictingCache should keep satisfied Stitches for getOrElseUpdate") {
    val cache = new TestableStitchCache[String, String]
    val sCache = new EvictingCache(cache)

    val p = Promise[String]()
    val s = sCache.getOrElseUpdate(key)(Stitch.callFuture(p))
    val c = Stitch.run(s)
    assert(cache.getOrElseUpdateComputeResult.contains(s))
    p.setValue(value)
    assert(awaitFuture(c) == value)
    assert(sCache.get(key).contains(s))
  }
}
