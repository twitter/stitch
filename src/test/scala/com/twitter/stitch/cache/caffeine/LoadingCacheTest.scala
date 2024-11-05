package com.twitter.stitch.cache.caffeine

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.twitter.stitch.Stitch
import com.twitter.stitch.cache.{ConcurrentMapCache, StitchCache}
import com.twitter.stitch.cache.abstractsuites.AbstractStitchCacheTest

class LoadingCacheTest extends AbstractStitchCacheTest {
  override def name: String = "CaffeineCache"

  // cacheloader is ignored so existing tests should behave normally
  def cacheLoader: CacheLoader[String, Stitch[String]] = new CacheLoader[String, Stitch[String]] {
    override def load(k: String): Stitch[String] = Stitch.value("fromLoader")
  }

  def caffeine(): LoadingCache[String, Stitch[String]] =
    Caffeine.newBuilder().build[String, Stitch[String]](cacheLoader)

  override def fixture(): Fixture = new Fixture {
    override val cache = new ConcurrentMapCache[String, String](caffeine().asMap())
  }

  test("CacheLoader is ignored for `get` after doing `.asMap`") {
    val underlying = caffeine()
    val cache = new ConcurrentMapCache[String, String](underlying.asMap())

    // get on missing (cacheloader not invoked)
    assert(cache.get("k").isEmpty)

    // accessing the underlying, no one should ever do this
    assert(await(underlying.get("k")) == "fromLoader")
    assert(cache.get("k").map(await(_)).contains("fromLoader"))
  }

  test("CacheLoader is ignored for `getOrElseUpdate` after doing `.asMap`") {
    val underlying = caffeine()
    val cache = new ConcurrentMapCache[String, String](underlying.asMap())

    // getOrElseUpdate on missing updates (cacheloader not invoked)
    val update = cache.getOrElseUpdate("k")(Stitch.value("fromUpdate"))
    assert(await(update) == "fromUpdate") // cacheloader is ignored

    // accessing the underlying, no one should ever do this
    assert(await(underlying.get("u")) == "fromLoader")
    assert(await(cache.getOrElseUpdate("u")(Stitch.value("fromUpdate"))) == "fromLoader")
  }

  test("CacheLoader is respected when passed in directly") {
    val cache = StitchCache.fromMap(cacheLoader.load, caffeine().asMap())
    assert(await(cache("k")) == "fromLoader") // cacheloader is ignored
  }
}
