package com.twitter.stitch.cache.caffeine

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.twitter.stitch.Stitch
import com.twitter.stitch.cache.ConcurrentMapCache
import com.twitter.stitch.cache.abstractsuites.AbstractStitchCacheTest

class CaffeineCacheTest extends AbstractStitchCacheTest {
  override def name: String = "CaffeineCache"

  override def fixture(): Fixture = new Fixture {
    val caffeine: Cache[String, Stitch[String]] =
      Caffeine.newBuilder().build[String, Stitch[String]]()
    override val cache = new ConcurrentMapCache[String, String](caffeine.asMap())
  }
}
