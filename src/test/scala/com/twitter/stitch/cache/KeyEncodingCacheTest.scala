package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import com.twitter.stitch.cache.abstractsuites.AbstractStitchCacheTest
import java.util.concurrent.ConcurrentHashMap

class KeyEncodingCacheTest extends AbstractStitchCacheTest {
  override def name: String = "KeyEncodingCache"

  override def fixture(): Fixture = new Fixture {
    val underlyingMap: ConcurrentHashMap[Int, Stitch[String]] = new ConcurrentHashMap()
    val underlyingCache: StitchCache[Int, String] = new ConcurrentMapCache(underlyingMap)
    override val cache: StitchCache[String, String] =
      new KeyEncodingCache({ num: String => num.hashCode }, underlyingCache)
  }
}
