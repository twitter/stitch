package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import com.twitter.stitch.cache.abstractsuites.AbstractStitchCacheTest
import java.util.concurrent.ConcurrentHashMap

class ConcurrentMapCacheTest extends AbstractStitchCacheTest {

  override def name: String = "ConcurrentMapCache"

  override def fixture(): Fixture = new Fixture {
    val map = new ConcurrentHashMap[String, Stitch[String]]()
    override val cache = new ConcurrentMapCache[String, String](map)
  }
}
