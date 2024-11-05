package com.twitter.stitch.cache

import com.twitter.stitch.cache.abstractsuites.AbstractStitchCacheTest
import com.twitter.stitch.cache.testable.TestableStitchCache

/** Since we use this cache for testing, lets make sure it behaves as a StitchCache */
class TestableStitchCacheTest extends AbstractStitchCacheTest {
  override def isWrapper: Boolean = true

  override def name: String = "TestableStitchCache"

  override def fixture(): Fixture = new Fixture {
    override val cache: StitchCache[String, String] = new TestableStitchCache[String, String]()
  }
}
