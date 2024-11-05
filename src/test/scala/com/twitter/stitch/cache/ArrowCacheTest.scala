package com.twitter.stitch.cache

import com.twitter.stitch.{Arrow, NotFound}
import com.twitter.stitch.cache.testable.TestableStitchCache
import com.twitter.stitch.helpers.AwaitHelpers
import com.twitter.util.{Return, Throw}
import org.scalatest.funsuite.AnyFunSuite

class ArrowCacheTest extends AnyFunSuite with AwaitHelpers {

  val key = "key"
  val value = "value"

  case class MockException() extends Exception

  test("ArrowCache should wrap an arrow in a cache") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ArrowCache(cache)

    var ran = 0
    val arrow = sCache.wrap(Arrow.map[String, String] { s => ran += 1; s })

    intercept[MockException](await(arrow(Throw(MockException()))))
    assert(ran == 0)
    assert(await(arrow(Return(value))) == value)
    assert(ran == 1)
    assert(await(arrow(Return(value))) == value)
    assert(ran == 1)
  }

  test("ArrowCache should wrap an arrow in an async cache") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ArrowCache(cache)

    var ran = 0
    val arrow = sCache.wrapAsync(Arrow.map[String, String] { s => ran += 1; s })

    intercept[MockException](await(arrow(Throw(MockException()))))
    assert(ran == 0) // exceptions skip evaluation
    assert(intercept[Exception](await(arrow(Return(value)))) == NotFound) // not present so NotFound
    assert(ran == 1) // runs even though we returned NotFound
    assert(await(arrow(Return(value))) == value) // in cache so returns result
    assert(ran == 1) // didnt run again
  }
}
