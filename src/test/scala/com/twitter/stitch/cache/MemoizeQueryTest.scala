package com.twitter.stitch.cache

import com.twitter.stitch.Stitch
import com.twitter.stitch.cache.testable.TestableStitchCache
import com.twitter.stitch.helpers.AwaitHelpers
import org.scalatest.funsuite.AnyFunSuite

class MemoizeQueryTest extends AnyFunSuite with AwaitHelpers {

  test("MemoizeQuery saves the query if it doesn't exist in the cache") {
    val cache = new TestableStitchCache[String, String]
    val key = "key"
    val value = Stitch.value("value")

    val memoized = MemoizeQuery(
      { k: String =>
        k match {
          case "key" => value
          case _ => fail()
        }
      },
      cache)

    assert(await(memoized(key)) == "value")
    assert(cache.getOrElseUpdateCalls == 1)
    assert(cache.getOrElseUpdateComputeResult.contains(value)) // not in cache so computed
    assert(cache.get(key).contains(value)) // saved in the cache
  }

  test("MemoizeQuery returns the stored value from the cache") {
    val cache = new TestableStitchCache[String, String]
    val key = "key"
    val value = Stitch.value("value")

    cache.set(key, value)

    val memoized = MemoizeQuery({ _: String => fail() }, cache)

    assert(await(memoized(key)) == "value")
    assert(cache.getOrElseUpdateCalls == 1)
    assert(cache.getOrElseUpdateComputeResult.isEmpty) // in cache so never computed
  }

}
