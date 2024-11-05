package com.twitter.stitch.cache

import com.twitter.conversions.DurationOps._
import com.twitter.stitch.Stitch.Const
import com.twitter.stitch.cache.testable.TestableStitchCache
import com.twitter.stitch.helpers.AwaitHelpers
import com.twitter.stitch.{Arrow, Stitch}
import com.twitter.util.{JavaTimer, MockTimer, Return, Throw, Time, TimeoutException}
import org.scalatest.funsuite.AnyFunSuite

class ValueCacheTest extends AnyFunSuite with AwaitHelpers {
  val key = "key"
  val value = "value"

  case class MockException() extends Exception

  test("ValueCache should only cache Const Stitches of the result when calling set") {
    val cache = new TestableStitchCache[String, Unit]()
    val sCache = ValueCache(cache)

    // a non-const Stitch that will complete immediately when run
    val s = Stitch.Done.before(Stitch.Done)
    await(sCache.set(key, s))
    assert(cache.get(key).exists(_.isInstanceOf[Const[Unit]]))
  }

  test("ValueCache should only cache Const Stitches of the result when calling getOrElseUpdate") {
    val cache = new TestableStitchCache[String, Unit]()
    val sCache = ValueCache(cache)

    // a non-const Stitch that will complete immediately when run
    val s = Stitch.Done.before(Stitch.Done)
    await(sCache.getOrElseUpdate(key)(s))
    assert(cache.get(key).exists(_.isInstanceOf[Const[Unit]]))
  }

  test("ValueCache should cache complete Stitches for set") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ValueCache(cache)

    val const = Stitch.value(value)

    val s = sCache.set(key, const)
    assert(cache.setCalls == 0)
    await(s)
    assert(cache.setCalls == 1)
    assert(sCache.get(key).map(await(_)).contains(value))
  }

  test("ValueCache shouldn't cache incomplete Stitches for set") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ValueCache(cache)

    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val s = sCache.set(key, Stitch.Never)
      assert(cache.setCalls == 0)
      val runningStitch = Stitch.run(s.within(1.seconds)(timer))
      tc.advance(1.second)
      timer.tick()
      intercept[TimeoutException](awaitFuture(runningStitch))
      assert(cache.setCalls == 0)
    }
  }

  test("ValueCache should cache complete Stitches for getOrElseUpdate") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ValueCache(cache)

    val const = Stitch.value(value)

    val s = sCache.getOrElseUpdate(key)(const)
    assert(cache.getOrElseUpdateCalls == 0)
    await(s)
    assert(cache.getOrElseUpdateCalls == 1)
    assert(sCache.get(key).map(await(_)).contains(value))
  }

  test("ValueCache shouldn't cache incomplete Stitches for getOrElseUpdate") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ValueCache(cache)

    val s = sCache.getOrElseUpdate(key)(Stitch.Never)
    assert(cache.getOrElseUpdateCalls == 0)
    intercept[TimeoutException](await(s.within(1.seconds)(new JavaTimer)))
    assert(cache.getOrElseUpdateCalls == 0)
  }

  test("ValueCache should get for getOrElseUpdate") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ValueCache(cache)

    await(sCache.set(key, Stitch.value(value)))

    val s = sCache.getOrElseUpdate(key)(Stitch.Never)
    assert(cache.getOrElseUpdateCalls == 0)
    assert(await(s) == value)
    assert(cache.getOrElseUpdateCalls == 0)
  }

  test("ValueCache should cache complete Stitches for getOrElseUpdateAsync") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ValueCache(cache)

    val const = Stitch.value(value)

    val s = sCache.getOrElseUpdateAsync(key)(const)
    assert(cache.getOrElseUpdateCalls == 0)
    assert(await(s.liftNotFoundToOption).isEmpty)
    assert(cache.getOrElseUpdateCalls == 1)
    assert(sCache.get(key).map(await(_)).contains(value))
  }

  test("ValueCache shouldn't cache incomplete Stitches for getOrElseUpdateAsync") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = ValueCache(cache)

    val s = sCache.getOrElseUpdateAsync(key)(Stitch.value(value))
    assert(cache.getOrElseUpdateCalls == 0)
    assert(await(s.liftNotFoundToOption).isEmpty)
    assert(cache.getOrElseUpdateCalls == 1)
    assert(sCache.get(key).map(await(_)).contains(value))
  }

  test("ValueCache should get for getOrElseUpdateAsync") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = new ValueCache(cache)

    await(sCache.set(key, Stitch.value(value)))

    val s = sCache.getOrElseUpdateAsync(key)(Stitch.Never)
    assert(cache.getOrElseUpdateCalls == 0)
    assert(await(s) == value)
    assert(cache.getOrElseUpdateCalls == 0)
  }

  test("ValueCache should wrap an arrow in a cache") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = new ValueCache(cache)

    var ran = 0
    val arrow = sCache.wrap(Arrow.map[String, String] { s => ran += 1; s })

    intercept[MockException](await(arrow(Throw(MockException()))))
    assert(ran == 0)
    assert(await(arrow(Return(value))) == value)
    assert(ran == 1)
    assert(await(arrow(Return(value))) == value)
    assert(ran == 1)
  }

  test("ValueCache should wrap an arrow in an async cache") {
    val cache = new TestableStitchCache[String, String]()
    val sCache = new ValueCache(cache)

    var ran = 0
    val arrow = sCache.wrapAsync(Arrow.map[String, String] { s => ran += 1; s })

    intercept[MockException](await(arrow(Throw(MockException()))))
    assert(ran == 0)
    assert(await(arrow(Return(value)).liftNotFoundToOption).isEmpty)
    assert(ran == 1)
    assert(await(arrow(Return(value))) == value)
    assert(ran == 1)
  }

  test("AsyncValueCache delegates to ValueCache") {
    val input = Stitch.value(value)
    val inputArrow = Arrow.identity[String]

    val expected = Stitch.value("expected")
    val expectedArrow = Arrow.map[String, String](_ => "expected")
    val expectedAsyncArrow = Arrow.map[String, String](_ => "expectedAsync")

    // ValueCache that returns the expected values and verifies the inputs are the expected inputs
    val vCache = new ValueCache[String, String](new TestableStitchCache[String, String]()) {
      override def getOrElseUpdateAsync(key: String)(compute: => Stitch[String]): Stitch[String] = {
        assert(key == key)
        assert(compute eq input)
        expected
      }
      override def wrap(a: Arrow[String, String]): Arrow[String, String] = {
        assert(a eq inputArrow)
        expectedArrow
      }
      override def wrapAsync(a: Arrow[String, String]): Arrow[String, String] = {
        assert(a eq inputArrow)
        expectedAsyncArrow
      }
    }
    val asyncCache = new AsyncValueCache(vCache)

    // each method delegates to the correct underlying method
    assert(asyncCache.getOrElseUpdate(key)(input) eq expected)
    assert(asyncCache.wrap(inputArrow) eq expectedArrow)
    assert(asyncCache.wrapAsync(inputArrow) eq expectedAsyncArrow)
  }
}
