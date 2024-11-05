package com.twitter.stitch.cache

import com.twitter.conversions.DurationOps._
import com.twitter.stitch.Stitch
import com.twitter.stitch.Stitch.SynchronizedRef
import com.twitter.stitch.helpers.AwaitHelpers
import com.twitter.util.Duration
import com.twitter.util.Time
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import com.twitter.util.mock.Mockito

class RefreshTest extends AnyFunSuite with Mockito with AwaitHelpers {

  class Fixture {
    val provider: () => Stitch[Int] = mock[() => Stitch[Int]]
    val ttl: Duration = 1.minute
    val memoizedStitch: () => Stitch[Int] = Refresh.every(ttl) { provider() }
  }

  case class MockException() extends Exception()

  test("Refresh should call the provider on the first request but not on second request") {
    val fixture = new Fixture
    import fixture._

    when(provider())
      .thenReturn(Stitch.value(1))
      .thenReturn(Stitch.value(2))

    val result1 = memoizedStitch()
    assert(await(result1) == 1)
    verify(provider, times(1))()
    val result2 = memoizedStitch()
    assert(await(result2) == 1)
    verify(provider, times(1))()
  }

  test("Refresh should call the provider after timeout, but only once") {
    val fixture = new Fixture
    import fixture._

    when(provider())
      .thenReturn(Stitch.value(1))
      .thenReturn(Stitch.value(2))

    Time.withTimeAt(Time.fromMilliseconds(0)) { timeControl =>
      assert(await(memoizedStitch()) == 1)
      timeControl.advance(ttl - 1.millis)
      assert(await(memoizedStitch()) == 1)
      timeControl.advance(2.millis)
      assert(await(memoizedStitch()) == 2)
      assert(await(memoizedStitch()) == 2)
      verify(provider, times(2))()
    }
  }

  test("Refresh shouldn't save failures") {
    val fixture = new Fixture
    import fixture._

    when(provider())
      .thenReturn(Stitch.exception(MockException()))
      .thenReturn(Stitch.value(2))

    intercept[MockException](await(memoizedStitch()))
    assert(await(memoizedStitch()) == 2)
    verify(provider, times(2))()
  }

  test("Refresh returns SychronizedRefs which handle concurrent calls") {
    val fixture = new Fixture
    import fixture._

    val result1 = memoizedStitch()
    val result2 = memoizedStitch()

    // calls when the value hasn't changed return the same Stitch
    assert(result1.eq(result2))
    // and the returned Stitch is a SychronizedRef
    assert(memoizedStitch().isInstanceOf[SynchronizedRef[_]])
  }

  test("Refresh calls `provider` lazily") {
    val fixture = new Fixture
    import fixture._

    when(provider()).thenReturn(Stitch.value(1))

    val result = memoizedStitch()
    verify(provider, times(0))()
    await(result)
    verify(provider, times(1))()
  }

  test("Refresh should only call the provider once, even with concurrent requests") {
    val fixture = new Fixture
    import fixture._

    when(provider())
      .thenReturn(Stitch.value(1))
      .thenReturn(Stitch.value(2))

    val result1 = memoizedStitch()
    val result2 = memoizedStitch()

    assert(result1.eq(result2))

    assert(await(result1) == 1)
    assert(await(result2) == 1)

    verify(provider, times(1))()
  }

  test(
    "Refresh should only call the provider once, even with concurrent requests, when refreshing") {
    val fixture = new Fixture
    import fixture._

    Time.withCurrentTimeFrozen { tc =>
      when(provider())
        .thenReturn(Stitch.value(1))
        .thenReturn(Stitch.value(2))

      assert(await(memoizedStitch()) == 1)
      verify(provider, times(1))()

      tc.advance(ttl - 1.millisecond)

      assert(await(memoizedStitch()) == 1)
      verify(provider, times(1))()

      tc.advance(2.milliseconds)

      val result1 = memoizedStitch()
      val result2 = memoizedStitch()

      // should be the same Stitch instance
      assert(result1.eq(result2))

      assert(await(result1) == 2)
      assert(await(result2) == 2)

      verify(provider, times(2))()
    }
  }

  test("Refresh should fail concurrent requests if the actual request fails") {
    val fixture = new Fixture
    import fixture._

    when(provider())
      .thenReturn(Stitch.exception(MockException()))
      .thenReturn(Stitch.value(2))

    val result1 = memoizedStitch()
    val result2 = memoizedStitch()

    // should be the same Stitch instance
    assert(result1.eq(result2))

    intercept[MockException](await(result1))
    intercept[MockException](await(result2))
    verify(provider, times(1))()

    assert(await(memoizedStitch()) == 2)
    verify(provider, times(2))()
  }
}
