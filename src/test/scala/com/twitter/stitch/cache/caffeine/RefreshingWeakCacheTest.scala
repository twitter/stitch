package com.twitter.stitch.cache.caffeine

import com.twitter.stitch.Arrow
import com.twitter.stitch.Stitch
import com.twitter.stitch.helpers.AwaitHelpers
import com.twitter.util.MockTimer
import com.twitter.util.Time
import com.twitter.conversions.DurationOps._
import com.twitter.stitch.cache.caffeine.RefreshingWeakCache.BaseObserver
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.Return
import com.twitter.util.Try
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.funsuite.AnyFunSuite

class RefreshingWeakCacheTest extends AnyFunSuite with AwaitHelpers {

  test("initial fetch") {
    new Context {
      val arrow = cached(k)
      assert(run(arrow) == v)
      checkUpdates(fetches = List((k, None)), transforms = List((k, u)))
    }
  }

  test("refresh") {
    Time.withCurrentTimeFrozen { t =>
      new Context {
        val arrow = cached(k)

        val u0 = u
        val u1 = "u1"
        val v0 = v
        val v1 = "v1"

        assert(run(arrow) == v0)
        checkUpdates(fetches = List((k, None)), transforms = List((k, u0)))

        u = u1
        v = v1

        // no refresh yet
        assert(run(arrow) == v0)
        checkUpdates(fetches = List.empty, transforms = List.empty)

        // refresh
        t.advance(interval)
        timer.tick()

        assert(run(arrow) == v1)
        checkUpdates(fetches = List((k, Some(u0))), transforms = List((k, u1)))
      }
    }
  }

  test("refresh same value") {
    Time.withCurrentTimeFrozen { t =>
      new Context {
        val arrow = cached(k)

        assert(run(arrow) == v)
        checkUpdates(fetches = List((k, None)), transforms = List((k, u)))

        // no refresh yet
        assert(run(arrow) == v)
        checkUpdates(fetches = List.empty, transforms = List.empty)

        // refresh
        t.advance(interval)
        timer.tick()

        assert(run(arrow) == v)
        checkUpdates(fetches = List((k, Some(u))), transforms = List.empty)
      }
    }
  }

  test("refresh failure") {
    Time.withCurrentTimeFrozen { t =>
      new Context {

        var fail = false

        override lazy val fetch = Arrow.flatMap[(String, Option[String]), String] {
          case (kk, uu) =>
            fetches :+= ((kk, uu))

            if (fail)
              Stitch.exception(new Exception)
            else
              Stitch.value(u)
        }

        val arrow = cached(k)
        assert(run(arrow) == v)
        checkUpdates(fetches = List((k, None)), transforms = List((k, u)))

        // ok refresh
        u = "u1"
        v = "v1"
        t.advance(interval)
        timer.tick()
        assert(run(arrow) == v)
        checkUpdates(fetches = List((k, Some("u0"))), transforms = List((k, u)))

        // failed refresh
        u = "u2"
        v = "v2"
        fail = true
        t.advance(interval)
        timer.tick()
        assert(run(arrow) == "v1")
        checkUpdates(fetches = List((k, Some("u1"))), transforms = List.empty)

        // ok refresh
        u = "u3"
        v = "v3"
        fail = false
        t.advance(interval)
        timer.tick()
        assert(run(arrow) == v)
        checkUpdates(fetches = List((k, Some("u1"))), transforms = List((k, u)))
      }
    }
  }

  test("parallel fetch and refresh") {
    Time.withCurrentTimeFrozen { t =>
      new Context {
        val u0 = u
        val u1 = "u1"
        val v0 = v
        val v1 = "v1"

        val arrow1 = cached(k)
        val arrow2 = cached(k)
        assert(run(arrow1) == v)
        assert(run(arrow2) == v)
        checkUpdates(fetches = List((k, None)), transforms = List((k, u)))

        u = u1
        v = v1

        // no refresh yet
        assert(run(arrow1) == v0)
        assert(run(arrow2) == v0)
        checkUpdates(fetches = List.empty, transforms = List.empty)

        // refresh
        t.advance(interval)
        timer.tick()

        assert(run(arrow1) == v1)
        assert(run(arrow2) == v1)
        checkUpdates(fetches = List((k, Some(u0))), transforms = List((k, u1)))
      }
    }
  }

  test("weak ref not expired") {
    new Context {
      var arrow = cached(k)
      assert(run(arrow) == v)
      checkUpdates(fetches = List((k, None)), transforms = List((k, u)))

      arrow = cached(k)
      assert(run(arrow) == v)
      checkUpdates(fetches = List.empty, transforms = List.empty)
    }
  }

  test("weak ref expired") {
    new Context {
      var arrow = cached(k)
      assert(run(arrow) == v)
      checkUpdates(fetches = List((k, None)), transforms = List((k, u)))

      arrow = null
      System.gc()

      arrow = cached(k)
      assert(run(arrow) == v)
      checkUpdates(fetches = List((k, None)), transforms = List((k, u)))
    }
  }

  test("refresh loop continues while the cache is not GCed") {
    Time.withCurrentTimeFrozen { t =>
      new Context {
        var arrow = cached(k)
        assert(run(arrow) == v)
        checkUpdates(fetches = List((k, None)), transforms = List((k, u)))

        arrow = null
        System.gc()
        t.advance(interval)
        timer.tick()

        assert(timer.tasks.nonEmpty)
        checkUpdates(fetches = List.empty, transforms = List.empty)
      }
    }
  }

  test("refresh loop stops when the cache is GCed") {
    Time.withCurrentTimeFrozen { t =>
      new Context {
        var arrow = cached(k)
        assert(run(arrow) == v)
        checkUpdates(fetches = List((k, None)), transforms = List((k, u)))

        arrow = null
        cached = null
        System.gc()
        t.advance(interval)
        timer.tick()

        assert(timer.tasks.isEmpty)
        checkUpdates(fetches = List.empty, transforms = List.empty)
      }
    }
  }

  test("individual keys stop being refreshed when their arrow is GCed") {
    val k2 = "k2"
    Time.withCurrentTimeFrozen { t =>
      new Context {
        var arrow1 = cached(k)
        val arrow2 = cached(k2)
        assert(run(arrow1) == v)
        assert(run(arrow2) == v)
        checkUpdates(fetches = List((k, None), (k2, None)), transforms = List((k, u), (k2, u)))

        arrow1 = null
        System.gc()
        t.advance(interval)
        timer.tick()

        assert(timer.tasks.nonEmpty)
        // Still refresh k2, but not k
        checkUpdates(fetches = List((k2, Some(u))), transforms = List.empty)
      }
    }
  }

  test("doesn't cache a failed initialization") {
    new Context {

      var fail = false

      override lazy val fetch = Arrow.flatMap[(String, Option[String]), String] {
        case (kk, uu) =>
          fetches :+= ((kk, uu))

          if (fail)
            Stitch.exception(new Exception)
          else
            Stitch.value(u)
      }

      // initial failed fetch
      fail = true
      val arrow = cached(k)
      assert(Try(run(arrow)).isThrow)
      checkUpdates(fetches = List((k, None)), transforms = List.empty)

      // re-fetches if the initialization failed
      fail = false
      val arrow2 = cached(k)
      assert(run(arrow) == v)
      assert(run(arrow2) == v)
      checkUpdates(fetches = List((k, None)), transforms = List((k, u)))
    }
  }

  test("handles multiple failed initializations") {
    new Context {

      var fail = false

      override lazy val fetch = Arrow.flatMap[(String, Option[String]), String] {
        case (kk, uu) =>
          fetches :+= ((kk, uu))

          if (fail)
            Stitch.exception(new Exception)
          else
            Stitch.value(u)
      }

      // initial failed fetch
      fail = true
      val arrow1 = cached(k)
      assert(Try(run(arrow1)).isThrow)
      checkUpdates(fetches = List((k, None)), transforms = List.empty)

      // second failed fetch
      fail = true
      val arrow2 = cached(k)
      assert(Try {
        // Run these concurrently so there's only one fetch
        await(Stitch.join(arrow1(()), arrow2(())))
      }.isThrow)
      checkUpdates(fetches = List((k, None)), transforms = List.empty)
      // And then check for two more fetches (because these are not run concurrently)
      assert(Try(run(arrow1)).isThrow)
      assert(Try(run(arrow2)).isThrow)
      checkUpdates(fetches = List((k, None), (k, None)), transforms = List.empty)

      // re-fetches
      fail = false
      assert(run(arrow1) == v)
      assert(run(arrow2) == v)
      val arrow3 = cached(k)
      assert(run(arrow3) == v)
      checkUpdates(fetches = List((k, None)), transforms = List((k, u)))
    }
  }

  test("batching is preserved") {
    Time.withCurrentTimeFrozen { t =>
      new Context {
        val arrow = Arrow.map { _: Int => () }.andThen(cached(k)).andThen(Arrow.ExtractBatch)

        // The ExtractBatch arrow should get all 3 keys, indicating that the cached(k) arrow didn't
        // split up the batch
        val expectedBatch = Seq(v, v, v).map(Return(_))
        assert(
          await(arrow.traverse(Seq(1, 2, 3))) == Seq(
            expectedBatch,
            expectedBatch,
            expectedBatch
          ))

        // Now, change the value
        u = "u2"
        v = "v2"
        t.advance(interval)
        timer.tick()

        // The ExtractBatch arrow should get all 3 keys, indicating that the cached(k) arrow didn't
        // split up the batch
        val expectedBatch2 = Seq(v, v, v).map(Return(_)) // use new v
        assert(
          await(arrow.traverse(Seq(1, 2, 3))) == Seq(
            expectedBatch2,
            expectedBatch2,
            expectedBatch2
          ))
      }
    }
  }

  test("failures are not refreshed concurrently") {
    val concurrency = 1000
    new Context {
      var fail = true
      val fetchCalls = new AtomicInteger(0)
      val refreshes = new AtomicInteger(0)

      override lazy val fetch = Arrow.flatMap[(String, Option[String]), String] {
        case (kk, uu) =>
          fetchCalls.incrementAndGet()
          fetches :+= ((kk, uu))

          if (fail)
            Stitch.exception(new Exception)
          else
            Stitch.value(u)
      }

      cached = RefreshingWeakCache(
        fetch,
        transform,
        interval,
        new BaseObserver {
          override def willRefresh(key: Any): Unit = refreshes.incrementAndGet()
        })(timer)

      val arrow = cached(k)
      assert(Try(run(arrow)).isThrow)
      assert(fetchCalls.get() == 1)
      assert(refreshes.get() == 1)

      fail = false
      val barrier = new CyclicBarrier(concurrency)
      val futures = (1 to concurrency).map { _ =>
        FuturePool.unboundedPool {
          barrier.await()
          Stitch.run(arrow(()))
        }.flatten
      }
      val results = awaitFuture(Future.collect(futures))
      assert(results.size == concurrency)
      results.foreach { entry =>
        assert(entry == v)
      }
      assert(fetchCalls.get() == 2) // Only one more fetch
      assert(refreshes.get() == 2) // Check that the observer is also only updated once
    }
  }

  class Context {
    var k = "k0"
    var u = "u0"
    var v = "v0"
    val interval = 1.seconds
    implicit val timer = new MockTimer
    var fetches = List.empty[(String, Option[String])]
    lazy val fetch = Arrow.map[(String, Option[String]), String] {
      case (kk, uu) =>
        fetches :+= ((kk, uu))
        u
    }
    var transforms = List.empty[(String, String)]
    val transform: (String, String) => String = {
      case (kk, uu) =>
        transforms :+= ((kk, uu))
        v
    }
    var cached = RefreshingWeakCache(fetch, transform, interval)
    def checkUpdates(
      fetches: List[(String, Option[String])],
      transforms: List[(String, String)]
    ) = {
      assert(this.fetches == fetches)
      assert(this.transforms == transforms)
      this.fetches = List.empty
      this.transforms = List.empty
    }
  }

  def run[V](a: Arrow[Unit, V]): V = await(a(()))
}
