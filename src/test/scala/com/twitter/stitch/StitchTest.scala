package com.twitter.stitch

import com.twitter.conversions.DurationOps._
import com.twitter.stitch.Stitch.Const
import com.twitter.stitch.Stitch.SynchronizedRef
import com.twitter.stitch.helpers.StitchTestSuite
import com.twitter.util._

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class StitchTest extends StitchTestSuite {

  test("value") {
    await(Stitch.value("foo")) must equal("foo")
  }

  test("apply") {
    await(Stitch { "foo" }) must equal("foo")
    val exc = new Exception
    val s = Stitch { throw exc }
    intercept[Exception] { await(s) } must equal(exc)
  }

  test("unit") {
    await(Stitch.value("foo").unit) must equal(())
  }

  test("collect") {
    await(
      Stitch.collect(
        Seq(
          Stitch.value("foo"),
          Stitch.value("bar")
        )
      )
    ) must equal(Seq("foo", "bar"))

    await(
      Stitch.collect(
        Map(
          "g" -> Stitch.value(5),
          "r" -> Stitch.value(6)
        )
      )
    ) must equal(Map("g" -> 5, "r" -> 6))
  }

  test("collectToTry") {
    val e = new Exception("bar")
    await(
      Stitch.collectToTry(
        Seq(
          Stitch.value("foo"),
          Stitch.exception(e)
        )
      )
    ) must equal(Seq(Return("foo"), Throw(e)))
  }

  test("join returns exception without waiting for other branch") {
    intercept[Exception] {
      await(
        Stitch.join(
          Stitch.callFuture(new Promise[Unit]()),
          Stitch.exception(new Exception())
        )
      )
    }
  }

  test("join returns exception without simplifying later branch") {
    var simplified = false
    intercept[Exception] {
      await(
        Stitch.join(
          Stitch.exception(new Exception()),
          Stitch.value(()).ensure { simplified = true }
        )
      )
    }
    simplified must equal(false)
  }

  test("map") {
    await(
      Stitch.value("foo") map { _.reverse }
    ) must equal("oof")
  }

  test("map fails") {
    val s = Stitch.value("foo") map { x => throw new Exception() }

    intercept[Exception] { await(s) }
  }

  test("flatMap") {
    await(
      Stitch.value("foo") flatMap { x => Stitch.value(x.reverse) }
    ) must equal("oof")
  }

  test("flatMap fails") {
    val s = Stitch.value("foo") flatMap { x => throw new Exception() }

    intercept[Exception] { await(s) }
  }

  test("flatten") {
    val s = Stitch.value(Stitch.value("foo")).flatten
    await(s) must equal("foo")
  }

  test("filter") {
    await(
      for {
        Some(x) <- Stitch.value(Some("foo"))
      } yield x
    ) must equal("foo")
  }

  test("filter fails") {
    intercept[MatchError] {
      await(
        for {
          Some(x) <- Stitch.value[Option[_]](None)
        } yield x
      )
    }
  }

  test("handle success") {
    await(
      Stitch.value("foo") handle { case e => "bar" }
    ) must equal("foo")
  }

  test("handle failure") {
    await(
      Stitch.exception(new Exception()) handle { case e => "bar" }
    ) must equal("bar")
  }

  test("partial handle failure") {
    await(
      Stitch.exception(new Exception("exp")) handle {
        case e: Exception if e.getMessage == "exp" => "bar"
      }
    ) must equal("bar")

    try {
      await(
        Stitch.exception(new Exception()) handle {
          case e: Exception if e.getMessage == "exp" => "bar"
        }
      )
    } catch {
      case e: MatchError => throw e
      case _: Throwable =>
    }
  }

  test("rescue success") {
    await(
      Stitch.value("foo") rescue { case e => Stitch.value("bar") }
    ) must equal("foo")
  }

  test("rescue failure") {
    await(
      Stitch.exception(new Exception()) rescue { case e => Stitch.value("bar") }
    ) must equal("bar")
  }

  test("partial rescue") {
    await(
      Stitch.exception(new Exception("exp")) rescue {
        case e: Exception if e.getMessage == "exp" => Stitch.value("bar")
      }
    ) must equal("bar")

    try {
      await(
        Stitch.exception(new Exception()) rescue {
          case e: Exception if e.getMessage == "exp" => Stitch.value("bar")
        }
      )
    } catch {
      case e: MatchError => throw e
      case _: Throwable =>
    }
  }

  test("transform success") {
    await(Stitch.value("foo").transform {
      case Return(t) => Stitch.value(t)
      case Throw(e) => Stitch.value("exn")
    }) must equal("foo")
  }

  test("transform failure") {
    await(Stitch.exception(new Exception()).transform {
      case Return(t) => Stitch.value(t)
      case Throw(e) => Stitch.value("exn")
    }) must equal("exn")
  }

  test("ensure on failure") {
    var i = 0
    Await.ready(Stitch.run(Stitch.exception(new Exception()) ensure { i += 1 }))
    i must equal(1)
  }

  test("ensure on success") {
    var i = 0
    val result = await(Stitch.value("foo") ensure { i += 1 })
    result must equal("foo")
    i must equal(1)
  }

  test("respond on failure") {
    var i = 0
    Await.ready(
      Stitch.run(
        Stitch.exception(new Exception()) respond {
          case Return(t) => i += 2
          case Throw(e) => i += 1
        }
      )
    )
    i must equal(1)
  }

  test("respond on success") {
    var i = 0
    val result = await(
      Stitch.value("foo") respond {
        case Return(t) => i += 2
        case Throw(e) => i += 1
      }
    )
    result must equal("foo")
    i must equal(2)
  }

  test("onSuccess on failure") {
    var i = 1
    Await.ready(Stitch.run(Stitch.exception[Int](new Exception()) onSuccess { r => i += r }))
    i must equal(1)
  }

  test("onSuccess on success") {
    var i = 1
    val result = await(Stitch.value(5) onSuccess { r => i += r })
    result must equal(5)
    i must equal(6)
  }

  test("onFailure on failure") {
    var i = ""
    Await.ready(Stitch.run(Stitch.exception(new Exception("fail")) onFailure { e =>
      i += e.getMessage
    }))
    i must equal("fail")
  }

  test("onFailure on success") {
    var i = ""
    val result = await(Stitch.value(5) onFailure { e => i += e.getMessage })
    result must equal(5)
    i must equal("")
  }

  test("applyEffect on success") {
    var i = 1
    val result = await(Stitch.value(5).applyEffect(x => Stitch.value({ i = x })))
    result must equal(5)
    i must equal(5)
  }

  test("applyEffect on failure") {
    var i = 1
    val ex = new Exception("fail")
    intercept[Exception] {
      await(Stitch.exception[Int](ex).applyEffect(x => Stitch.value({ i = x })))
    }
    i must equal(1)
  }

  test("applyEffect fails") {
    val ex = new Exception("fail")
    intercept[Exception] {
      await(Stitch.value(5).applyEffect(x => Stitch.exception(ex)))
    }
  }

  test("liftToTry success") {
    await(Stitch.value("foo").liftToTry) must equal(Return("foo"))
  }

  test("liftToTry failure") {
    val e = new Exception()
    await(Stitch.exception(e).liftToTry) must equal(Throw(e))
  }

  test("lowerFromTry success") {
    await(Stitch.value(Return("foo")).lowerFromTry) must equal("foo")
  }

  test("lowerFromTry failure") {
    val e = new Exception()
    val e1 = intercept[Exception] {
      await(Stitch.value(Throw(e)).lowerFromTry)
    }
    assert(e == e1)
  }

  test("lowerFromTry exception") {
    val e = new Exception()
    val e1 = intercept[Exception] {
      await((Stitch.exception(e): Stitch[Try[Nothing]]).lowerFromTry)
    }
    assert(e == e1)
  }

  test("liftToOption Some") {
    await(Stitch.value("foo").liftToOption()) must equal(Some("foo"))
  }

  test("liftToOption None") {
    val e = new Exception()
    await(Stitch.exception(e).liftToOption({ case e2 => e2 eq e })) must equal(None)
  }

  test("liftToOption failure") {
    val e = new Exception()
    intercept[Exception] {
      await(Stitch.exception(e).liftToOption({ case e2 => e2 ne e }))
    }
  }

  test("liftNotFoundToOption Some") {
    await(Stitch.value("foo").liftNotFoundToOption) must equal(Some("foo"))
  }

  test("liftNotFoundToOption None") {
    await(Stitch.NotFound.liftNotFoundToOption) must equal(None)
  }

  test("liftNotFoundToOption failure") {
    val e = new Exception()
    intercept[Exception] {
      await(Stitch.exception(e).liftNotFoundToOption)
    }
  }

  test("lowerFromOption Some") {
    val e = new Exception()
    await(Stitch.value(Some("foo")).lowerFromOption(e)) must equal("foo")
  }

  test("lowerFromOption None") {
    val e = new Exception()
    an[Exception] mustBe thrownBy {
      await(Stitch.value(None).lowerFromOption(e))
    }
  }

  test("lowerFromOption failure") {
    class OriginalException extends Exception
    val oe = new OriginalException()
    val e = new Exception()
    intercept[OriginalException] {
      await(Stitch.exception(oe).lowerFromOption(e))
    }
  }

  test("NonLocalReturnControl") {
    def method(): Stitch[Unit] = Stitch { return Stitch.Unit }
    val s = method()
    intercept[StitchNonLocalReturnControl] { await(s) }

    val s1 = Stitch.Unit.flatMap(_ => method())
    intercept[StitchNonLocalReturnControl] { await(s1) }
  }

  test("transform of transform becomes a single Transform") {
    val s = Stitch.future(new Promise[Int])
    s.map { _ + 1 }.map { _ + 2 } match {
      case Stitch.Transform(s2, _, _) if s2 eq s =>
      case _ => fail("not a single Transform")
    }
  }

  test("transform of const does not apply transformation") {
    Stitch.value("foo").map { _ + "bar" } match {
      case Stitch.Transform(_, _, _) =>
      case _ => fail("Const")
    }
  }

  test("transform of transform simplifies to a single Transform") {
    val p1 = new Promise[Int]
    val p2 = new Promise[Int]
    val s1 = Stitch.future(p1)
    val s2 = Stitch.future(p2)
    val s = for { s1 <- s1; s2 <- s2 } yield (s1 + s2)
    p1() = Return(7)
    s.simplify(null) match {
      case Stitch.Transform(s, _, _) if s eq s2 =>
      case _ => fail("not a single Transform")
    }
  }

  test("interrupted callFuture") {
    val ise = new IllegalStateException
    val s1 = Stitch.callFuture(throw ise)
    awaitFuture(Stitch.run(s1).liftToTry) must equal(Throw(ise))

    val ie = new InterruptedException
    val s2 = Stitch.callFuture(throw ie)
    awaitFuture(Stitch.run(s2).liftToTry) must equal(Throw(ie))
  }

  test("callFuture") {
    await(
      Stitch.join(
        Stitch.callFuture(Future.value("foo")),
        Stitch.callFuture(Future.value("bar"))
      )
    ) must equal(("foo", "bar"))
  }

  test("callFuture is rerun each time it's called") {
    val timer = new JavaTimer()
    var count = 0
    val s = Stitch.callFuture(Future.sleep(1.millisecond)(timer).map(_ => count += 1))
    await(s.before(s))
    assert(count == 2)
    await(s)
    assert(count == 3)
    await(s)
    assert(count == 4)
  }

  test("callFuture is atomic and can be rerun") {
    val p = new Promise[Boolean]()
    val s = Stitch.callFuture(p)
    val f = Stitch.run(s)
    p.setValue(true)
    assert(awaitFuture(f))
    assert(await(s))
  }

  test("concurrent callFutures get the right locals") {
    val local = new Local[Boolean]
    val cf = Stitch.callFuture(Future.value(local()))
    val s = Stitch.join(
      cf,
      Stitch.let(local)(true)(cf),
      Stitch.let(local)(false)(cf)
    )
    assert(await(s) == ((None, Some(true), Some(false))))
  }

  test("hangs if Pending.clear() returns a lazy Seq") {
    val s =
      Stitch.traverse(Seq("foo", "bar")) { arg =>
        Stitch.join(Stitch.callFuture(Future.value("baz")), Stitch.callFuture(Future.value("quux")))
      }
    await(s) must equal(Seq(("baz", "quux"), ("baz", "quux")))
  }

  def or(a: Stitch[Boolean], b: Stitch[Boolean]): Stitch[Boolean] = {
    Stitch.partialJoinMap(a, b) {
      case (Some(false), Some(false)) => Some(false)
      case (Some(true), _) => Some(true)
      case (_, Some(true)) => Some(true)
      case _ => None
    }
  }

  def or3(a: Stitch[Boolean], b: Stitch[Boolean], c: Stitch[Boolean]) = {
    Stitch.partialJoinMap(a, b, c) {
      case (Some(false), Some(false), Some(false)) => Some(false)
      case (Some(true), _, _) => Some(true)
      case (_, Some(true), _) => Some(true)
      case (_, _, Some(true)) => Some(true)
      case _ => None
    }
  }

  test("partialJoinMap of 3 stitches") {
    Await.result(
      Stitch.run(or3(Stitch.value(true), Stitch.value(false), Stitch.value(true)))) must equal(true)
    Await.result(
      Stitch.run(or3(Stitch.value(false), Stitch.value(true), Stitch.value(false)))) must equal(
      true)
    Await.result(
      Stitch.run(or3(Stitch.value(false), Stitch.value(false), Stitch.value(false)))) must equal(
      false)
    // short-circuit expensive stitch
    Await.result(
      Stitch.run(
        or3(
          Stitch.callFuture(FuturePool.unboundedPool {
            Thread.sleep(10000)
            throw new RuntimeException
          }),
          Stitch.value(true),
          Stitch.value(false)))) must equal(true)
    Await.result(
      Stitch.run(
        or3(
          Stitch.callFuture(Future.value(false)).flatMap(_ => Stitch.value(true)),
          Stitch.value(false),
          Stitch.value(false)
        )
      )
    ) must equal(true)
  }

  test("partialJoinMap of stitches") {
    Await.result(Stitch.run(or(Stitch.value(true), Stitch.value(false)))) must equal(true)
    Await.result(Stitch.run(or(Stitch.value(false), Stitch.value(true)))) must equal(true)
    Await.result(Stitch.run(or(Stitch.value(false), Stitch.value(false)))) must equal(false)
  }

  test("partialJoinMap of complex stitches") {
    val a = Stitch.value(false)
    val b = Stitch.callFuture(Future.value(true)).flatMap(_ => Stitch.value(false))

    Await.result(Stitch.run(or(a, b))) must equal(false)
  }

  class PartialJoinMapError extends Exception
  test("partialJoinMap of stitches with exception") {
    val a = Stitch.value(false)
    val b = Stitch.value(false)

    val s = Stitch.partialJoinMap(a, b) {
      case _ => throw new PartialJoinMapError()
    }
    intercept[PartialJoinMapError](Await.result(Stitch.run(s)))
  }

  test("partialJoinMap with function k that always return None") {
    val a = Stitch.value(1)
    val b = Stitch.value(2)

    val s = Stitch.partialJoinMap(a, b) {
      case _ => None
    }
    intercept[StitchInvalidState](Await.result(Stitch.run(s)))
  }

  test("collect of batch") {
    val p = Promise[Seq[Int]]
    val s = Stitch.callFuture(p)
    val f = Stitch.run(Stitch.collect(Seq(s.map(_(0)), s.map(_(1)))))
    p.update(Return(Seq(1, 2)))
    Await.result(f) must equal(Seq(1, 2))
  }

  test("collect of non-batch") {
    val p1 = Promise[Int]
    val p2 = Promise[Int]
    val f = Stitch.run(Stitch.collect(Seq(Stitch.callFuture(p1), Stitch.callFuture(p2))))
    p1.update(Return(1))
    p2.update(Return(2))
    Await.result(f) must equal(Seq(1, 2))
  }

  test("collect unsimplifiable") {
    val p = Promise[Seq[Int]]
    val s = Stitch.callFuture(p)
    val p2 = Promise[Int]
    val f =
      Stitch.run(Stitch.join(Stitch.collect(Seq(s.map(_(0)), s.map(_(1)))), Stitch.callFuture(p2)))
    p2.update(Return(3))
    p.update(Return(Seq(1, 2)))
    Await.result(f) must equal((Seq(1, 2), 3))
  }

  test("collect preserves batching") {
    val in = Seq(0, 0, 1)
    val batch = Seq(0, 1)
    val seqGroupCounter = new AtomicInteger(0)

    val seqGroup = SeqGroup[Int, Int] { c =>
      assert(c == batch)
      seqGroupCounter.incrementAndGet()
      Future.value(c.map(item => Return(item + 10)))
    }
    val runnable = Stitch.collect(in.map(i => Stitch.call(i, seqGroup)))

    val result = Await.result(Stitch.run(runnable))
    assert(result == Seq(10, 10, 11))
    assert(seqGroupCounter.get() == 1)
  }

  test("collect within does not break batching") {
    val timer = new MockTimer()
    val in = Seq(0, 0, 1)
    val batch = Seq(0, 1)
    val seqGroupCounter = new AtomicInteger(0)

    val seqGroup = SeqGroup[Int, Int] { c =>
      assert(c == batch)
      seqGroupCounter.incrementAndGet()
      Future.value(c.map(item => Return(item + 10))).delayed(1.second)(timer)
    }
    val runnable =
      Stitch.collectToTry(in.map(i => Stitch.call(i, seqGroup).within(2.second)(timer)))

    Time.withCurrentTimeFrozen { tc =>
      val running = Stitch.run(runnable)
      assert(!running.isDefined)
      tc.advance(1.seconds)
      timer.tick()
      val result = Await.result(running)
      assert(result == Seq(Return(10), Return(10), Return(11)))
      assert(seqGroupCounter.get() == 1)
    }
  }

  test("interrupts are propagated") {
    val p1 = Promise[Int]
    val p2 = Promise[Int]
    val f = Stitch.run(Stitch.collect(Seq(Stitch.callFuture(p1), Stitch.callFuture(p2))))
    f.raise(new Exception())
    (p1.isInterrupted, p2.isInterrupted) match {
      case (Some(_), Some(_)) =>
      case _ => fail("interrupts not propagated")
    }
  }

  test("computation continues after interrupt") {
    // usual interrupt behavior for Finagle RPC calls
    val p1 = Promise[Int]
    p1.setInterruptHandler { case e => p1.updateIfEmpty(Throw(e)) }
    val p2 = Promise[Int]
    p2.setInterruptHandler { case e => p2.updateIfEmpty(Throw(e)) }

    val s = Stitch.callFuture(p1).rescue { case _ => Stitch.callFuture(p2) }
    val f = Stitch.run(s)
    f.raise(new Exception())
    p2.setValue(7)

    Await.result(f) must equal(7)
  }

  test("non-ref executes more than once when used more than once") {
    var count = 0
    val s = Stitch.call(
      (),
      new SeqGroup[Unit, Unit] {
        def run(keys: Seq[Unit]) = {
          count += 1
          Future.value(Seq(Return.Unit))
        }
      })
    await(s.flatMap { _ => s })
    count must equal(2)
  }

  test("ref executes only once when used more than once") {
    var count = 0
    val s = Stitch.ref(
      Stitch.call(
        (),
        new SeqGroup[Unit, Unit] {
          def run(keys: Seq[Unit]) = {
            count += 1
            Future.value(Seq(Return.Unit))
          }
        }))
    await(s.flatMap { _ => s })
    count must equal(1)
  }

  test("SynchronizedRef can be re-used across Stitch runs") {
    val p1 = new Promise[String]()
    val p2 = new Promise[String]()
    val s1 = Stitch.synchronizedRef(Stitch.callFuture(p1))
    val s2 = Stitch.synchronizedRef(Stitch.callFuture(p2))
    val forward = s1.flatMap(str => s2.map(str ++ _))
    val backward = s2.flatMap(str => s1.map(str ++ _))
    val forwardRun = Stitch.run(forward)
    val backwardRun = Stitch.run(backward)
    p1.setValue("one")
    p2.setValue("two")
    awaitFuture(forwardRun) must equal("onetwo")
    awaitFuture(backwardRun) must equal("twoone")
  }

  test("synchronizedRef executes only once, other calls wait") {
    val count = new AtomicInteger(0)

    val p = new Promise[Seq[Try[Unit]]]()

    val s = Stitch.synchronizedRef(
      Stitch.call(
        (),
        new SeqGroup[Unit, Unit] {
          def run(keys: Seq[Unit]) = {
            count.incrementAndGet()
            p
          }
        }
      )
    )

    val first = Stitch.run(s)

    first.isDefined mustBe false

    val second = Stitch.run(s)

    first.isDefined mustBe false
    second.isDefined mustBe false
    count.get() must equal(1)

    p.setValue(Seq(Return.Unit))
    first.poll.exists(_.isReturn) mustBe true
    second.poll.exists(_.isReturn) mustBe true
    count.get() must equal(1)
  }

  test("synchronizedRef executes only once when used more than once") {
    // 500 concurrent futures
    val size = 500

    val count = new AtomicInteger(0)

    val s = Stitch.synchronizedRef(
      Stitch.call(
        (),
        new SeqGroup[Unit, Unit] {
          def run(keys: Seq[Unit]) = {
            count.incrementAndGet()
            Future.value(keys.map(_ => Return.Unit))
          }
        }))

    val futures = Array.fill(size)(FuturePool.unboundedPool {
      Stitch.run(s.flatMap { _ => s })
    }.flatten)

    val results = awaitFuture(Future.collect(futures))
    results must have size size
    results must contain only (())
    count.get() must equal(1)
  }

  test("synchronizedRef executes only once when used more than once with any contained Stitch") {
    // 500 concurrent futures
    val size = 500

    val callCount = new AtomicInteger(0)
    val flatMapCount = new AtomicInteger(0)
    val mapCount = new AtomicInteger(0)
    val traverseCount = new AtomicInteger(0)

    val s = Stitch.synchronizedRef(
      Stitch
        .call(
          (),
          new SeqGroup[Unit, Unit] {
            def run(keys: Seq[Unit]) = {
              callCount.getAndIncrement()
              Future.value(keys.map(_ => Return.Unit))
            }
          }).flatMap { _ =>
          flatMapCount.getAndIncrement()
          Stitch
            .async {
              Stitch.traverse(Seq((), (), ())) { _ =>
                traverseCount.getAndIncrement()
                Stitch.Unit
              }
            }.map { _ =>
              mapCount.getAndIncrement()
              ()
            }
        }
    )

    val futures = Array.fill(size)(FuturePool.unboundedPool {
      Stitch.run(Stitch.traverse(Seq.fill(size)(()))(_ => s.flatMap(_ => s)))
    }.flatten)

    val results = awaitFuture(Future.collect(futures))
    results must have size size
    results must contain only Seq.fill(size)(())
    callCount.get() must equal(1)
    flatMapCount.get() must equal(1)
    mapCount.get() must equal(1)
    traverseCount.get() must equal(3)
  }

  test(
    "synchronizedRef executes only once when used more than once with any contained Stitch when used with Stitch.runCached") {
    // 500 concurrent futures
    val size = 500

    val callCount = new AtomicInteger(0)
    val flatMapCount = new AtomicInteger(0)
    val mapCount = new AtomicInteger(0)
    val traverseCount = new AtomicInteger(0)

    val s = Stitch.synchronizedRef(
      Stitch
        .call(
          (),
          new SeqGroup[Unit, Unit] {
            def run(keys: Seq[Unit]) = {
              callCount.getAndIncrement()
              Future.value(keys.map(_ => Return.Unit))
            }
          }).flatMap { _ =>
          flatMapCount.getAndIncrement()
          Stitch
            .async {
              Stitch.traverse(Seq((), (), ())) { _ =>
                traverseCount.getAndIncrement()
                Stitch.Unit
              }
            }.map { _ =>
              mapCount.getAndIncrement()
              ()
            }
        }
    )

    val futures = Array.fill(size)(FuturePool.unboundedPool {
      Stitch.runCached(Stitch.traverse(Seq.fill(size)(()))(_ => s.flatMap(_ => s)))
    }.flatten)

    val results = awaitFuture(Future.collect(futures))
    results must have size size
    results must contain only Seq.fill(size)(())
    callCount.get() must equal(1)
    flatMapCount.get() must equal(1)
    mapCount.get() must equal(1)
    traverseCount.get() must equal(3)
  }

  test("synchronizedRef interrupts propagated to futures") {
    val p1 = Promise[Unit]
    val p2 = Promise[Unit].interruptible()

    val s = Stitch.synchronizedRef(for {
      _ <- Stitch.callFuture(p1)
      s <- Stitch.callFuture(p2)
    } yield s)

    val f1 = Stitch.run(s)
    val e = new Exception

    p1.setDone()
    f1.raise(e)

    assert(intercept[Exception](awaitFuture(p2)).eq(e))
  }

  test("synchronizedRef doesn't leak interrupts") {

    val p1 = Promise[Unit].interruptible()
    val p2 = Promise[Unit]

    val s = Stitch.synchronizedRef(Stitch.callFuture(p1))

    val f1 = FuturePool.unboundedPool { Stitch.run(s) }.flatten
    val f2 = FuturePool.unboundedPool { Stitch.run(s.flatMap(_ => Stitch.callFuture(p2))) }.flatten

    val e = new Exception
    f1.raise(e)

    assert(intercept[Exception](awaitFuture(p1)).eq(e))
    p2.isInterrupted mustBe empty
  }

  test("nested forward references work and simplify correctly") {
    val call = Stitch.call(
      5,
      new SeqGroup[Int, Int] {
        def run(keys: Seq[Int]) = Future.value(keys.map(Return.apply))
      }
    )

    var cache: Stitch[Int] = null
    var forwardRef: Stitch[Int] = null
    val s = Stitch.synchronizedRef(call.onSuccess(_ => cache = forwardRef))
    forwardRef = s

    await(s) mustBe 5

    s mustBe new SynchronizedRef(Const(Return(5)))
    s must be theSameInstanceAs cache
    s must be theSameInstanceAs forwardRef
  }

  test("locals are available") {
    val local = new Local[String]
    implicit val timer = new JavaTimer(true)

    local.set(Some("foo"))

    val s =
      for {
        _ <- Stitch.callFuture(Future.sleep(Duration.fromMilliseconds(5)))
        _ <- Stitch.callFuture(Future.Unit)
      } yield local()

    await(s) must equal(Some("foo"))
  }

  test("locals are available after interrupting") {
    val local = new Local[String]
    implicit val timer = new MockTimer

    val s = Stitch.sleep(1.minute).map(_ => "wrong").handle { case _ => local.apply() }

    Time.withCurrentTimeFrozen { time =>
      val run = local.let("foo") { Stitch.run(s) }
      run.raise(new Exception())
      time.advance(2.minutes)
      Await.result(run) must equal(Some("foo"))
    }
  }

  test("time") {
    Time.withCurrentTimeFrozen { control =>
      val p = Promise[Unit]
      val s = Stitch.time(Stitch.callFuture(p))
      control.advance(1.second)
      val f = Stitch.run(s)
      control.advance(1.second)
      p.setValue(Unit)
      val (_, d) = Await.result(f)
      d must equal(1.second)
    }
  }

  test("sleep") {
    implicit val timer = new MockTimer

    Time.withCurrentTimeFrozen { tc =>
      val s = Stitch.sleep(100.milliseconds)
      val f = Stitch.run(s)
      timer.tick()
      assert(!f.isDefined)
      tc.advance(99.milliseconds); timer.tick()
      assert(!f.isDefined)
      tc.advance(2.milliseconds); timer.tick()
      assert(f.isDefined)
    }
  }

  test("within times out") {
    implicit val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val s = Stitch
        .sleep(1.hour)
        .map { _ => "wat" }
        .within(1.millisecond)
      val f = Stitch.run(s)

      timer.tick()
      assert(!f.isDefined)

      tc.advance(1.millisecond); timer.tick()
      assert(f.isDefined)

      val ex = intercept[TimeoutException](Await.result(f))
      assert(ex.getMessage == "Operation timed out after 1.milliseconds")
    }
  }

  test("within succeeds") {
    implicit val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val s = Stitch
        .sleep(1.millisecond)
        .map { _ => "wat" }
        .within(10.milliseconds)
      val f = Stitch.run(s)

      timer.tick()
      assert(!f.isDefined)

      tc.advance(1.millisecond); timer.tick()
      assert(f.isDefined)

      assert(Await.result(f) == "wat")
    }
  }

  test("within - timeout will win a race") {
    implicit val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val s = Stitch
        .sleep(1.millisecond)
        .map { _ => "wat" }
        .within(1.millisecond)
      val f = Stitch.run(s)

      timer.tick()
      assert(!f.isDefined)

      tc.advance(1.millisecond); timer.tick()
      assert(f.isDefined)

      val ex = intercept[TimeoutException](Await.result(f))
      assert(ex.getMessage == "Operation timed out after 1.milliseconds")
    }
  }

  class TestError extends TimeoutException("gah")

  test("within - custom exception") {
    implicit val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val f = Stitch.run(Stitch.future(new Promise).within(timer, 1.millisecond, new TestError))
      tc.advance(2.milliseconds); timer.tick()
      assert(f.isDefined)
      intercept[TestError](Await.result(f))
    }
  }

  test("async") {
    var called = false
    val p = Promise[Unit]
    val s = Stitch.callFuture(p)
    val f = Stitch.run(
      for {
        _ <- s
        _ <- Stitch.async(s.onSuccess { _ => called = true })
      } yield 7
    )
    p.setDone()
    Await.result(f) must equal(7)
    called must be(true)
  }

  test("async continues after run returns") {
    var called = false
    val p1 = Promise[Unit]
    val p2 = Promise[Unit]
    val f = Stitch.run(
      for {
        _ <- Stitch.callFuture(p1)
        _ <- Stitch.async(Stitch.callFuture(p2).onSuccess { _ => called = true })
      } yield 7
    )
    p1.setDone()
    Await.result(f) must equal(7)
    called must be(false)
    p2.setDone()
    called must be(true)
  }

  test("async interrupted if run interrupted") {
    val p1 = Promise[Unit]
    val p2 = Promise[Unit]
    val p3 = Promise[Unit]
    val f = Stitch.run(
      for {
        _ <- Stitch.callFuture(p1)
        _ <- Stitch.async(Stitch.callFuture(p2))
        s <- Stitch.callFuture(p3)
      } yield s
    )
    p1.setDone()
    f.raise(new Exception())
    p2.isInterrupted match {
      case Some(_) =>
      case _ => fail("interrupts not propagated")
    }
  }

  test(".unit of exception is exception") {
    intercept[Exception] {
      await(Stitch.exception(new Exception()).unit)
    }
  }

  test("shared traversed ArrayBuffer") {
    val ts = new mutable.ArrayBuffer[Int]
    ts += 1
    ts += 2
    val f: Int => Stitch[String] = i => Stitch.value(i.toString)
    val g: Int => Stitch[Int] = i => Stitch.value(i)
    await(
      Stitch.join(
        Stitch.traverse(ts)(f),
        Stitch.traverse(ts)(g)
      )
    ) must be((Seq("1", "2"), Seq(1, 2)))
  }

  test("reused TransformSeq") {
    val ss = mutable.ArrayBuffer(Stitch.value(7))
    Await.result(
      Stitch.run(
        Stitch.join(
          Stitch.collect(ss),
          Stitch.collect(ss)
        )
      )
    )
  }

  test("reused TransformSeq with arrow") {
    val a = Arrow.flatMap { x: Int => Stitch.value(x) }
    val s = a.traverse(Seq(7))
    Await.result(Stitch.run(Stitch.join(s, s)))
  }

  test("reused TransformSeqTry with arrow") {
    val a = Arrow.map { x: Int => x }
    val s = a.traverse(Seq(7))
    Await.result(Stitch.run(Stitch.join(s, s)))
  }

  def assertRpcCount(cached: Boolean, expected: Long): Unit = {

    @volatile var count = 0L
    val rpc = Stitch.callFuture {
      Future.value("foo").onSuccess { _ => count += 1L }
    }

    val stitch = rpc.flatMap { _ => rpc }

    if (cached) {
      Await.result(Stitch.runCached(stitch))
    } else {
      Await.result(Stitch.run(stitch))
    }
    count must be(expected)
  }

  test("cache prevents multiple RPCs for the same key") {
    assertRpcCount(cached = true, 1L)
  }

  test("without cache, multiple RPCs for the same key can take place") {
    assertRpcCount(cached = false, expected = 2L)
  }

  test("recursive") {
    def fact(i: Int): Stitch[Int] =
      Stitch.recursive {
        if (i <= 1) Stitch.value(1)
        else fact(i - 1).map(_ * i)
      }

    await(fact(0)) must be(1)
    await(fact(1)) must be(1)
    await(fact(2)) must be(2)
    await(fact(3)) must be(6)
    await(fact(4)) must be(24)
  }

  private val recursiveInputs =
    List(
      0,
      10,
      Run.maxRecursionDepth - 1,
      Run.maxRecursionDepth,
      Run.maxRecursionDepth + 1,
      100000
    )

  test("recursive is stack safe") {
    def countDown(i: Int): Stitch[Unit] =
      Stitch.recursive {
        if (i == 0) Stitch.Done
        else countDown(i - 1)
      }

    recursiveInputs.foreach(i => await(countDown(i)))
  }

  test("recursive calls can be collected") {
    def loop(limit: Int, i: Int = 0): Stitch[Int] =
      Stitch.recursive {
        if (i == limit) Stitch.value(limit)
        else loop(limit, i + 1)
      }

    val s = Stitch.collect(recursiveInputs.map(loop(_)))
    assert(await(s) == recursiveInputs)
  }

  test("recursive doesn't break batching") {
    val groupCalls = new AtomicInteger()
    val seqGroup = SeqGroup[Int, Int] { c =>
      groupCalls.incrementAndGet()
      Future.value(c.map(item => Return(item + 10)))
    }
    def loop(limit: Int, i: Int = 0): Stitch[Int] =
      Stitch.recursive {
        if (i == limit) Stitch.call(limit, seqGroup)
        else loop(limit, i + 1)
      }
    val s = Stitch.collect(recursiveInputs.map(loop(_)))
    assert(await(s) == recursiveInputs.map(_ + 10))
    assert(groupCalls.get() == 1)
  }

  test("recursive synchronizes evaluation when wrapped by a sync ref") {
    val cdl = new CountDownLatch(1)
    val running = Promise[Unit]()
    val s =
      Stitch.synchronizedRef {
        def loop(i: Int): Stitch[Int] = {
          if (i < Run.maxRecursionDepth) {
            // make sure to trigger a delayed recursion
            Stitch.recursive(loop(i + 1))
          } else {
            // this recursion will be delayed and, when
            // evaluated, it will block the thread. Only
            // one thread should be able to enter the body
            // of the recursive Stitch and block on the cdl
            Stitch.recursive {
              assert(!running.isDefined)
              running.setValue(())
              cdl.await()
              Stitch.value(1)
            }
          }
        }
        loop(0)
      }
    val fut1 = FuturePool.unboundedPool(Stitch.run(s)).flatten

    // wait for the concurrent run loop to reach
    // the delayed recursion and block its thread
    awaitFuture(running)

    // start another run loop, which must not trigger
    // another execution of the delayed recursion
    val fut2 = Stitch.run(s)

    // both futures continue pending since both depend
    // on the simplification of the delayed recursion
    // that is running on the blocked run loop
    assert(!fut1.isDefined)
    assert(!fut2.isDefined)

    // release the blocked run loop
    cdl.countDown()

    // both futures are satisfied correctly
    assert(awaitFuture(fut1) == 1)
    assert(awaitFuture(fut2) == 1)
  }

  test("Twitter Locals are defined for simple compositions") {
    val local = new Local[Boolean]
    // simple compositions
    assert(await(Stitch.let(local)(true)(Stitch.value(local()))).contains(true))
    assert(
      await(Stitch.let(local)(true)(Stitch.Unit.flatMap(_ => Stitch.value(local()))))
        .contains(true))
    assert(await(Stitch.let(local)(true)(Stitch.Unit.map(_ => local()))).contains(true))
    assert(await(Stitch.let(local)(true)(Stitch.Unit.flatMap(_ =>
      Stitch.Unit.map(identity).flatMap(_ => Stitch.value(local()))))).contains(true))
    // nested compositions
    assert(
      await(Stitch.let(local)(true)(Stitch.Unit.flatMap(_ =>
        Stitch.Unit.map(identity).flatMap(_ => Stitch.let(local)(false)(Stitch.value(local()))))))
        .contains(false))
    // defined in callFuture
    assert(
      await(Stitch.let(local)(true)(Stitch.callFuture(Future.value(local()))))
        .contains(true))

    { // callFuture doesn't capture creation point locals
      val cf = local.let(false)(Stitch.callFuture(Future.value(local())))
      assert(await(cf).isEmpty)
    }
  }

  test("Twitter Locals are defined for complex compositions") {
    val local = new Local[Boolean]
    val timer: Timer = new JavaTimer()
    // defined after a call
    assert(
      await(
        Stitch.let(local)(true)(Stitch
          .sleep(200.milliseconds)(timer)
          .map(_ => local())))
        .contains(true))
    // delayed compositions with different values in different scopes
    val complexStitch = Stitch.collect(
      Seq(
        Stitch.Unit.map(_ => local()).lowerFromOption(),
        Stitch
          .sleep(200.milliseconds)(timer).map(_ =>
            Stitch.join(
              Stitch.value(local()).lowerFromOption(),
              Stitch
                .sleep(200.milliseconds)(timer)
                // delayed nested composition
                .map(_ => local()).lowerFromOption())).flatten.map {
            case (left, right) => left && right
          },
        Stitch
          .callFuture(Future.sleep(500.milliseconds)(timer)).before(
            Stitch.value(local())).lowerFromOption(),
        Stitch
          .sleep(200.milliseconds)(timer).map(_ =>
            Stitch.join(
              Stitch.value(local()).lowerFromOption(),
              Stitch.let(local)(false)( // differently scoped in a nested composition
                Stitch.sleep(200.milliseconds)(timer).map(_ => local()).lowerFromOption())
            )).flatten.map {
            case (left, right) => left && !right
          },
        Stitch.value(local()).map(_.isEmpty),
        Stitch.Unit.before(Stitch.value(local()).lowerFromOption()),
        Stitch.letClear(local)(Stitch.value(local())).map(_.isEmpty), // cleared
        Stitch.letClear(local)(Stitch.Unit.map(_ => local())).map(_.isEmpty) // cleared
      ))
    assert(await(Stitch.let(local)(true)(complexStitch)).forall(identity))
  }

  test("Twitter Locals scoped with Stitch.let are not defined in Groups") {
    val localAroundWholeStitch = new Local[Unit]
    val localAroundPartOfStitch = new Local[Unit]

    val g = new SeqGroup[Int, Int] {
      override protected def run(keys: Seq[Int]): Future[Seq[Try[Int]]] = {
        assert(localAroundPartOfStitch().isEmpty)
        assert(localAroundWholeStitch().isDefined)
        Future.value(keys.map(Return(_)))
      }
    }

    val s = Stitch.let(localAroundPartOfStitch)(())(Stitch.call(1, g))
    localAroundWholeStitch.let(())(await(s))
  }

  test("Twitter Locals scoped with Stitch.let you care about can be passed into a Group") {
    val l = new Local[Int]

    /**
     * Group with the local we care about in the constructor,
     * calls with different [[local]] values will __not__ be batched together
     * and will only be deduplicated if their [[local]] is the same in addition to their input arg.
     */
    case class GroupWithLocalInConstructor(local: Option[Int]) extends SeqGroup[Int, Option[Int]] {
      override protected def run(keys: Seq[Int]): Future[Seq[Try[Option[Int]]]] = {
        assert(keys.forall(k => local.contains(-k)))
        Future.value(keys.map(_ => Return(local)))
      }
    }

    val s = Stitch.join(
      Stitch.let(l)(-1)(
        Stitch.value(1).flatMap(i => Stitch.call(i, GroupWithLocalInConstructor(l.apply())))),
      Stitch.let(l)(-2)(
        Stitch.value(2).flatMap(i => Stitch.call(i, GroupWithLocalInConstructor(l.apply())))),
      // local let-scoped around the Stitch.run applies here
      Stitch.value(3).flatMap(i => Stitch.call(i, GroupWithLocalInConstructor(l.apply())))
    )
    assert(l.let(-3)(await(s)) == ((Some(-1), Some(-2), Some(-3))))
  }

  test(
    "Twitter Locals scoped with Stitch.let you care about can be passed into a Group with a call") {
    val local = new Local[Int]
    case class IntWithContext(i: Int, local: Option[Int])

    /**
     * Group with the local we care about in the input arg,
     * calls with different [[local]] values __will be__ batched together
     * and will be deduplicated by [[IntWithContext]] containing the [[local]].
     */
    val groupWithLocalInInputArg = new SeqGroup[IntWithContext, Option[Int]] {
      override protected def run(keys: Seq[IntWithContext]): Future[Seq[Try[Option[Int]]]] = {
        assert(keys.forall(iwc => iwc.local.contains(-iwc.i)))
        Future.value(keys.map(iwc => Return(iwc.local)))
      }
    }

    val s = Stitch.join(
      Stitch.let(local)(-1)(
        Stitch
          .value(1).flatMap(i =>
            Stitch.call(IntWithContext(i, local.apply()), groupWithLocalInInputArg))),
      Stitch.let(local)(-2)(
        Stitch
          .value(2).flatMap(i =>
            Stitch.call(IntWithContext(i, local.apply()), groupWithLocalInInputArg))),
      // local let-scoped around the Stitch.run applies here
      Stitch
        .value(3).flatMap(i =>
          Stitch.call(IntWithContext(i, local.apply()), groupWithLocalInInputArg))
    )
    assert(local.let(-3)(await(s)) == ((Some(-1), Some(-2), Some(-3))))
  }
}
