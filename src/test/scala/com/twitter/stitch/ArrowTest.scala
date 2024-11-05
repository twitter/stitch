package com.twitter.stitch

import com.twitter.conversions.DurationOps._
import com.twitter.stitch
import com.twitter.stitch.Arrow.AllLocals
import com.twitter.stitch.Arrow.AndThen
import com.twitter.stitch.Arrow.Const
import com.twitter.stitch.Arrow.FlatMap
import com.twitter.stitch.Arrow.JoinMap3
import com.twitter.stitch.Arrow.LocalUnavailable
import com.twitter.stitch.Arrow.TransformTry
import com.twitter.stitch.helpers.StitchTestSuite
import com.twitter.util._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Shrink
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class ArrowTest extends StitchTestSuite with ScalaCheckDrivenPropertyChecks {

  /**
   * Checks both the application of a single input argument and traversing over multiple
   * input values.
   */
  def checkPure[A, B](arrow: Arrow[A, B], f: A => B)(implicit arb: Arbitrary[A]): Unit = {
    forAll { (x: A) =>
      await(arrow(x)) must equal(f(x))
      await(arrow(Return(x))) must equal(f(x))
      await(arrow(Stitch.value(x))) must equal(f(x))
      await(arrow.traverse(Some(x))) must equal(Some(f(x)))
    }

    forAll { (xs: Seq[A]) => await(arrow.traverse(xs)) must equal(xs.map(f)) }
  }

  def checkJoin[A](
    n: Int
  )(
    build: Seq[Arrow[A, A]] => Arrow[A, Any]
  )(
    implicit arb: Arbitrary[A]
  ): Unit = {
    val exception = new RuntimeException
    val genArrows =
      Gen.listOfN(
        n,
        Gen.frequency(
          (n * 4) -> Arbitrary.arbitrary[A].map(x => Arrow.map[A, A](_ => x)),
          1 -> Arbitrary.arbitrary[A].map(Arrow.value(_)),
          1 -> Gen.const(Arrow.exception(exception))
        )
      )

    implicit def noShrink[T] = Shrink[T](_ => Stream.empty)

    forAll(genArrows, Arbitrary.arbitrary[A]) { (arrows, x) =>
      val arrow = build(arrows)
      val hasThrow = arrows.exists { case Arrow.Const(Throw(_)) => true; case _ => false }

      if (hasThrow) {
        await(arrow.liftToTry(x)) must equal(Throw(exception))
      } else {
        val vs = await(Stitch.traverse(arrows) { a => a(x) })
        val res = await(arrow(x)).asInstanceOf[Product]

        res.productArity must equal(vs.size)

        vs.zipWithIndex.foreach {
          case (v, i) =>
            res.productElement(i) must equal(v)
        }
      }
    }
  }

  case class MockException(i: Int) extends Exception()

  test("identity") {
    checkPure[String, String](Arrow.identity[String], identity)
  }

  test("application") {
    assert(await(Arrow.application(1)((i: Int) => Return(i))) == 1)
    val e = new Exception()
    assert(intercept[Exception](await(Arrow.application(1)((i: Int) => Throw(e)))).eq(e))
  }

  test("value") {
    checkPure[String, String](Arrow.value("foo"), _ => "foo")
  }

  test("apply") {
    checkPure[String, String](Arrow(_ => Stitch.value("foo")), _ => "foo")
  }

  test("unit") {
    checkPure[String, Unit](Arrow.unit[String], _ => ())
  }

  test("map") {
    checkPure[String, String](Arrow.map[String, String](_.reverse), _.reverse)
  }

  test("map fails") {
    val a = Arrow.map { _: String => throw new Exception() }
    val s = a("foo")
    intercept[Exception] { await(s) }
  }

  test("tryTransform") {
    checkPure[String, String](Arrow.transformTry[String, String](_.map(_.reverse)), _.reverse)
  }

  test("tryTransform fails") {
    val e = new Exception
    // captures throws
    val a = Arrow.transformTry { _: Try[String] => throw e }
    assert(intercept[Exception] { await(a("foo")) }.eq(e))

    // passes through throws
    val b = Arrow.transformTry { arg: Try[String] => arg }
    assert(intercept[Exception] { await(b(Throw(e))) }.eq(e))

    // returns throws
    val c = Arrow.transformTry { _: Try[String] => Throw(e) }
    assert(intercept[Exception] { await(c("foo")) }.eq(e))
  }

  test("flatMap") {
    checkPure[String, String](
      Arrow.flatMap[String, String](x => Stitch.value(x.reverse)),
      _.reverse
    )
  }

  test("flatMap fails") {
    val a = Arrow.flatMap { _: String => throw new Exception() }
    val s = a("foo")
    intercept[Exception] { await(s) }
  }

  test("foldLeft") {
    val a = Arrow.foldLeftArrow[Int, Int](Arrow.map[(Int, Int), Int] { case (a, b) => a + b })

    { // respects initial conditions
      val s = a((50, Seq()))
      assert(await(s) == 50)
    }

    { // works with size 1
      val s = a((0, Seq(100)))
      assert(await(s) == 100)
    }

    { // works with size 2
      val s = a((0, Seq(100, 200)))
      assert(await(s) == 300)
    }

    { // works with size > 2
      val s = a((0, Seq(100, 200, 300, 400)))
      assert(await(s) == 1000)
    }

    { // doesnt stack overflow
      val input = (0, 0 to 100000)
      val expected = input._2.sum
      val s = a(input)
      assert(await(s) == expected)
    }

    { // batch inputs work
      val ss = a.traverse(
        Seq(
          (0, Seq()),
          (0, Seq(1, 2, 3, 4, 5)),
          (100, Seq(1, 2, 3, 4)),
          (50, Seq()),
          (0, 0 to 100)))
      assert(await(ss) == Seq(0, 15, 110, 50, (0 to 100).sum))
    }
  }

  test("foldLeft fails") {
    val e = new IllegalArgumentException

    { // failures are passed to the inner Arrow
      val a = Arrow
        .foldLeftArrow[Int, Int](Arrow.liftToTry[(Int, Int)].map {
          case Return(_) => 0
          case Throw(_) => throw e
        }).liftToTry
      val s = a(Throw[(Int, Seq[Int])](new Exception))
      assert(await(s) == Throw(e))
    }

    val a = Arrow
      .foldLeftArrow[Int, Int](Arrow.map[(Int, Int), Int] {
        case (a, b) if b % 2 == 0 => a + b
        case _ => throw e
      }).liftToTry

    { // works with size even numbers pass
      val s = a((0, Seq(2, 4, 6)))
      assert(await(s) == Return(12))
    }

    { // odd numbers fail
      val s = a((0, Seq(1, 3, 5)))
      assert(await(s) == Throw(e))
    }

    { // odd numbers in the middle fail
      val s = a((0, Seq(2, 4, 1)))
      assert(await(s) == Throw(e))
    }

    { // failures only impact their input
      val ss = a.traverse(Seq((0, Seq(2, 4, 6)), (0, Seq(2, 4, 1)), (0, Seq(2, 4, 6))))
      assert(await(ss) == Seq(Return(12), Throw(e), Return(12)))
    }
  }

  test("foldLeft locals") {
    val l = new Arrow.Local[Int]()
    val a = Arrow.foldLeftArrow[Int, Int](l.apply())
    val aWithL = l.let(10)(a)
    assert(await(aWithL((0, Seq(100)))) == 10)
    assert(await(aWithL.traverse(Seq((0, Seq(100)), (0, Seq(200))))) == Seq(10, 10))
  }

  test("foldLeft exceptions are propagated") {
    val p = new Promise[Int]
    val a = Arrow.foldLeftArrow(Arrow.callFuture[(Int, Int), Int](_ => p))
    val f = Stitch.run(a((0, Seq(0, 1, 2, 3, 4, 5, 6))))
    val e = new Exception
    f.raise(e)
    assert(p.isInterrupted.contains(e))
  }

  test("handle success") {
    checkPure[String, String](Arrow.handle { case _ => "bar" }, identity)
  }

  test("handle failure") {
    val a = Arrow.handle[Any, Any] { case _ => "bar" }
    await(a(Throw(new Exception()))) must equal("bar")
  }

  test("partial handle failure") {
    val a = Arrow.handle[Any, Any] { case e: Exception if e.getMessage == "exp" => "bar" }
    await(a(Throw(new Exception("exp")))) must equal("bar")

    val e = new Exception()
    val e1 = intercept[Exception] { await(a(Throw(e))) }
    assert(e1 == e)
  }

  test("rescue success") {
    checkPure[String, String](Arrow.rescue { case _ => Stitch.value("bar") }, identity)
  }

  test("rescue failure") {
    val a = Arrow.rescue[Any, Any] { case _ => Stitch.value("bar") }
    await(a(Throw(new Exception()))) must equal("bar")
  }

  test("partial rescue") {
    val a = Arrow.rescue[Any, Any] {
      case e: Exception if e.getMessage == "exp" => Stitch.value("bar")
    }
    await(a(Throw(new Exception("exp")))) must equal("bar")

    val e = new Exception()
    val e1 = intercept[Exception] { await(a(Throw(e))) }
    assert(e1 == e)
  }

  test("mapFailure - defined") {
    val e1 = new Exception
    val e2 = new Exception
    val a =
      Arrow.exception(e1).mapFailure {
        case `e1` => e2
      }
    val e3 = intercept[Exception] { await(a(1)) }
    assert(e2 == e3)
  }

  test("mapFailure - undefined") {
    val e1 = new Exception
    val a =
      Arrow.exception(e1).mapFailure {
        case _: NullPointerException => new NullPointerException
      }
    val e2 = intercept[Exception] { await(a(1)) }
    assert(e1 == e2)
  }

  test("transform success") {
    checkPure[String, String](
      Arrow.transform[String, String] {
        case Return(t) => Stitch.value(t)
        case Throw(_) => Stitch.value("exn")
      },
      identity
    )
  }

  test("transform failure") {
    val a = Arrow.transform[String, String] {
      case Return(t) => Stitch.value(t)
      case Throw(_) => Stitch.value("exn")
    }
    await(a(Throw(new Exception()))) must equal("exn")
  }

  test("ensure on failure") {
    var i = 0
    val a = Arrow.exception(new Exception()).ensure { i += 1 }
    intercept[Exception] { await(a(())) }
    i must equal(1)
  }

  test("ensure on success") {
    var i = 0
    val a = Arrow.value("foo").ensure { i += 1 }
    await(a(())) must equal("foo")
    i must equal(1)
  }

  test("respond on failure") {
    var i = 0
    val a = Arrow.exception(new Exception()).respond {
      case Return(_) => i += 2
      case Throw(_) => i += 1
    }
    intercept[Exception] { await(a(())) }
    i must equal(1)
  }

  test("respond on success") {
    var i = 0
    val a = Arrow.value("foo").respond {
      case Return(_) => i += 2
      case Throw(_) => i += 1
    }
    await(a(())) must equal("foo")
    i must equal(2)
  }

  test("onSuccess on failure") {
    var i = 1
    val a = Arrow.exception(new Exception()).onSuccess { _ => i += 1 }
    intercept[Exception] { await(a(())) }
    i must equal(1)
  }

  test("onSuccess on success") {
    var i = 1
    val a = Arrow.value(5).onSuccess { r => i += r }
    await(a(())) must equal(5)
    i must equal(6)
  }

  test("onFailure on failure") {
    var i = ""
    val a = Arrow.exception(new Exception("fail")).onFailure { e => i += e.getMessage }
    intercept[Exception] { await(a(())) }
    i must equal("fail")
  }

  test("onFailure on success") {
    var i = ""
    val a = Arrow.value(5).onFailure { e => i += e.getMessage }
    await(a(())) must equal(5)
    i must equal("")
  }

  test("liftToTry success") {
    checkPure[String, Try[String]](Arrow.liftToTry, Return(_))
  }

  test("liftToTry failure") {
    val a = Arrow.liftToTry[String]
    val e = new Exception()
    await(a(Throw(e))) must equal(Throw(e))
  }

  test("lowerFromTry success") {
    val a = Arrow.lowerFromTry[String]
    await(a(Return("foo"))) must equal("foo")
  }

  test("lowerFromTry failure") {
    val a = Arrow.lowerFromTry[String]
    val e = new Exception()
    val e1 = intercept[Exception] { await(a(Throw(e): Try[String])) }
    assert(e1 == e)
  }

  test("lowerFromTry exception") {
    val a = Arrow.lowerFromTry[String]
    val e = new Exception()
    val e1 = intercept[Exception] { await(a(Throw(e): Try[Try[String]])) }
    assert(e1 == e)
  }

  test("liftToOption Some") {
    checkPure[String, Option[String]](Arrow.liftToOption[String](), Some(_))
  }

  test("liftToOption None") {
    val e = new Exception()
    val a = Arrow.liftToOption[String] { case e2 => e2 eq e }
    await(a(Throw(e))) must equal(None)
  }

  test("liftToOption failure") {
    val e = new Exception()
    val a = Arrow.liftToOption[String] { case e2 => e2 ne e }
    intercept[Exception] { await(a(Throw(e))) }
  }

  test("liftNotFoundToOption Some") {
    checkPure[String, Option[String]](Arrow.liftNotFoundToOption[String], Some(_))
  }

  test("liftNotFoundToOption None") {
    val a = Arrow.liftNotFoundToOption[String]
    await(a(Throw(NotFound))) must equal(None)
  }

  test("liftNotFoundToOption failure") {
    val e = new Exception()
    val a = Arrow.liftNotFoundToOption[String]
    intercept[Exception] { await(a(Throw(e))) }
  }

  test("lowerFromOption Some") {
    val e = new Exception()
    val a = Arrow.lowerFromOption[String](e)
    await(a(Some("foo"))) must equal("foo")
  }

  test("lowerFromOption None") {
    val e = new Exception()
    val a = Arrow.lowerFromOption[String](e)
    val e1 = intercept[Exception] { await(a(None)) }
    assert(e1 == e)
  }

  test("lowerFromOption failure") {
    val e = new Exception()
    val a = Arrow.lowerFromOption[String](e)
    val e1 = new Exception()
    val e2 = intercept[Exception] { await(a(Throw(e1))) }
    assert(e1 == e2)
  }

  test("zipWithArg success") {
    checkPure[String, (String, Int)](
      Arrow.zipWithArg(Arrow.map[String, Int](_.length)),
      s => (s, s.length)
    )
  }

  test("zipWithArg failure") {
    val e = new Exception()
    val a = Arrow.zipWithArg(Arrow.map[Any, String](_ => throw e))
    val s = a("foo")
    intercept[Exception] { await(s) }
  }

  test("const of exception is exception") {
    intercept[Exception] {
      await(Arrow.const(Return(7))(Throw(new Exception())))
    }
  }

  test("join") {
    checkPure[Int, (Int, String)](
      Arrow.join(
        Arrow.map { x: Int => x + 1 },
        Arrow.map { x: Int => x.toString }
      ),
      x => (x + 1, x.toString)
    )
  }

  test("join 3") {
    checkPure[Int, (Int, String, Int)](
      Arrow.join(
        Arrow.map { x: Int => x + 1 },
        Arrow.map { x: Int => x.toString },
        Arrow.map { x: Int => x + 2 }
      ),
      x => (x + 1, x.toString, x + 2)
    )
  }

  test("join 4") {
    checkPure[Int, (Int, String, Int, String)](
      Arrow.join(
        Arrow.map { x: Int => x + 1 },
        Arrow.map { x: Int => x.toString },
        Arrow.map { x: Int => x + 2 },
        Arrow.map { x: Int => x.toString.reverse }
      ),
      x => (x + 1, x.toString, x + 2, x.toString.reverse)
    )
  }

  test("join 5") {
    checkPure[Int, (Int, Int, Int, Int, Int)](
      Arrow.join(
        Arrow.map { x: Int => x + 1 },
        Arrow.map { x: Int => x + 2 },
        Arrow.map { x: Int => x + 3 },
        Arrow.map { x: Int => x + 4 },
        Arrow.map { x: Int => x + 5 }
      ),
      x => (x + 1, x + 2, x + 3, x + 4, x + 5)
    )
  }

  test("join impure") {
    checkPure[Int, (Int, String)](
      Arrow.join(
        Arrow.map { x: Int => x + 1 },
        Arrow.flatMap { x: Int => Stitch.value(x.toString) }
      ),
      x => (x + 1, x.toString)
    )
  }

  test("join with continuation singular apply") {
    checkPure[Int, Int](
      Arrow
        .join(
          Arrow.map { x: Int => x + 1 },
          Arrow.value(2)
        )
        .map { case (x, y) => x + y },
      x => (x + 1) + 2
    )
  }

  test("sequence success") {
    checkPure[Seq[Int], Seq[Int]](
      Arrow.sequence(Arrow.map[Int, Int](_ + 1)),
      xs => xs.map(_ + 1)
    )
  }

  test("sequence - failure") {
    val e1 = new Exception
    val a = Arrow.sequence[Int, Int](Arrow.exception(e1))
    assert(e1 == intercept[Exception] { await(a(Seq(1, 2))) })
    assert(e1 == intercept[Exception] { await(a.traverse(Seq(Seq(1, 2), Seq(3, 4)))) })
  }

  test("sequence - mix of success/failure") {
    val ex = new IllegalArgumentException
    val a = Arrow.sequence(Arrow.map[Int, Int](_ * 2))
    val b =
      Arrow.flatMap[Seq[Int], Seq[Int]] {
        case xs if xs.nonEmpty => Stitch.value(xs.map(_ + 1))
        case _ => Stitch.exception(ex)
      }
    val ba = b.andThen(a).liftToTry

    await(ba(Seq(1, 2, 3))) must equal(Return(Seq(4, 6, 8)))
    await(ba.traverse(Seq(Seq(1, 2), Nil, Seq(3, 4)))) must
      equal(Seq(Return(Seq(4, 6)), Throw(ex), Return(Seq(8, 10))))
  }

  test("option success") {
    checkPure[Option[Int], Option[Int]](
      Arrow.option(Arrow.map[Int, Int](_ + 1)),
      xs => xs.map(_ + 1)
    )
  }

  test("option - failure") {
    val e = new Exception
    val a = Arrow.option[Int, Int](Arrow.exception(e))
    assert(await(a(None)) == None)
    assert(intercept[Exception] { await(a(Some(1))) } == e)
  }

  test("collect two arrows - success") {
    checkPure[Int, Seq[Int]](
      Arrow.collect(
        Seq(
          Arrow.map[Int, Int](_ + 1),
          Arrow.map[Int, Int](_ + 2)
        )
      ),
      x => Seq(x + 1, x + 2)
    )
  }

  test("collect one arrow - success") {
    checkPure[Int, Seq[Int]](
      Arrow.collect(
        Seq(
          Arrow.map[Int, Int](_ + 1)
        )
      ),
      x => Seq(x + 1)
    )
  }

  test("collect no arrows - success") {
    val a = Arrow.collect[Int, Int](Seq.empty)
    await(a(1)) must equal(Seq.empty)
  }

  test("collect - failure") {
    val e1 = new Exception
    val a =
      Arrow.collect[Int, Int](
        Seq(
          Arrow.exception(e1),
          Arrow.map[Int, Int](_ + 2)
        )
      )
    val e2 = intercept[Exception] { await(a(1)) }
    assert(e1 == e2)
  }

  test("applyEffect - success") {
    var input = 0

    val a =
      Arrow
        .applyEffect[Int](Arrow.map(input = _))
        .map(_ + 1)

    await(a(1)) must equal(2)
    assert(input == 1)
  }

  test("applyEffect - effect failure") {
    val e1 = new Exception

    val a: Arrow.Effect[Int] =
      Arrow.exception(e1)

    Arrow.applyEffect(a).map(_ + 1)

    val e2 = intercept[Exception] { await(a(1)) }
    assert(e1 == e2)
  }

  test("applyEffect - arrow failure") {
    var input = 0
    val e1 = new Exception

    val a =
      Arrow
        .applyEffect[Int](Arrow.map(input = _))
        .andThen(Arrow.exception(e1))

    val e2 = intercept[Exception] { await(a(1)) }
    assert(e1 == e2)
    assert(input == 1)
  }

  test("call") {
    val g = new SeqGroup[Int, String] {
      def run(keys: Seq[Int]) = Future.value(keys.map(key => Return(key.toString)))
    }
    checkPure[Int, String](Arrow.call(g), _.toString)
  }

  test("call with common context") {
    val g = new SeqGroup[Int, String] {
      def run(keys: Seq[Int]) = Future.value(keys.map(key => Return(key.toString)))
    }
    checkPure[Int, String](
      Arrow
        .map[Int, (Int, Unit)](x => (x, ()))
        .andThen(Arrow.call { _: Unit => g }),
      _.toString
    )
  }

  test("call with uncommon context") {
    val g = new SeqGroup[Int, String] {
      def run(keys: Seq[Int]) = Future.value(keys.map(key => Return(key.toString)))
    }
    checkPure[Int, String](
      Arrow
        .map[Int, (Int, String)](x => (x, x.toString))
        .andThen(Arrow.call { _: String => g }),
      _.toString
    )
  }

  test("call with common context is common") {
    var adds = 0
    var addSeqTrys = 0

    val g = new SeqGroup[String, Int] {
      def run(keys: Seq[String]) = Promise[Seq[Try[Int]]]
      override def runner() = {
        val runner = super.runner()
        new SeqRunner[String, Int]() {
          override def add(k: String) = { adds += 1; runner.add(k) }
          override def addSeqTry(ks: Seq[Try[String]]) = { addSeqTrys += 1; runner.addSeqTry(ks) }
          override def run() = runner.run()
          def run(keys: Seq[String]) = ???
        }
      }
    }

    val a = Arrow.call { _: Unit => g }
    val s = a.traverse(Seq(("1", ()), ("2", ()), ("3", ())))
    Stitch.run(s)

    adds must equal(0)
    addSeqTrys must equal(1)
  }

  test("call with uncommon context is uncommon") {
    var adds = 0
    var addSeqTrys = 0

    val g = new SeqGroup[String, Int] {
      def run(keys: Seq[String]) = Promise[Seq[Try[Int]]]
      override def runner() = {
        val runner = super.runner()
        new SeqRunner[String, Int]() {
          override def add(k: String) = { adds += 1; runner.add(k) }
          override def addSeqTry(ks: Seq[Try[String]]) = { addSeqTrys += 1; runner.addSeqTry(ks) }
          override def run() = runner.run()
          def run(keys: Seq[String]) = ???
        }
      }
    }

    val a = Arrow.call { _: Int => g }
    val s = a.traverse(Seq(("1", 1), ("2", 2), ("3", 3)))
    Stitch.run(s)

    adds must equal(3)
    addSeqTrys must equal(0)
  }

  test("callFuture") {
    checkPure[Int, Int](
      Arrow.callFuture[Int, Int](x => Future.value(x * 2)),
      _ * 2
    )
  }

  test("ifelse") {
    checkPure[Int, Int](
      Arrow
        .ifelse[Int, Int](_ >= 0, Arrow.map(_ + 1), Arrow.map(_ - 1))
        .andThen(Arrow.map(_ * 2)),
      {
        case x if x >= 0 => (x + 1) * 2
        case x => (x - 1) * 2
      }
    )
  }

  test("ifelse exception only") {
    val e = new Exception()

    checkPure[Int, Try[Int]](
      Arrow
        .exception(e)
        .andThen(Arrow.ifelse[Int, Int](_ >= 0, Arrow.map(_ + 1), Arrow.map(_ - 1)))
        .liftToTry,
      _ => Throw(e)
    )
  }

  test("ifelse with mixture of left, right, and exceptions") {
    val e = new Exception()

    checkPure[Int, Try[Int]](
      Arrow
        .ifelse[Int, Int](_ >= 0, Arrow.identity, Arrow.exception(e))
        .andThen(Arrow.ifelse[Int, Int](_ % 2 == 1, Arrow.map(_ + 1), Arrow.identity))
        .liftToTry,
      {
        case x if x < 0 => Throw(e)
        case x if x % 2 == 1 => Return(x + 1)
        case x => Return(x)
      }
    )
  }

  test("choose") {
    checkPure[Option[Int], Int](
      Arrow.choose[Option[Int], Int](
        Arrow.Choice.ifDefinedAt(
          { case Some(x) if x >= 0 => x.toString },
          Arrow.map[String, Int](_.length)
        ),
        (_.exists(_ < 0), Arrow.value(0)),
        Arrow.Choice.otherwise(Arrow.value(-1))
      ),
      {
        case Some(x) if x >= 0 => x.toString.length
        case Some(x) if x < 0 => 0
        case None => -1
      }
    )
  }

  test("choose throws MatchError when no match") {
    val a =
      Arrow.choose[Int, Int](
        (_ > 0, Arrow.value(0))
      )

    intercept[MatchError] { await(a(-1)) }
  }

  test("choose - choosen arrow never sees Throw from the outside") {
    var sawException = false
    val arrow =
      Arrow[Int, Int] { x =>
        if (x % 2 == 0) Stitch.value(x) else Stitch.exception(new Exception())
      }.andThen(
        Arrow.choose[Int, Int](
          Arrow.Choice.otherwise(
            Arrow
              .map[Int, Int](_ * 2)
              .rescue { case e => sawException = true; Stitch.exception(e) }
          )
        )
      )

    intercept[Exception] { await(arrow(1)) }
    await(arrow(2))
    intercept[Exception] { await(arrow.traverse(Seq(1, 2, 3, 4))) }

    sawException must be(false)
  }

  test("time") {
    Time.withCurrentTimeFrozen { control =>
      val p = Promise[Unit]
      val a = Arrow[Unit, Unit](_ => Stitch.callFuture(p))
      control.advance(1.second)
      val f = Stitch.run(Arrow.time(a)(()))
      control.advance(1.second)
      p.setValue(Unit)
      val (_, d) = Await.result(f)
      d must equal(1.second)
    }
  }

  test("time - shared arrow") {
    Time.withCurrentTimeFrozen { control =>
      val p = Promise[Unit]
      val a = Arrow[Unit, Unit](_ => Stitch.callFuture(p))
      control.advance(1.second)
      val f1 = Stitch.run(Arrow.time(a)(()))
      control.advance(1.second)
      val f2 = Stitch.run(Arrow.time(a)(()))
      control.advance(1.second)
      p.setValue(Unit)
      val (_, d1) = Await.result(f1)
      val (_, d2) = Await.result(f2)
      d1 must equal(2.second)
      d2 must equal(1.second)
    }
  }

  test("time - immediate execution") {
    val a =
      Arrow
        .time(Arrow.map({ _: Unit => Thread.sleep(500) })).map {
          case (_, duration) => duration
        }

    val d = Await.result(Stitch.run(a(())))
    assert(d.inMillis >= 500)
  }

  test("within: finishes before timeout") {
    Time.withCurrentTimeFrozen { control =>
      implicit val timer = new MockTimer()
      val timeout = 1.second
      val arrow = Arrow
        .within(timeout)(
          Arrow[Int, Unit] { duration => Stitch.sleep(duration.milliseconds) }).liftToTry
      val duration = 100
      val f = Stitch.run(arrow(duration))
      control.advance(150.milliseconds); timer.tick()
      assert(Await.result(f).isReturn)
    }
  }

  test("within: finishes after timeout") {
    Time.withCurrentTimeFrozen { tc =>
      implicit val timer = new MockTimer()
      val timeout = 250.milliseconds
      val arrow = Arrow
        .within(timeout)(
          Arrow[Int, Unit] { duration => Stitch.sleep(duration.milliseconds) }).liftToTry
      val duration = 1000
      val f = Stitch.run(arrow(duration))
      tc.advance(500.milliseconds); timer.tick()
      Await.result(f) match {
        case Return(()) => assert(false)
        case Throw(e) => assert(e.isInstanceOf[com.twitter.util.TimeoutException])
      }
    }
  }

  test("within: return partial results") {
    Time.withCurrentTimeFrozen { tc =>
      implicit val timer = new MockTimer()
      val timeout = 300.milliseconds
      def assertResult[U](duration: Duration, result: Try[U]): Unit = {
        if (duration < timeout)
          assert(result.isReturn)
        else
          assert(result.isThrow)
      }
      val arrow = Arrow
        .within(timeout)(
          Arrow[Int, Unit] { duration => Stitch.sleep(duration.milliseconds) }).liftToTry
      val durations = Seq(100, 200, 700)
      val f = Stitch.run(arrow.traverse(durations))
      timer.tick()
      // First advance 250ms, the first two results will be Return.
      // Then advance 100ms, the last result will be Throw.
      //
      // Note: we cannot advance 350ms (250ms+100ms) in one step, otherwise
      // the first two could be either Try or Return due to race condition. However, if we
      // advance 280ms (100ms, 200ms < 280ms) in one step, the first two results are
      // guaranteed to be Return but the third Stitch will never finish and test will halt
      tc.advance(250.milliseconds); timer.tick()
      tc.advance(100.milliseconds); timer.tick()
      val results = Await.result(f)
      durations.zip(results).foreach {
        case (d, r) => assertResult(d.milliseconds, r)
      }
    }
  }

  test("Effect.andThen - success") {
    var v1 = 0
    var v2 = 0
    val a1: Arrow.Effect[Int] = Arrow.map(v1 = _)
    val a2: Arrow.Effect[Int] = Arrow.map(v2 = _)

    val a = Arrow.Effect.andThen(a1, a2)

    await(a(1))
    assert(v1 == 1)
    assert(v2 == 1)
  }

  test("Effect.andThen - failure") {
    val e1 = new Exception
    var v1 = 0
    val a1: Arrow.Effect[Int] = Arrow.map(v1 = _)
    val a2: Arrow.Effect[Int] = Arrow.exception(e1)

    val a = Arrow.Effect.andThen(a1, a2)

    val e2 = intercept[Exception] { await(a(1)) }
    assert(e1 == e2)
    assert(v1 == 1)
  }

  test("Effect.sequentially - success") {
    var v = 0
    val a1: Arrow.Effect[Int] = Arrow.map(v += _)
    val a2: Arrow.Effect[Int] = Arrow.map(v *= _)

    val a = Arrow.Effect.sequentially(a1, a2)

    await(a(2))
    assert(v == 4)
  }

  test("Effect.sequentially - failure") {
    val e1 = new Exception
    var v = 0
    val a1: Arrow.Effect[Int] = Arrow.map(v += _)
    val a2: Arrow.Effect[Int] = Arrow.exception(e1)

    val a = Arrow.Effect.sequentially(a1, a2)

    val e2 = intercept[Exception] { await(a(1)) }
    assert(e1 == e2)
    assert(v == 1)
  }

  test("Effect.inParallel - success") {
    var v = 0
    val a1: Arrow.Effect[Int] = Arrow.map(v += _)
    val a2: Arrow.Effect[Int] = Arrow.map(v += _)

    val a = Arrow.Effect.inParallel(a1, a2)

    await(a(1))
    assert(v == 2)
  }

  test("Effect.onlyIf - satisfied") {
    var v = 0
    val a =
      Arrow.Effect.onlyIf[Int](_ == 1) {
        Arrow.map(v = _)
      }

    await(a(1))
    assert(v == 1)
  }

  test("Effect.onlyIf - not satisfied") {
    var v = 0
    val a =
      Arrow.Effect.onlyIf[Int](_ == 1) {
        Arrow.map(v = _)
      }

    await(a(2))
    assert(v == 0)
  }

  test("Iso.onlyIf - satisfied") {
    val a =
      Arrow.Iso.onlyIf[Int](_ == 1) {
        Arrow.map(_ + 1)
      }

    await(a(1)) must equal(2)
  }

  test("Iso.onlyIf - not satisfied") {
    val a =
      Arrow.Iso.onlyIf[Int](_ == 1) {
        Arrow.map(_ + 1)
      }

    await(a(0)) must equal(0)
  }

  test("Effect.enabledBy - satisfied") {
    var v = 0
    val a =
      Arrow.Effect.enabledBy[Int](true) {
        Arrow.map(v = _)
      }

    await(a(1))
    assert(v == 1)
  }

  test("Effect.enabledBy - not satisfied") {
    var v = 0
    val a =
      Arrow.Effect.enabledBy[Int](false) {
        Arrow.map(v = _)
      }

    await(a(2))
    assert(v == 0)
  }

  test("Iso.enabledBy - satisfied") {
    val a =
      Arrow.Iso.enabledBy[Int](true) {
        Arrow.map(_ + 1)
      }

    await(a(1)) must equal(2)
  }

  test("Iso.enabledBy - not satisfied") {
    val a =
      Arrow.Iso.enabledBy[Int](false) {
        Arrow.map(_ + 1)
      }

    await(a(0)) must equal(0)
  }

  test("recursive") {
    val fact =
      Arrow.recursive[Int, Int] { self =>
        Arrow.choose[Int, Int](
          Arrow.Choice.when(_ <= 1, Arrow.value(1)),
          Arrow.Choice.otherwise {
            Arrow
              .zipWithArg(
                Arrow
                  .map[Int, Int](_ - 1)
                  .andThen(self)
              )
              .map {
                case (a, b) => a * b
              }
          }
        )
      }

    await(fact(0)) must be(1)
    await(fact(1)) must be(1)
    await(fact(2)) must be(2)
    await(fact(3)) must be(6)
    await(fact(4)) must be(24)
  }

  test("recursive is stack safe") {
    val countDown =
      Arrow.recursive[Int, Unit] { self =>
        Arrow.choose[Int, Unit](
          Arrow.Choice.when(_ == 0, Arrow.value(())),
          Arrow.Choice.otherwise {
            Arrow
              .map[Int, Int](_ - 1)
              .andThen(self)
          }
        )
      }
    await(countDown(100000))
  }

  test("andThen constant folding") {
    // Should not fold
    Arrow.value(1).map(_ => Random.nextLong) mustNot matchPattern { case Arrow.Const(_) => }
    Arrow.value(1).ensure(()) mustNot matchPattern { case Arrow.Const(_) => }
    Arrow.value(1).onSuccess { _ => } mustNot matchPattern { case Arrow.Const(_) => }
    Arrow.value(1).liftToTry mustNot matchPattern { case Arrow.Const(_) => }

    // Should fold
    Arrow.value(1).unit must matchPattern { case Arrow.Const(_) => }
    Arrow.value(None).lowerFromOption() must matchPattern { case Arrow.Const(_) => }
    Arrow.exception(NotFound).lowerFromOption() must matchPattern { case Arrow.Const(_) => }
    Arrow.value(Return.Unit).lowerFromTry must matchPattern { case Arrow.Const(_) => }
    Arrow.value(1).andThen(Arrow.identity[Int]) must matchPattern { case Arrow.Const(_) => }
    Arrow.value(1).andThen(Arrow.value(2)) must matchPattern { case Arrow.Const(_) => }

    // Test that constant-folding happens regardless of associativity:
    val ineligible: Arrow[Int, Int] = Arrow.map(x => x)
    val arr1 = Arrow.value(1).andThen(Arrow.value(1).andThen(ineligible))
    val arr2 = Arrow.value(1).andThen(Arrow.value(1)).andThen(ineligible)
    arr1 must equal(arr2)
  }

  test("join constant folding") {
    checkJoin[Int](2) { as => Arrow.join(as(0), as(1)) }
    checkJoin[Int](3) { as => Arrow.join(as(0), as(1), as(2)) }
    checkJoin[Int](4) { as => Arrow.join(as(0), as(1), as(2), as(3)) }
    checkJoin[Int](5) { as => Arrow.join(as(0), as(1), as(2), as(3), as(4)) }
    checkJoin[Int](6) { as => Arrow.join(as(0), as(1), as(2), as(3), as(4), as(5)) }
    checkJoin[Int](7) { as => Arrow.join(as(0), as(1), as(2), as(3), as(4), as(5), as(6)) }
    checkJoin[Int](8) { as => Arrow.join(as(0), as(1), as(2), as(3), as(4), as(5), as(6), as(7)) }
    checkJoin[Int](9) { as =>
      Arrow.join(as(0), as(1), as(2), as(3), as(4), as(5), as(6), as(7), as(8))
    }
    checkJoin[Int](10) { as =>
      Arrow.join(as(0), as(1), as(2), as(3), as(4), as(5), as(6), as(7), as(8), as(9))
    }
    checkJoin[Int](12) { as =>
      Arrow.join(
        as(0),
        as(1),
        as(2),
        as(3),
        as(4),
        as(5),
        as(6),
        as(7),
        as(8),
        as(9),
        as(10),
        as(11)
      )
    }
    checkJoin[Int](13) { as =>
      Arrow.join(
        as(0),
        as(1),
        as(2),
        as(3),
        as(4),
        as(5),
        as(6),
        as(7),
        as(8),
        as(9),
        as(10),
        as(11),
        as(12)
      )
    }
  }

  test("collect - constant folding") {
    Arrow.collect(
      Seq(
        Arrow.value(1),
        Arrow.value(2)
      )
    ) must matchPattern { case Arrow.Const(_) => }
    Arrow.collect(
      Seq(
        Arrow.identity[Int],
        Arrow.value(2)
      )
    ) must matchPattern { case Arrow.Collect(_) => }
    Arrow.collect(
      Seq(
        Arrow.identity[Int],
        Arrow.exception(null)
      )
    ) must matchPattern { case Arrow.Const(_) => }
  }

  test("wrap dynamically calls underlying") {
    var underlying: Arrow[Int, Int] = Arrow.map(_ * 2)
    val a = Arrow.wrap(underlying)

    checkPure[Int, Int](a, _ * 2)

    underlying = Arrow.map(_ + 1)

    checkPure[Int, Int](a, _ + 1)
  }

  test("async with long running side effect") {
    def f(a: Int) = a + a

    val p = Promise[Unit]
    val sideEffect = Arrow.Effect[Int](_ => Stitch.callFuture(p))
    val arrow = Arrow[Int, Int] { n => Stitch(f(n)) }
    val pipeline = arrow.applyEffect(Arrow.async(sideEffect))
    val result = Await.result(Stitch.run(pipeline(2)))
    p.setValue(Unit)
    result must equal(f(2))
  }

  test("Locals single") {
    val l = new Arrow.Local[Int]
    val a =
      l.letWithArg((curr: Int) => curr)(
        Arrow
          .map[Int, Int](_ + 1)
          .andThen(Arrow.zipWithArg(l()))
      )

    val in = 1
    val expected = (in + 1, in)
    assert(await(a(in)) == expected)
  }

  test("Locals with default uses let value") {
    val l = new Arrow.Local[Int](-999)
    val a =
      l.letWithArg((curr: Int) => curr)(
        Arrow
          .map[Int, Int](_ + 1)
          .andThen(Arrow.zipWithArg(l()))
      )

    val in = 1
    val expected = (in + 1, in)
    assert(await(a(in)) == expected)
  }

  test("Locals assigned after the arrow is created") {
    val l = new Arrow.Local[Int]
    val unassigned = Arrow
      .map[Int, Int](_ + 1)
      .andThen(Arrow.zipWithArg(l()))

    val a =
      l.letWithArg((curr: Int) => curr)(unassigned)

    val in = 1
    val expected = (in + 1, in)
    assert(await(a(in)) == expected)
  }

  test("Locals are updated based on the existing value of the local") {
    val l = new Arrow.Local[List[String]]

    val innerArrow: Arrow[Int, (Int, List[String])] =
      l.let((currLocal: List[String]) => "second" :: currLocal)(
        l.applyWithArg().map { case (i: Int, ll) => (i + 1, ll) }
      )

    val a =
      l.let(List("first"))(
        Arrow
          .map[Int, Int](_ + 1)
          .andThen(innerArrow)
      )

    val in = 1
    val expected = (3, List("second", "first"))
    assert(await(a(in)) == expected)
  }

  test("Locals traverse") {
    val l = new Arrow.Local[Int]
    val a =
      l.letWithArg((curr: Int) => curr)(
        Arrow
          .map[Int, Int](_ + 1)
          .andThen(Arrow.zipWithArg(l()))
      )

    val in = Seq(1, 2, 3)
    val expected = in.map(i => (i + 1, i))
    assert(await(a.traverse(in)) == expected)
  }

  test("Locals undefined returns LocalUnavailable") {
    val l = new Arrow.Local[Int]
    val a =
      Arrow
        .map[Int, Int](_ + 1)
        .andThen(Arrow.zipWithArg(l()))

    val in = 1
    intercept[Arrow.LocalUnavailable[Int]](await(a(in)))
  }

  test("Local with default returns default when undefined") {
    val l = new Arrow.Local[Int](0)
    val a =
      Arrow
        .map[Int, Int](_ + 1)
        .andThen(Arrow.zipWithArg(l()))

    assert(await(a(1)) == ((2, 0)))
  }

  test("Locals async let") {
    val p = Promise[Seq[Try[Int]]]
    val g = new SeqGroup[Int, Int] { def run(keys: Seq[Int]) = p }
    val l = new Arrow.Local[Int]
    val a = l.let(1)(Arrow.call(g).andThen(l()))

    val f = Stitch.run(a(0))
    p.setValue(Seq(Return(0)))
    assert(Await.result(f) == 1)
  }

  test("Locals interference") {
    val p = Promise[Seq[Try[Int]]]
    val g = new SeqGroup[Int, Int] { def run(keys: Seq[Int]) = p }
    val l = new Arrow.Local[Int]
    val a =
      l.let(0)(
        Arrow.join(
          Arrow.call(g).andThen(l()),
          l.let(1)(Arrow.call(g).andThen(l()))
        )
      )

    val f = Stitch.run(a(0))
    p.setValue(Seq(Return(0)))
    val expected = (0, 1)
    assert(Await.result(f) == expected)
  }

  test("Locals cleared returns LocalUnavailable") {
    val l = new Arrow.Local[Int]
    val a =
      l.let(1)(
        l.letClear(
          Arrow
            .map[Int, Int](_ + 1)
            .andThen(Arrow.zipWithArg(l()))
        )
      )

    val in = 1
    intercept[LocalUnavailable[Int]](await(a(in)))
  }

  test("Local with default returns default when cleared") {
    val l = new Arrow.Local[Int](0)
    val a =
      l.let(1)(
        l.letClear(
          Arrow
            .map[Int, Int](_ + 1)
            .andThen(Arrow.zipWithArg(l()))
        )
      )

    assert(await(a(1)) == ((2, 0)))
  }

  test("locals applyWithArg") {
    val l = new Arrow.Local[Int]
    val a =
      l.let(2)(
        l.applyWithArg[Int]()
          .map { case (in, local) => in + local }
      )
    assert(await(a(1)) == 3)
  }

  test("locals after join points") {
    val l = new Arrow.Local[Int]
    val a =
      l.let(2)(
        Arrow.flatMap[Any, Unit](_ => Stitch.Done).andThen(l.apply())
      )
    assert(await(a(1)) == 2)
  }

  test("locals in flatMapArrow") {
    val l = new Arrow.Local[Int]
    val a =
      l.let(2)(
        Arrow.flatMapArrow[Any, Int](l.apply())
      )
    assert(await(a(1)) == 2)
  }

  test("locals in a wrap") {
    val l = new Arrow.Local[Int]
    val a = l.let[Int, Int](2)(Arrow.wrap(l.apply()))

    assert(await(a(1)) == 2)
  }

  test("locals in a timer") {
    val l = new Arrow.Local[Int]
    val a = l.let[Int, (Try[Int], Duration)](2)(Arrow.time(l.apply()))

    assert(await(a(1))._1 == Return(2))
  }

  test("Locals throw when trying to update an unset local") {
    val l = new Arrow.Local[Int]

    val a =
      l.letWithArg((in: Int, currLocal: Int) => currLocal + in)(
        l.applyWithArg().map { case (i: Int, ll) => (i + 1, ll) }
      )

    val in = 1
    intercept[LocalUnavailable[Int]](await(a(in)))

    val b =
      l.let[Int, (Int, Int)]((currLocal: Int) => currLocal)(
        l.applyWithArg()
      )
    intercept[LocalUnavailable[Int]](await(b(in)))
  }

  test("Locals with defaults use the default when trying to update an unset local") {
    val l = new Arrow.Local[Int](0)

    val a =
      l.letWithArg((in: Int, currLocal: Int) => currLocal + in)(
        l.applyWithArg().map { case (i: Int, ll) => (i + 1, ll) }
      )
    assert(await(a(1)) == ((2, 1)))

    val b =
      l.let[Int, (Int, Int)]((currLocal: Int) => currLocal)(
        l.applyWithArg()
      )
    assert(await(b(1)) == ((1, 0)))
  }

  test("Locals update when updating a set local") {
    val l = new Arrow.Local[Int]

    val a =
      l.let(1)(
        l.letWithArg((in: Int, currLocal: Int) => currLocal + in)(
          l.apply()
        ))

    val in = 1
    assert(await(a(in)) == 2)

    val b =
      l.let(1)(
        l.let((currLocal: Int) => currLocal + 1)(
          l.apply()
        ))
    assert(await(b(in)) == 2)
  }

  test("Locals are passed through joins correctly") {
    val local = new Arrow.Local[Int]

    val a =
      local.let(1) {
        Arrow.join(Arrow.identity[Any], Arrow.identity[Any]).andThen(local.apply())
      }

    assert(await(a.apply(0)) == 1)
  }

  test("locals don't leak into stitches") {
    val local = new Arrow.Local[Boolean]

    val expectTrue = local.apply.map(l => assert(l == true))
    val s = Stitch.Transform(Stitch.value(AnyRef), Map(local -> Return(true)), expectTrue)

    val expectFalse = local.apply.map(l => assert(l == false))

    await(expectFalse(s: Stitch[Any], Map(local -> Return(false)): Arrow.Locals))
  }

  test("locals don't leak into flatmaps") {
    val local = new Arrow.Local[Boolean]

    val a =
      local.let(false) {
        Arrow.flatMap[Any, Any] { v =>
          Arrow
            .join(Arrow.identity[Any], Arrow.identity[Any])
            .andThen(local.apply()) // should not be defined here due to flatMap!!
            .apply(v)
        }
      }

    intercept[Arrow.LocalUnavailable[_]] {
      await(a(AnyRef))
    }
  }

  test("Locals don't update when the input is a Throw") {
    val l = new Arrow.Local[Int]

    val a =
      l.let[Int, Int](1)(
        Arrow
          .exception(new Exception)
          .andThen(l.letWithArg((in: Int, currLocal: Int) => currLocal + in)(
            l.apply()
          )))

    val in = 9
    assert(await(a.apply(in)) == 1)

    val b =
      l.let(1)(
        Arrow
          .exception(new Exception)
          .andThen(l.let((currLocal: Int) => currLocal + 9)(
            l.apply()
          )))
    assert(await(b(in)) == 1)

    val c =
      l.let(1)(
        Arrow
          .exception(new Exception)
          .andThen(l.letWithArg((in: Int) => in + 9)(
            l.apply()
          )))
    assert(await(c(in)) == 1)

    val d =
      l.let(1)(
        Arrow
          .exception(new Exception)
          .andThen(l.let(9)(
            l.apply()
          )))
    assert(await(d(in)) == 1)
  }

  test("Locals keep the correct exceptions") {
    val local = new Arrow.Local[Int]
    val ex = new Exception

    val expected = (Throw(ex), Throw(local.Unavailable))
    assert(
      await(
        Arrow
          .exception(ex).andThen(local.letWithArg((i: Int) => i + 1)(
            Arrow.join(Arrow.liftToTry, local().liftToTry)))(AnyRef)
      ) == expected)
  }

  test("Locals are undefined in places where they should not be defined") {
    val Sentinel = new Arrow.Local[Boolean]

    intercept[Arrow.LocalUnavailable[_]](
      Await.result(
        Stitch.run(
          Sentinel
            .let(false)(Arrow.flatMap[Any, Any](v =>
              Arrow
                .join[Any, Any, Any](Arrow.identity, Arrow.identity)
                .andThen(Sentinel.apply()) // should not be defined here due to flatMap!!
                .apply(v))).apply(AnyRef))))

    intercept[Arrow.LocalUnavailable[_]](
      Await.result(
        Stitch.run(
          Sentinel
            .let(false)(
              Arrow.flatMap[Any, Any](v =>
                Arrow
                  .join[Any, Any, Any](
                    Arrow.identity,
                    Sentinel.apply()
                  ) // should not be defined here due to flatMap!!
                  .apply(v))).apply(AnyRef))))
  }

  test("Locals are extracted and zipped") {
    val l1 = new Arrow.Local[Int]
    val l2 = new Arrow.Local[String]

    val a =
      l1.let(1)(l2.let("hello")(Arrow.locals))

    val in = 1
    assert(await(a(in)) == Map(l1 -> Return(1), l2 -> Return("hello")))

    val b =
      l1.let(1)(l2.let("hello")(Arrow.zipWithLocals[Int]))

    assert(await(b(in)) == Tuple2(1, Map(l1 -> Return(1), l2 -> Return("hello"))))
  }

  test("Locals are extracted correctly") {
    val l1 = new Arrow.Local[Int]
    val l2 = new Arrow.Local[String]

    def localsMap(i: Try[Int]): Arrow.Locals =
      Map(l1 -> Return(i.get() + 10), l2 -> Return(i.get + 20))

    val arg: ArrayBuffer[Try[Int]] = ArrayBuffer(Return(1), Return(2), Return(3))
    val locals: ArrayBuffer[Arrow.Locals] = arg.map(localsMap)

    assert(await(AllLocals.run(arg, locals)) == locals.map(Return(_)))
  }

  test("Applies the arrow to each element of the Seq while maintaining Locals") {
    val l1 = new Arrow.Local[Int]

    val a = l1.let[(Arrow[Int, Int], Seq[Int]), Seq[Int]](5)(Arrow.applyArrowToSeq)

    val addLocalWithElement = Arrow.zipWithArg[Int, Int](l1()).map { case (a, b) => a + b }
    val seq = Seq(1, 2, 3, 4, 5)

    assert(await(a.apply((addLocalWithElement, seq))) == Seq(6, 7, 8, 9, 10))

    val b = l1.let[(Arrow[Int, Int], Seq[Int]), Seq[Int]](5)(Arrow.identity.applyArrowToSeq)

    assert(await(b.apply((addLocalWithElement, seq))) == Seq(6, 7, 8, 9, 10))
  }

  test("applyArrow applies an arrow (single arg)") {
    val simple = Arrow.identity[(Arrow[Int, Int], Int)].applyArrow
    assert(await(simple.traverse(Seq(Arrow.identity[Int] -> 1))) == Seq(1))

    val throws = Arrow.identity[(Arrow[Int, Int], Int)].map(_ => throw MockException(0)).applyArrow
    val e = intercept[MockException](await(throws.traverse(Seq(Arrow.identity[Int] -> 1))))
    assert(e == MockException(0))

    val l = new stitch.Arrow.Local[Int]()

    val in = (Arrow.map[Int, Int](_ + 1).andThen(l.applyWithArg()), 5)

    val a =
      l.let(1)(
        Arrow
          .Identity[(Arrow[Int, (Int, Int)], Int)].applyArrow
          .andThen(l.applyWithArg()))

    val results = await(a.apply(in))
    assert(results._1._1 == 6) // check that arrow is applied
    assert(results._1._2 == 1) // check that locals are propagated to input arrow
    assert(results._2 == 1) // check that locals are propagated to tail

    assert(results == (((6, 1), 1)))
  }

  test("applyArrow") {
    val a = Arrow.identity[(Arrow[Int, Int], Int)].applyArrow
    assert(await(a.traverse(Seq(Arrow.identity[Int] -> 1))) == Seq(1))

    val throws = Arrow.identity[(Arrow[Int, Int], Int)].map(_ => throw MockException(0)).applyArrow
    val e = intercept[MockException](await(throws.traverse(Seq(Arrow.identity[Int] -> 1))))
    assert(e == MockException(0))
  }

  test("applyArrow applies an arrow (multiple args)") {
    val l = new stitch.Arrow.Local[Int]()

    val in = Seq(
      (Arrow.map[Int, Int](_ + 1).andThen(l.applyWithArg()), 5),
      (Arrow.map[Int, Int](_ + 2).andThen(l.applyWithArg()), 6),
      (Arrow.map[Int, Int](_ + 3).andThen(l.applyWithArg()), 7)
    )

    val a =
      l.let(1)(
        Arrow
          .Identity[(Arrow[Int, (Int, Int)], Int)].applyArrow
          .andThen(l.applyWithArg()))

    assert(await(a.traverse(in)) == Seq(((6, 1), 1), ((8, 1), 1), ((10, 1), 1)))
  }

  test("applyArrow bypass internal logic for duplicate inputs works") {
    val arrow1 = Arrow.map[Int, Int](_ + 1).andThen(Arrow.ExtractBatch)

    val in = Seq(
      (arrow1, 5),
      (arrow1, 6),
      (arrow1, 7)
    )

    val a = Arrow.Identity[(Arrow[Int, Seq[Any]], Int)].applyArrow

    assert(await(a.traverse(Seq.empty)) == Seq.empty)

    assert(
      await(a.traverse(in)) ==
        Seq(
          Seq(6, 7, 8),
          Seq(6, 7, 8),
          Seq(6, 7, 8)
        ).map(_.map(Return(_))))
  }

  test("applyArrow all Throws") {
    var ranArrow1 = false
    val toBeApplied = Arrow
      .map[Int, Int] { i =>
        ranArrow1 = true
        i
      }.andThen(Arrow.ExtractBatch)

    val a = Arrow.identity[Int].map(toBeApplied -> _).applyArrow.liftToTry

    val input = ArrayBuffer[Try[Int]](
      Throw(MockException(0)),
      Throw(MockException(1)),
      Throw(MockException(2)))

    val result = await(a.run(input, Arrow.Locals.emptyBuffer(input.length)))

    assert(!ranArrow1)
    assert(result == input)

  }

  test("applyArrow groups correctly") {
    val arrow1 = Arrow.map[Int, Int](_ + 1).andThen(Arrow.ExtractBatch)
    val arrow2 = Arrow.map[Int, Int](_ + 5).andThen(Arrow.ExtractBatch)
    val arrow3 = Arrow.map[Int, Int](_ + 10).andThen(Arrow.ExtractBatch)

    // This case won't work since passing `add2` into `Arrow.map` creates a new pointer to the function
    // instead create `val a = Arrow.map(f)` and reference `a` instead of `f`
    def add2(i: Int): Int = i + 2

    // This case works as long as you don't unintentionally create a separate lambda by doing `add3(_)` or  `i => add3(i)`
    val add3: Int => Int = (i: Int) => i + 3

    val in = ArrayBuffer[Try[(Arrow[Int, Seq[Any]], Int)]](
      Throw(MockException(0)), // exceptions are passed through
      Return((arrow1, 5)),
      Return((arrow2, 6)),
      Return((arrow2, 7)),
      Return((arrow2, 8)),
      Return((arrow3, 9)),
      Return((arrow3, 10)),
      Throw(MockException(1)),
      Return((Arrow.map(add2).andThen(Arrow.ExtractBatch), 11)), // new pointer, won't batch
      Return((Arrow.map(add2).andThen(Arrow.ExtractBatch), 12)), // new pointer, won't batch
      Return((Arrow.map(add3).andThen(Arrow.ExtractBatch), 13)),
      Return((Arrow.map(add3).andThen(Arrow.ExtractBatch), 14)),
      Return((Arrow.map(add3(_)).andThen(Arrow.ExtractBatch), 15)), // new lambda, won't batch
      Return(
        (Arrow.map((a: Int) => add3(a)).andThen(Arrow.ExtractBatch), 16)
      ), //new lambda, won't batch
      Return((arrow2, 17)) // non-adjacent inputs get batched correctly
    )

    val a = Arrow.applyArrow[Int, Seq[Any]]()

    val result = await(a.run(in, Arrow.Locals.emptyBuffer(in.length)))

    assert(
      result == Seq(
        Throw(MockException(0)),
        Return(Seq(Return(6))), // 5 -> 6
        Return(Seq(Return(11), Return(12), Return(13), Return(22))), // 6,7,8,17 -> 11,12,13,22
        Return(Seq(Return(11), Return(12), Return(13), Return(22))),
        Return(Seq(Return(11), Return(12), Return(13), Return(22))),
        Return(Seq(Return(19), Return(20))), // 9, 10 -> 19, 20
        Return(Seq(Return(19), Return(20))),
        Throw(MockException(1)),
        Return(Seq(Return(13))), // new pointer for def, won't batch
        Return(Seq(Return(14))), // new pointer for def, won't batch
        Return(Seq(Return(16), Return(17))), // 13, 14 -> 16, 17
        Return(Seq(Return(16), Return(17))),
        Return(Seq(Return(18))), // new lambda, won't batch
        Return(Seq(Return(19))), // new lambda, won't batch
        Return(Seq(Return(11), Return(12), Return(13), Return(22)))
      )
    )
  }

  test("pure arrows andThen'ed are optimized at build time") {
    // mix of pure arrows
    val a = Arrow
      .map[Int, Int](_ * 2)
      .map(_ * 2)
      .liftToTry
      .lowerFromTry
      .liftToOption(PartialFunction.empty)
      .lowerFromOption()
      .ensure(())
      .map(_ * 2)
      .respond(_ => ())
      .map(_ * 2)
      .mapFailure(PartialFunction.empty)
      .map(_ * 2)
      .onSuccess(_ => ())
      .map(_ * 2)
      .handle[Int](PartialFunction.empty)
      .map(_ * 2)
    a match {
      case TransformTry(f, true, true) => assert(f(Return(2)) == Return(256))
      case _ => fail
    }
  }

  test("pure arrows aren't unnecessarily transformed") {
    val map = Arrow.map[Int, Int](_ * 2)
    val flatMap = Arrow.flatMap[Int, Int](Stitch.value)
    // map should remain a map and not be transformed into a TryTransform
    val a = map.andThen(flatMap)
    assert(a == Arrow.AndThen(map, flatMap))
  }

  test("a mix of pure arrows and non-pure arrows andThen'ed are optimized at build time") {
    // mix of pure arrows with non pure arrow in the middle
    val a = Arrow
      .map[Int, Int](_ * 2)
      .map(_ * 2)
      .liftToTry
      .lowerFromTry
      .liftToOption(PartialFunction.empty)
      .lowerFromOption()
      .ensure(())
      .flatMap(i => Stitch.value(i * 2)) // impure
      .respond(_ => ())
      .map(_ * 2)
      .mapFailure(PartialFunction.empty)
      .map(_ * 2)
      .onSuccess(_ => ())
      .map(_ * 2)
      .handle[Int](PartialFunction.empty)
      .map(_ * 2)

    a match {
      case AndThen(TransformTry(_, true, true), AndThen(FlatMap(_), TransformTry(_, true, true))) =>
      case _ => fail
    }

    // mix of pure arrows joined with non-pure arrows
    val b = Arrow.join(a, Arrow.const(Return(1)).andThen(a), a.unit) // join is impure
    b match {
      case JoinMap3(
            AndThen(TransformTry(_, true, true), AndThen(FlatMap(_), TransformTry(_, true, true))),
            AndThen(
              Const(_),
              AndThen(
                TransformTry(_, true, true),
                AndThen(FlatMap(_), TransformTry(_, true, true)))),
            AndThen(TransformTry(_, true, true), AndThen(FlatMap(_), TransformTry(_, true, true))),
            _
          ) =>
      case _ => fail
    }
  }

  type Letter = Arrow[Unit, Boolean] => Arrow[Unit, Boolean]
  type Getter = () => Option[Boolean]

  trait TwitterLocalTest {
    val timer: Timer = new JavaTimer()
    val local = new Local[Boolean]
    val scopingToTest: Seq[(Arrow[Unit, Boolean] => Arrow[Unit, Boolean], () => Option[Boolean])] =
      Seq(
        (Arrow.let(local)(true)(_), () => local()),
        (Arrow.letWithArg[Unit, Boolean, Boolean](local)(_ => true)(_), () => local())
      )

    def testTwitterLetArrow(
      a: ArrowWithLocal,
      expected: Boolean = true
    ): Unit = {
      val arrowsWithLetAndGetScoped = scopingToTest.map {
        case (letter, getter) => a(letter, getter)
      }
      arrowsWithLetAndGetScoped.foreach { a =>
        assert(await(a.apply(())) == expected)
        assert(await(a.traverse(Seq.fill(10)(()))).forall(_ == expected))
      }
    }
  }

  trait ArrowWithLocal {
    def apply(
      letter: Letter,
      getter: Getter
    ): Arrow[Unit, Boolean]
  }

  test("Twitter Locals are defined for simple compositions") {
    new TwitterLocalTest {
      // simple compositions
      testTwitterLetArrow((letter: Letter, getter: Getter) =>
        letter(Arrow.unit.map(_ => getter()).lowerFromOption()))
      testTwitterLetArrow((letter: Letter, getter: Getter) =>
        letter(
          Arrow.unit
            .flatMap(_ =>
              Arrow.unit
                .map(identity).flatMap(_ => Stitch.value(getter())).apply(())).lowerFromOption()))
      // sync arrow created at runtime gets locals
      testTwitterLetArrow((letter: Letter, getter: Getter) =>
        letter(
          Arrow.unit
            .flatMap(_ =>
              Arrow.unit
                .map(identity).flatMap(_ => Arrow.value(getter()).apply(())).apply(
                  ())).lowerFromOption()))
      // nested compositions
      testTwitterLetArrow(
        (letter: Letter, getter: Getter) =>
          letter(
            Arrow.unit
              .flatMap(_ =>
                Stitch.Unit
                  .map(identity).flatMap(_ =>
                    Stitch.let(local)(false)(Stitch.value(getter())))).lowerFromOption()),
        expected = false
      )
      // is defined in callFuture
      testTwitterLetArrow((letter: Letter, getter: Getter) =>
        letter(Arrow.callFuture((_: Unit) => Future.value(getter().contains(true)))))

      { // let clear local
        val a = Arrow.letClear(local)(Arrow.unit.map(_ => local())).map(_.isEmpty)
        assert(local.let(true)(await(a.apply(()))))
        assert(local.let(true)(await(a.traverse(Seq((), ())))) == Seq(true, true))
      }
      { // let override local
        val a = Arrow.let(local)(false)(Arrow.unit.map(_ => local())).map(_.contains(false))
        assert(local.let(true)(await(a.apply(()))))
        assert(local.let(true)(await(a.traverse(Seq((), ())))) == Seq(true, true))
      }
      { // let override local
        val a = Arrow.let(local)(!local().get)(Arrow.unit.map(_ => local())).map(_.contains(false))
        assert(local.let(true)(await(a.apply(()))))
        assert(local.let(true)(await(a.traverse(Seq((), ())))) == Seq(true, true))
      }
      { // missing locals when not scoped around runtime
        val a = Arrow.unit.map(_ => local()).lowerFromOption()
        assert(intercept[Exception](await(a.apply(()))) == NotFound)
        assert(intercept[Exception](await(a.traverse(Seq((), ())))) == NotFound)
      }
      // locals scoped at build time
      assert(
        intercept[Exception](await(
          local.let(true)(Arrow.unit.map(_ => local())).lowerFromOption().apply(()))) == NotFound)
      assert(
        intercept[Exception](
          await(local
            .let(true)(Arrow.unit.map(_ => local())).lowerFromOption().traverse(
              Seq((), ())))) == NotFound)
      // scoped at apply time, may get some locals since sync code is executed when applied
      assert(
        await(local.let(true)(Arrow.unit.map(_ => local()).lowerFromOption().apply(()))) === true)
      assert(
        intercept[Exception](
          await(local.let(true)(Arrow
            .sleep[Unit](1.millisecond)(timer).map(_ => local()).lowerFromOption().apply(
              ())))) == NotFound)
      // scoped at stitch let around an arrow works
      assert(await(Stitch.let(local)(true)(Arrow
        .sleep[Unit](1.millisecond)(timer).map(_ => local()).lowerFromOption().apply(()))) === true)
      // callFuture has locals defined at Future creation
      await(
        Arrow.let(local)(true)(Arrow.callFuture[Unit, Boolean](_ => Future.value(local().get)))(()))
    }
  }

  test("Twitter Locals are defined for complex compositions") {
    new TwitterLocalTest {
      // delayed compositions with different values in different scopes
      testTwitterLetArrow((letter: Letter, getter: Getter) =>
        letter(
          Arrow
            .collect(Seq(
              Arrow.unit.map(_ => getter()).lowerFromOption(),
              Arrow
                .sleep[Unit](1.millisecond)(timer).andThen(
                  Arrow.join(
                    Arrow.unit.map(_ => getter()).lowerFromOption(),
                    Arrow
                      .sleep[Unit](1.millisecond)(timer)
                      // delayed nested composition
                      .map(_ => getter()).lowerFromOption()
                  )).map {
                  case (left, right) => left && right
                },
              Arrow.exception(new Exception).handle { case _ => getter() }.lowerFromOption(),
              Arrow
                .sleep[Unit](1.millisecond)(timer).andThen(
                  Arrow.map(_ => getter())).lowerFromOption(),
              Arrow
                .sleep[Unit](1.millisecond)(timer).andThen(Arrow.join(
                  Arrow.unit.map(_ => getter()).lowerFromOption(),
                  Arrow.let(local)(false)( // differently scoped in a nested composition
                    Arrow
                      .sleep[Unit](1.millisecond)(timer).map(_ => getter()).lowerFromOption())
                )).map {
                  case (left, right) => left && !right
                },
              Arrow.unit.map(_ => getter()).lowerFromOption(),
              Arrow.letClear(local)(Arrow.unit.map(_ => getter())).map(_.isEmpty) // cleared
            )).map(_.forall(identity))))
    }
  }

  test("Twitter Locals are defined for Stitch and Arrow combinations") {
    new TwitterLocalTest {
      val arrowWithLocal: ArrowWithLocal =
        (letter: Letter, getter: Getter) =>
          Arrow
            .collect[Unit, Boolean](Seq(
              Arrow.unit
                .flatMap(_ => Stitch.Unit.before(Stitch.value(getter()))).lowerFromOption(),
              Arrow.unit
                .flatMap(_ =>
                  Stitch.Unit.before(Arrow.unit.map(_ => getter()).apply(()))).lowerFromOption(),
              Arrow.unit
                .flatMap(_ =>
                  Stitch.Unit
                    .before(
                      Arrow.unit
                        .flatMap(_ =>
                          Stitch.callFuture(Future
                            .sleep(1.millisecond)(timer)
                            .before(Future.value(getter()))))
                        .apply(())
                    ).lowerFromOption()),
              Arrow.exception(new Exception).handle { case _ => getter() }.lowerFromOption(),
              Arrow.unit
                .flatMap(_ =>
                  Stitch
                    .value(
                      Arrow.unit
                        .flatMap(_ =>
                          Stitch.callFuture(Future
                            .sleep(1.millisecond)(timer)
                            .before(Future.value(getter())))).apply(())
                    ).flatten).lowerFromOption(),
              Arrow.unit
                .flatMap(_ =>
                  Stitch // Arrow.value evaluated at runtime so should have locals
                    .value(Arrow.value(getter()).apply(())).flatten).lowerFromOption(),
              Arrow
                .ifelse[Unit, Option[Boolean]](
                  _ => getter().get,
                  Arrow.unit.map(_ => getter()),
                  Arrow.NotFound
                ).lowerFromOption(),
              Arrow
                .EnabledBy[Unit, Option[Boolean]](
                  () => getter().get,
                  Arrow.unit.map(_ => getter()),
                  Arrow.NotFound).lowerFromOption()
            )).map(_.forall(identity))

      // let scoped outside Arrow.run at runtime (existing behavior)
      {
        val a = arrowWithLocal(identity, () => local())
        assert(local.let(true)(await(a.apply(()))))
        assert(local.let(true)(await(a.traverse(Seq((), ())))).forall(identity))
      }
      // let scoped inside Stitch.run at runtime (new behavior)
      {
        val a = arrowWithLocal(identity, () => local())
        assert(await(Arrow.let(local)(true)(a).apply(())))
        assert(await(Arrow.let(local)(true)(a).traverse(Seq((), ()))).forall(identity))
      }
      {
        val a = arrowWithLocal(identity, () => local())
        assert(await(Stitch.let(local)(true)(a.apply(()))))
        assert(await(Stitch.let(local)(true)(a.traverse(Seq((), ())))).forall(identity))
      }
    }
  }

  test("Locals are missing and the correct exception is set when by-name values throw") {
    val local = new Local[Int]
    val e = new Exception

    val input = Seq(Return(0), Return(1))
    val expected = Seq(Return(2), Return(2))

    val a =
      Arrow
        .lowerFromTry[Int]
        .andThen(Arrow.let[Int, Int, Int](local)(throw e)(
          Arrow.handle {
            // only catch the exact exception we threw in let and only if the local isn't set
            case t if t == e && local().isEmpty => 2
          }
        )).liftToTry
    assert(await(a(input.head)) == expected.head)
    assert(await(a.traverse(input)) == expected)
  }

  test("Twitter Locals are defined in mixed Return and Throw states") {
    val local = new Local[Int]

    val t = Throw(new Exception)
    val caughtException = new Exception
    val thrownDuringWithArg = new Exception

    val inputAndExpected = Seq(
      (
        Seq(Return(4)),
        Seq(Throw(thrownDuringWithArg))
      ),
      (
        Seq(Return(0), Return(2)),
        Seq(Return(0), Return(2))
      ),
      (
        Seq(Return(0), Return(2), Return(4), Return(6)),
        Seq(Return(0), Return(2), Throw(thrownDuringWithArg), Return(6))
      ),
      (
        Seq(Return(0), t, Throw(caughtException), t, Return(4), t, Return(6)),
        Seq(Return(0), t, Return(99), t, Throw(thrownDuringWithArg), t, Return(6))
      ),
      (
        Seq(Return(4), t, Return(4), t, Return(4), t, Return(4)),
        Seq(
          Throw(thrownDuringWithArg),
          t,
          Throw(thrownDuringWithArg),
          t,
          Throw(thrownDuringWithArg),
          t,
          Throw(thrownDuringWithArg))
      ),
      (
        Seq(t, t, t),
        Seq(t, t, t)
      ),
    )

    inputAndExpected.foreach {
      case (input, expected) =>
        { // mixed batch, letWithArg local
          val a =
            Arrow
              .lowerFromTry[Int].andThen(
                Arrow.letWithArg[Int, Int, Int](local)(i =>
                  if (i == 4) throw thrownDuringWithArg else i)(
                  Arrow
                    .map[Any, Int](_ => local().get)
                    .handle { case e if e == caughtException => 99 }
                )).liftToTry
          assert(await(a.traverse(input)) == expected)
        }
        { // mixed batch, let local
          val a =
            Arrow
              .lowerFromTry[Int].andThen(
                Arrow.let[Int, Int, Int](local)(1)(
                  Arrow
                    .map[Any, Int](_ => local().get)
                    .handle { case e => local().get }
                )).liftToTry
          assert(await(a.traverse(input)) == input.map(_ => Return(1)))
        }
        { // mixed batch, dynamic let local, dependent values
          val a =
            Arrow
              .lowerFromTry[Int].andThen(
                Arrow.let[Int, Int, Int](local)(0)(
                  Arrow.let[Int, Int, Int](local)(local().get + 1)(
                    Arrow
                      .map[Any, Int](_ => local().get)
                      .handle { case e => local().get }
                  ))).liftToTry
          assert(await(a.traverse(input)) == input.map(_ => Return(1)))
        }
    }
  }

  test("Twitter Locals scoped with Arrow.let are not defined in Groups") {
    val localAroundWholeStitch = new Local[Unit]
    val localAroundPartOfStitch = new Local[Unit]

    val g = new SeqGroup[Int, Int] {
      override protected def run(keys: Seq[Int]): Future[Seq[Try[Int]]] = {
        assert(localAroundPartOfStitch().isEmpty)
        assert(localAroundWholeStitch().isDefined)
        Future.value(keys.map(Return(_)))
      }
    }

    val a = Arrow.let[Int, Int, Unit](localAroundPartOfStitch)(())(Arrow.call(g))
    localAroundWholeStitch.let(())(await(a.apply(1)))
  }

  test("Twitter Locals scoped with Arrow.let you care about can be passed into a Group") {
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

    val a = Arrow.join(
      Arrow.letWithArg[Int, Option[Int], Int](l)(i => -i)(
        Arrow
          .map[Int, (Int, Option[Int])](i => (i, l.apply()))
          .andThen(Arrow.call(localValue => GroupWithLocalInConstructor(localValue)))),
      // local let-scoped around the Stitch.run applies here
      Arrow
        .map[Int, (Int, Option[Int])](i => (i + 1, l.apply()))
        .andThen(Arrow.call(localValue => GroupWithLocalInConstructor(localValue)))
    )
    assert(l.let(-2)(await(a(1))) == ((Some(-1), Some(-2))))
  }

  test(
    "Twitter Locals scoped with Arrow.let you care about can be passed into a Group with a call") {
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

    val a = Arrow.join(
      Arrow.letWithArg[Int, Option[Int], Int](local)(i => -i)(
        Arrow
          .map[Int, IntWithContext](i => IntWithContext(i, local.apply()))
          .andThen(Arrow.call(groupWithLocalInInputArg))),
      // local let-scoped around the Stitch.run applies here
      Arrow
        .map[Int, IntWithContext](i => IntWithContext(i + 1, local.apply()))
        .andThen(Arrow.call(groupWithLocalInInputArg))
    )
    assert(local.let(-2)(await(a(1))) == ((Some(-1), Some(-2))))
  }
}
