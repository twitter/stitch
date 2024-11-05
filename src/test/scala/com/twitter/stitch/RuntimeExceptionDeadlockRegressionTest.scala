package com.twitter.stitch

import com.twitter.conversions.DurationOps._
import com.twitter.stitch.helpers.StitchTestSuite
import com.twitter.util.{Await, Future, Try}

class StitchRuntimeException(message: String) extends RuntimeException(message)

case class Uppercaser(shouldFail: Boolean) extends MapGroup[String, String] {
  override def maxSize = 20

  def run(strings: Seq[String]): Future[String => Try[String]] = {
    val upperCased = strings
      .map({ s => s -> s.toUpperCase })
      .toMap

    if (shouldFail) { throw new StitchRuntimeException("ha ha") }

    Future.value({ s: String =>
      Try { upperCased.getOrElse(s, throw new RuntimeException("key not found")) }
    })
  }
}

object Uppercaser {
  // For convenience.
  def upcase(s: String, shouldFail: Boolean): Stitch[String] =
    Stitch.call(s, Uppercaser(shouldFail))
}

/**
 * A regression test to make sure that Stitches don't produce Futures that never complete. See STITCH-80.
 */
class RuntimeExceptionDeadlockRegressionTest extends StitchTestSuite {
  test("Uppercaser(shouldFail = false).run should work") {
    Await.result(Stitch.run(Uppercaser.upcase("foo", shouldFail = false))) must be("FOO")
  }

  test("Uppercaser(shouldFail = true).run should raise a runtime exception for simple Stitches") {
    a[StitchRuntimeException] must be thrownBy {
      Await.result(Stitch.run(Uppercaser.upcase("foo", shouldFail = true)), 200.milliseconds)
    }
  }

  test("Uppercaser(shouldFail = true).run should raise a runtime exception for complex Stitches") {
    val firstStitch = Uppercaser.upcase("foo", shouldFail = false)
    val secondStitch = firstStitch.flatMap { _ => Uppercaser.upcase("bar", shouldFail = true) }
    val theFuture = Stitch.run(secondStitch)
    a[StitchRuntimeException] must be thrownBy {
      Await.result(theFuture, 200.milliseconds)
    }
  }
}
