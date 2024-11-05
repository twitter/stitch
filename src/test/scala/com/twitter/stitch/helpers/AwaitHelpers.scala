package com.twitter.stitch.helpers

import com.twitter.conversions.DurationOps._
import com.twitter.stitch.Stitch
import com.twitter.util.{Await, Duration, Future}

trait AwaitHelpers {
  def await[T](s: Stitch[T], timeout: Duration = 2.seconds): T = awaitFuture(Stitch.run(s), timeout)
  def awaitFuture[T](f: Future[T], timeout: Duration = 2.seconds): T = Await.result(f, timeout)
}
