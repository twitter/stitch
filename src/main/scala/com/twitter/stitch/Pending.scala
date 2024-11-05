package com.twitter.stitch

import com.twitter.stitch.Arrow.Apply
import com.twitter.stitch.Stitch.CallFutureGroup
import com.twitter.stitch.Stitch.Const
import com.twitter.stitch.Stitch.SFuture
import com.twitter.stitch.Stitch.Transform
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Try

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

private trait Pending {
  def add[C, T](c: C, g: Group[C, T]): Stitch[T]
  def addSeqTry[C, T](cs: Seq[Try[C]], group: ArrowGroup[C, T]): Stitch[C => Try[T]]
  def addNewRoot(s: Stitch[_]): Unit
  def recursive[T](s: Stitch.Recursive[T]): Stitch[T]
}

object Run {
  private val emptyRunnerArray = new Array[Runner[Any, Any]](0)
  private[stitch] val maxRecursionDepth =
    System.getProperty("stitch.maxRecursionDepth", "200").toInt
}

/** Run represents a single run of a query. It tracks the current
 *  simplified version of the query and the pending calls at each phase
 *  of the run.
 */
private class Run[T](s: Stitch[T]) extends Pending {
  private[this] val roots = new ArrayBuffer[Stitch[_]](1)
  // add the root Stitch as part of the class constructor
  // to avoid capturing the Stitch as a field, which can
  // lead to memory leaks in recursive computations
  roots += s

  private[this] val runnersByGroup = new java.util.HashMap[Any, Runner[_, _]]()
  private[this] val futures = new ArrayBuffer[Future[_]](0)
  private[this] val nwaiters = new AtomicInteger(0)
  private[this] val interrupts = new ConcurrentLinkedQueue[Throwable]
  private[this] val recursions = new util.ArrayDeque[Stitch.Recursive[_]](0)
  private[this] var recursionDepth = 0

  // Allows on-stack recursion until `maxRecursionDepth` is
  // reached, at which point the recursion is delayed
  // and the thread stack unfolded
  def recursive[U](s: Stitch.Recursive[U]): Stitch[U] = {
    if (recursionDepth < Run.maxRecursionDepth) {
      // still hasn't reached the on-stack recursion
      // limit, evaluate and simplify immediately
      recursionDepth += 1
      try s.s().simplify(this)
      finally recursionDepth -= 1
    } else {
      // limit has been reached, attach the recursion
      // to the current run loop for delayed evaluation
      recursions.push(s)
      s
    }
  }

  private[this] val p: Promise[T] = {
    val handler: PartialFunction[Throwable, Unit] = {
      case e =>
        interrupts.add(e)
        loop()
    }
    new Promise[T](handler)
  }

  private[this] def runnerOfGroup[C, R](group: Group[C, R]) = {
    var runner = runnersByGroup.get(group).asInstanceOf[Runner[C, R]]
    if (runner == null) {
      runner = group.runner
      runnersByGroup.put(group, runner)
    }
    runner
  }

  private[this] def runnerOfArrowGroup[C, R](group: ArrowGroup[C, R]) =
    runnerOfGroup(group).asInstanceOf[ArrowFutureRunner[C, R]]

  override def add[C, R](c: C, g: Group[C, R]): Stitch[R] =
    runnerOfGroup(g).add(c)

  override def addSeqTry[C, R](cs: Seq[Try[C]], group: ArrowGroup[C, R]): Stitch[C => Try[R]] =
    Stitch.future(runnerOfArrowGroup(group).addSeqTry(cs))

  override def addNewRoot(s: Stitch[_]): Unit = {
    roots += s
  }

  private[this] def loop(): Unit = {
    if (nwaiters.getAndIncrement() == 0) {
      do {
        val interrupt = interrupts.poll()
        if (interrupt ne null) {
          raiseOnPendingFutures(interrupt)
        }

        pruneSatisfiedFutures()
        simplifyAndPruneRoots()
        runNewBatches()
      } while (nwaiters.decrementAndGet() > 0)
    }
  }

  private[this] val loopCallback: Try[Any] => Unit = _ => loop()

  private[this] def pruneSatisfiedFutures(): Unit = {
    var i = 0
    var j = 0

    while (i < futures.length) {
      futures(i) match {
        case f if !f.isDefined =>
          futures(j) = f
          j += 1
        case _ =>
      }
      i += 1
    }

    futures.reduceToSize(j)
  }

  private[this] def simplifyAndPruneRoots(): Unit = {
    var i = 0
    var j = 0

    while (i < roots.length) {
      @tailrec def loop(s: Stitch[_]): Unit =
        s.simplify(this) match {
          case Stitch.Const(r) =>
            // satisfy the promise if the primary root is satisfied
            if (i == 0) p.updateIfEmpty(r.asInstanceOf[Try[T]])
          case s if !recursions.isEmpty =>
            // evaluate recursions and re-simplify
            do {
              recursions.poll().eval()
            } while (!recursions.isEmpty)
            // update root to avoid memory leak
            roots(i) = s
            loop(s)
          case s =>
            roots(j) = s
            j += 1
        }
      loop(roots(i))
      i += 1
    }

    roots.reduceToSize(j)
  }

  private[this] def runNewBatches(): Unit = {
    var i = 0
    val runners = runnersByGroup.values.toArray(Run.emptyRunnerArray)
    runnersByGroup.clear()

    while (i < runners.length) {
      runners(i) match {
        case runner: FutureRunner[_, _] =>
          val f = runner.run()
          futures += f
          f.respond(loopCallback)

        case runner: StitchRunner[_, _] =>
          runner.run()
          // increment nwaiters to force another loop run
          nwaiters.getAndIncrement()
      }
      i += 1
    }
  }

  private[this] def raiseOnPendingFutures(e: Throwable): Unit = {
    var i = 0

    while (i < futures.length) {
      futures(i).raise(e)
      i += 1
    }
  }

  def run(): Future[T] = {
    loop()
    p
  }
}

/**
 * A version of [[Run]] that maintains a local cache that lives for the duration of the evaluation.
 */
private final class CachedRun[T](s: Stitch[T]) extends Run(s) {
  private[this] val localCache = new java.util.HashMap[(Any, Any), Stitch[_]]()

  override def add[C, R](c: C, g: Group[C, R]): Stitch[R] = {
    (g match {
      case cfg: CallFutureGroup[_] =>
        /**
         * If it's a [[CallFutureGroup]] we want to check the cache to see if the parent [[Stitch.CallFuture]] instance is there instead
         * this is because there will be different [[CallFutureGroup]] instances depending on the available [[com.twitter.util.Local]]s in scope
         * at their creation, but if it's already been run we can just access the already computed value
         */
        (
          cfg.parentCallFutureInstance,
          localCache.get((c, cfg.parentCallFutureInstance)).asInstanceOf[Stitch[R]])
      case _ =>
        (g, localCache.get((c, g)).asInstanceOf[Stitch[R]])
    }) match {
      case (localCacheKey, null) =>
        // cache is empty, so go to the Runner with the actual group, not the `localCacheKey`
        val stitch: Stitch[R] = super.add(c, g)
        stitch match {
          case s @ (SFuture(_, _) | Const(_)) =>
            // simple single RPCs and constants can be cached
            localCache.put((c, localCacheKey), s)
          case s @ Transform(SFuture(_, _), _, a) if a.isInstanceOf[Apply[_, R]] =>
            // this is the kind of Stitch a SeqRunner or a MapRunner returns from [[add]]
            // it is ok to cache this because the Apply Arrow just unpacks the batch RPC response
            localCache.put((c, localCacheKey), s)
          case _ =>
          // do not cache other types of Stitches
        }
        stitch

      case (localCacheKey, s @ (SFuture(_, _) | Const(_) | Transform(_, _, _))) =>
        // this call to [[simplify]] will return self, if the Future is not Done;
        // or a Const if the Future is Done
        val simplified = s.simplify(this)
        localCache.put((c, localCacheKey), simplified)
        simplified

      case _ =>
        // we are guaranteed not to get here
        throw new UnsupportedOperationException("Unexpected")

    }
  }
}
