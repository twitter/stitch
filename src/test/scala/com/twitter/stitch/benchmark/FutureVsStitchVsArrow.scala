package com.twitter.stitch.benchmark

import com.twitter.stitch.Arrow
import com.twitter.stitch.ArrowGroup
import com.twitter.stitch.Stitch
import com.twitter.stitch.MapGroup
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Try
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Warmup
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Setup

import java.util.concurrent.TimeUnit

/**
 * To run the suite, you can do:
 * `bazel run stitch/stitch-core/src/test/scala/com/twitter/stitch/benchmark -- -prof gc`
 *
 * @note it's recommended to close all other applications when running benchmarks
 *
 * This benchmark suite builds up a computation of [[FutureVsStitchVsArrowState.syncOperations]] synchronous operations (`.map`)
 * with [[FutureVsStitchVsArrowState.asyncOperations]] async operations evenly distributed through out the synchronous operations.
 * Concurrent actions are added with `.collect` in 2 levels, and for [[Arrow]] and [[Stitch]] the outer level of concurrent actions
 * are handled with a [[Stitch.traverse]] or using the [[Arrow.traverse]] methods. Similar tests are added for using
 * [[Arrow.callFuture]]/[[Stitch.callFuture]] and compared to the benefits of batching using [[MapGroup]]s instead.
 *
 * @example for
 *          [[FutureVsStitchVsArrowState.inputSize]] = 2
 *          [[FutureVsStitchVsArrowState.parallelOperations]] = 2
 *          [[FutureVsStitchVsArrowState.syncOperations]] = 3
 *          [[FutureVsStitchVsArrowState.asyncOperations]] = 1
 *          the resulting computation would look something like this:
 *          {{{
 *            def forEachInput =  Const(0).map(_ + 1).map(_ + 1).map(_ + 1).flatMap(_ => futureThatWillBeCompletedLater)
 *            def inParallel = collect((0 to parallelOperations).map(_ => forEachInput))
 *            collect((0 to inputSize).map(_ => inParallel))
 *          }}}
 *          which unwinds to:
 *          {{{
 *            [ // inputSize
 *              [ // parallelOperations
 *                Const(0).map(_ + 1).map(_ + 1).map(_ + 1).flatMap(_ => futureThatWillBeCompletedLater),
 *                Const(0).map(_ + 1).map(_ + 1).map(_ + 1).flatMap(_ => futureThatWillBeCompletedLater)
 *              ],
 *              [ // parallelOperations
 *                Const(0).map(_ + 1).map(_ + 1).map(_ + 1).flatMap(_ => futureThatWillBeCompletedLater),
 *                Const(0).map(_ + 1).map(_ + 1).map(_ + 1).flatMap(_ => futureThatWillBeCompletedLater)
 *              ],
 *            ]
 *          }}}
 *
 * @note we specifically use [[Promise]] and set the value instead of using a [[Future.const]] to
 *       avoid some optimizations that [[Future]] will do when combining [[Future.const]] to better
 *       mimic actual async behavior.
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
class FutureVsStitchVsArrow {

  /** [[Benchmark]] for testing [[Future]] performance */
  @Benchmark def future(state: FutureVsStitchVsArrowState): Double = {
    def mkFutures(): Future[Int] = {
      (0 to state.syncOperations)
        .grouped(state.syncOperationsBetweenAsyncOperations)
        .foldLeft(Future.value(0)) {
          case (s, syncOps) =>
            syncOps
              .foldLeft(s) { case (s, _) => s.map { _ + 1 } }
              .flatMap { _ =>
                val p = new Promise[Int]()
                p.setValue(0)
                p
              }
        }
    }

    def collect(i: Int): Future[Seq[Int]] =
      Future.collect((0 to state.parallelOperations).map(_ => mkFutures()))

    val f = Future.collect((0 to state.inputSize).map(collect))
    Await.result(f, Duration.fromSeconds(1)).flatten.sum
  }

  /** [[Benchmark]] for testing [[Future]] performance */
  @Benchmark def futureBatched(state: FutureVsStitchVsArrowState): Double = {
    def mkFutures(seq: Seq[Int]): Future[Seq[Int]] = {
      (0 to state.syncOperations)
        .grouped(state.syncOperationsBetweenAsyncOperations)
        .foldLeft(Future.value(seq)) {
          case (s, syncOps) =>
            syncOps
              .foldLeft(s) { case (s, _) => s.map { _.map(_ + 1) } }
              .flatMap { s =>
                val p = new Promise[Seq[Int]]()
                p.setValue(s)
                p
              }
        }
    }

    val f = Future
      .collect(
        (0 to state.inputSize)
          .map(_ => mkFutures(0 to state.parallelOperations)))
    Await.result(f, Duration.fromSeconds(1)).flatten.sum
  }

  /** [[Benchmark]] for testing [[Stitch]] performance when using [[Stitch.callFuture]] */
  @Benchmark def stitchCallFuture(state: FutureVsStitchVsArrowState): Int = {
    def mkStitch(): Stitch[Int] = {
      (0 to state.syncOperations)
        .grouped(state.syncOperationsBetweenAsyncOperations)
        .foldLeft(Stitch.value(0)) {
          case (s, syncOps) =>
            syncOps
              .foldLeft(s) { case (s, _) => s.map { _ + 1 } }
              .flatMap { _ =>
                val p = new Promise[Int]()
                p.setValue(0)
                Stitch.callFuture(p)
              }
        }
    }

    def collect(i: Int): Stitch[Seq[Int]] =
      Stitch.collect((0 to state.parallelOperations).map(_ => mkStitch()))

    val s = Stitch.traverse(0 to state.inputSize)(collect)
    val f = Stitch.run(s)
    Await.result(f, Duration.fromSeconds(1)).flatten.sum
  }

  /** [[Benchmark]] for testing [[Stitch]] performance when using [[MapGroup]] */
  @Benchmark def stitchGroup(state: FutureVsStitchVsArrowState): Int = {
    def mkStitch(): Stitch[Int] = {
      (0 to state.syncOperations)
        .grouped(state.syncOperationsBetweenAsyncOperations)
        .foldLeft(Stitch.value(0)) {
          case (s, syncOps) =>
            syncOps
              .foldLeft(s) { case (s, _) => s.map { _ + 1 } }
              .flatMap(i => Stitch.call(i, TestGroup))
        }
    }

    def collect(i: Int): Stitch[Seq[Int]] =
      Stitch.collect((0 to state.parallelOperations).map(_ => mkStitch()))

    val s = Stitch.traverse(0 to state.inputSize)(collect)
    val f = Stitch.run(s)
    Await.result(f, Duration.fromSeconds(1)).flatten.sum
  }

  /** [[Benchmark]] for testing [[Arrow]] performance when using [[Arrow.callFuture]] */
  @Benchmark def arrowCallFuture(state: FutureVsStitchVsArrowState): Int = {
    val f = Stitch.run(state.arrowCallFuture.traverse((0 to state.inputSize)))
    Await.result(f, Duration.fromSeconds(1)).flatten.sum
  }

  /** [[Benchmark]] for testing [[Arrow]] performance when using [[ArrowGroup]] */
  @Benchmark def arrowGroup(state: FutureVsStitchVsArrowState): Int = {
    val f = Stitch.run(state.arrowGroup.traverse((0 to state.inputSize)))
    Await.result(f, Duration.fromSeconds(1)).flatten.sum
  }
}

@State(Scope.Benchmark)
class FutureVsStitchVsArrowState {

  /** size of the input to run, this is the size of the seq passed to [[Arrow.traverse]] but done for [[Future]] and [[Stitch]] too */
  @Param(Array("1", "10", "30"))
  var inputSize: Int = _

  /** the overall number of synchronous continuations to execute (`.map`) */
  @Param(Array("10", "50", "100", "200"))
  var syncOperations: Int = _

  /** the overall number of async continuations to execute (`.flatMap` and `.callFuture` containing a [[Promise]]) */
  @Param(Array("1", "5", "10", "20"))
  var asyncOperations: Int = _

  /** the number of operations to do in parallel for each input */
  @Param(Array("1", "10", "20"))
  var parallelOperations: Int = _

  var syncOperationsBetweenAsyncOperations: Int = _

  /** [[Arrow]]s are made ahead of time */
  var arrowCallFuture: Arrow[Int, Seq[Int]] = _
  var arrowGroup: Arrow[Int, Seq[Int]] = _

  /**
   * Setup the state based on the configured [[Param]]s, ensuring that we instantiate them
   * all within the [[Setup]] annotated method and not access any of them outside of there
   */
  @Setup(Level.Trial)
  def setup(): Unit = {
    syncOperationsBetweenAsyncOperations = syncOperations / asyncOperations

    arrowCallFuture = {
      def singularCase: Arrow[Int, Int] =
        (0 to syncOperations)
          .grouped(syncOperationsBetweenAsyncOperations)
          .foldLeft(Arrow.identity[Int]) {
            case (arrow, syncOps) =>
              syncOps
                .foldLeft(arrow) {
                  case (arrow, _) => arrow.map(_ + 1)
                }.andThen(
                  Arrow.callFuture { _ =>
                    val p = new Promise[Int]()
                    p.setValue(0)
                    p
                  }
                )
          }
      Arrow.collect((0 to parallelOperations).map(_ => singularCase))
    }

    arrowGroup = {
      def singularCase: Arrow[Int, Int] =
        (0 to syncOperations)
          .grouped(syncOperationsBetweenAsyncOperations)
          .foldLeft(Arrow.identity[Int]) {
            case (arrow, syncOps) =>
              syncOps
                .foldLeft(arrow) {
                  case (arrow, _) => arrow.map(_ + 1)
                }.andThen(Arrow.call(TestGroup))
          }
      Arrow.collect((0 to parallelOperations).map(_ => singularCase))
    }
  }
}

/** In the batch case for [[Stitch]], [[Arrow]], and [[Future]] we only have a single [[Promise]] */
case object TestGroup extends MapGroup[Int, Int] with ArrowGroup[Int, Int] {
  override protected def run(keys: Seq[Int]): Future[Int => Try[Int]] = {
    val p = new Promise[Map[Int, Try[Int]]]
    p.setValue(keys.map(i => i -> Return(i)).toMap)
    p
  }
}
