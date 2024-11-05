package com.twitter.stitch

import com.twitter.util.{Time => _, _}
import com.twitter.util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.ArrayBuffer
import scala.runtime.NonLocalReturnControl
import scala.util.control.NoStackTrace
import scala.util.control.NonFatal

case object NotFound extends Exception with NoStackTrace
case object Timeout extends Exception with NoStackTrace
class StitchInvalidState extends Exception
class StitchNonLocalReturnControl(cause: NonLocalReturnControl[_]) extends Exception(cause) {
  override def getMessage: String = "Invalid use of `return` in closure passed to a Stitch"
}

/**
 * A Stitch computation is like a [[com.twitter.util.Future]] except
 * that atomic computations (e.g. calls to RPC services) are not
 * executed immediately, but only at points where the whole
 * computation is blocked. This permits them to be batched together
 * (to take advantage of batch RPC interfaces transparently) and
 * otherwise rewritten (to provide more convenient APIs on top of raw
 * RPC service APIs).
 *
 * Like a Future, a Stitch represents an asynchronous computation,
 * the value of which may not yet be available. When the value (or
 * exception) becomes available (e.g. because an underlying RPC call
 * has returned) we say the Stitch computation "completes".
 *
 * Like a Future, a Stitch embodies a [[com.twitter.util.Try]], and
 * may complete successfully with a value or with an
 * exception. (Stitch computations do not actually '''throw'''
 * exceptions.) We say "succeeds with `v`" to mean "completes
 * successfully with value v" and "fails with `e`" to mean "completes
 * with exception `e`".
 *
 * Like Futures, Stitch computations may be composed in two ways:
 *
 *  - dependent composition, where one computation depends on the result of another (e.g. `flatMap`, `rescue`)
 *  - concurrent composition, where sub-computations have no interdependencies (e.g. `join`, `collect`, `traverse`)
 *
 * As with Futures, concurrently composed Stitch computations may be
 * executed concurrently (e.g. concurrent calls to underlying
 * services); moreover, execution of a Stitch query can "see" all
 * concurrent computations together, so they may be batched together,
 * identical calls may be coalesced, and so on.
 *
 * Generally concurrent composition is preferable wherever possible
 * because it offers more scope for optimization. A particular hazard
 * is a "false dependency", where there is not a data dependency
 * between two computations, but one is hidden inside a `flatMap` of
 * another, so it can't be executed (optimized) until the other has
 * completed. E.g. in {{{
 *   s1 flatMap { s1 =>
 *     s2 flatMap { s2 => ... }
 * }}} or equivalently {{{
 *   for {
 *     s1 <- s1
 *     s2 <- s2
 *     ...
 *   }
 * }}} the execution of `s2` can't begin until `s1` completes, even
 * though `s2` doesn't depend on the result of `s1`. We can replace
 * dependent composition with concurrent composition here {{{
 *   Stitch.join(s1, s2) flatMap { (s1, s2) => ... }
 * }}}
 *
 * Unlike a Future, a Stitch does not start executing when it's
 * created; you must call `Stitch.run` to start its execution (or
 * include it in a larger Stitch computation which is passed to
 * `Stitch.run`). Batching happens only within the scope of a call to
 * `Stitch.run`, so it's best to have only one such call in a
 * particular Future-returning code path (e.g. the implementation of
 * an RPC endpoint).
 *
 * In particular, side-effecting operations like `respond` aren't run
 * unless they're part of a computation passed to `Stitch.run`. When
 * you want to execute computations asynchronously for their
 * side-effects, you can use `Stitch.async` to embed them into the
 * currently-running computation.
 *
 * Stitch callbacks within a single execution of `Stitch.run` are
 * never executed concurrently, so it's safe to modify per-run state
 * without additional synchronization; however callbacks in different
 * runs may be executed concurrently.
 */
sealed abstract class Stitch[+T] {
  // abstract class so companion object methods are callable as static method from Java
  // sealed since we assume we can pattern-match exhaustively over the cases given here

  /** Returns a Stitch which succeeds with `f(v)` when the underlying
   * Stitch succeeds with `v`; or fails with `e` when the underlying
   * Stitch fails with `e` or the application `f(v)` throws `e`.
   *
   * @example {{{
   *   val s: Stitch[String] = ...
   *   val i: Stitch[Int] = s.map(_.toInt)
   * }}}
   */
  def map[U](f: T => U): Stitch[U] = Arrow.Map(f).apply(this)

  /** Returns a Stitch which applies `f(v)` when the underlying Stitch
   * succeeds with `v`, then succeeds/fails the same as `f(v)`; or
   * fails with `e` when the underlying Stitch fails with `e` or the
   * application `f(v)` throws `e`.
   *
   * @example {{{
   *   def toInt(s: String): Stitch[Int] = ... // i.e. an RPC service
   *   val s: Stitch[String] = ...
   *   val i: Stitch[Int] = s.flatMap { s => toInt(s) }
   * }}}
   */
  def flatMap[U](f: T => Stitch[U]): Stitch[U] = Arrow.FlatMap(f).apply(this)

  /** Returns a Stitch which composes this with `f`. This is a [[flatMap]],
   * but discards the result of `this`.
   *
   * @note this applies only to `Unit`-valued Stitches - i.e. side-effects
   */
  def before[U](f: => Stitch[U])(implicit ev: T <:< Unit): Stitch[U] =
    Arrow.FlatMap((_: T) => f).apply(this)

  /** Collapses a `Stitch[Stitch[T]]` into a `Stitch[T]`
   *
   * @example {{{
   *    val s: Stitch[Stitch[Boolean]] = Stitch.value(Stitch.value(true))
   *    val i: Stitch[Boolean] = s.flatten
   * }}}
   */
  def flatten[U](implicit ev: T <:< Stitch[U]): Stitch[U] = flatMap(identity[T])

  /** Returns a Stitch which succeeds with `v` when the underlying
   * Stitch succeeds with `v` and `p(v)` is true; or fails with
   * [[scala.MatchError]] when `p(v)` is false, or fails with `e`
   * when the application `p(v)` throws `e`.
   *
   * This is mostly useful by way of `for`-expressions, see the
   * example.
   *
   * @example {{{
   *   val so: Stitch[Option[String]] = ...
   *   for {
   *     Some(s) <- so // compiles to a call to filter
   *   } yield s
   * }}}
   */
  def filter(p: T => Boolean): Stitch[T] = map { v =>
    if (p(v)) v
    else throw new MatchError(this)
  }

  /** Used by for-comprehensions
   */
  final def withFilter(p: T => Boolean): Stitch[T] = filter(p)

  /** Returns a Stitch which succeeds with `f(e)` when the underlying
   * Stitch fails with `e`; or succeeds with `v` when the underlying
   * Stitch succeeds with `v`; or fails with `e2` when the
   * application `f(e)` throws `e2`.
   *
   * @example {{{
   *   val s: Stitch[Option[String]] = ...
   *   val s2 = s.handle { case e => None }
   * }}}
   */
  def handle[U >: T](f: PartialFunction[Throwable, U]): Stitch[U] =
    Arrow.Handle[T, U](f).apply(this)

  /** Returns a Stitch which applies `f(e)` when the underlying Stitch
   * fails with `e`, then succeeds/fails the same as `f(e)`; or
   * succeeds with `v` when the underlying Stitch succeeds with `v`;
   * or fails with `e2` when the application `f(e)` throws `e2`.
   *
   * @example {{{
   *   val s: Stitch[String] = ...
   *   def toInt(s: String): Stitch[Int] = ... // i.e. an RPC service
   *   def defaultInt(): Stitch[Int] = ... // i.e. an RPC service
   *   val i: Stitch[Int] = s.flatMap(toInt).handle { case e => defaultInt() }
   * }}}
   */
  def rescue[U >: T](f: PartialFunction[Throwable, Stitch[U]]): Stitch[U] =
    Arrow.Rescue[T, U](f).apply(this)

  /**
   * Invoked regardless of whether the computation succeeds or fails.
   */
  def ensure(f: => Unit): Stitch[T] = Arrow.Ensure[T](() => f).apply(this)

  /**
   * Returns a Stitch which applies `f(t)` on the `Try[T]` result of the underlying Stitch and returns the same
   * value as the underlying Stitch. `f` should be used only for side-effects.
   *
   * @note All NonFatal exceptions in `f` are silently ignored.
   * @note Result of [[respond]] must be included into a computation passed to [[Stitch.run]] for the side-effect to
   *       apply.
   * @param f the function to apply when computation completes
   * @return a new Stitch[T] encapsulating the side-effect
   */
  def respond(f: Try[T] => Unit): Stitch[T] = Arrow.Respond(f).apply(this)

  /**
   * Returns a Stitch which applies `f(v)` when the underlying Stitch
   * succeeds with `v`, then succeeds as `v`. This should be used purely for side effects.
   *
   * @note All NonFatal exceptions in `f` are silently ignored.
   * @note Result of [[onSuccess]] must be included into a computation passed to [[Stitch.run]] for the side-effect to
   *       apply.
   */
  def onSuccess(f: T => Unit): Stitch[T] = Arrow.OnSuccess(f).apply(this)

  /**
   * Returns a Stitch which applies `f(e)` when the underlying Stitch
   * fails with `e`, then fails as `e`. This should be used purely for side-effects.
   *
   * @note All NonFatal exceptions in `f` are silently ignored.
   * @note Result of [[onFailure]] must be included into a computation passed to [[Stitch.run]] for the side-effect to
   *       apply.
   */
  def onFailure(f: Throwable => Unit): Stitch[T] = Arrow.OnFailure[T](f).apply(this)

  /**
   * Returns a Stitch that applies `f(v)` when the underlying Stitch succeeds with `v`.
   * If the returned `Stitch[Unit]` succeeds, then the resulting `Stitch[T]` succeeds as `v`.
   * If the returned `Stitch[Unit]` fails, then the result `Stitch[T]` fails with the same
   * failure.
   *
   * `s.applyEffect(f)` is equivalent to `s.flatMap(r => f(r).map(_ => r))`
   *
   * @note Result of [[applyEffect]] must be included into a computation passed to [[Stitch.run]]
   *       for the side-effect to apply.
   *
   * @note The given function may call and return the result of `Stitch.async` to force the
   *       effect to be executed asynchronously instead of synchronously.
   */
  def applyEffect(f: T => Stitch[Unit]): Stitch[T] = Arrow.applyEffect(Arrow.Effect(f)).apply(this)

  /** Returns a Stitch which applies `f(t)` when the underlying Stitch
   * completes with `t`, then completes the same as `f(t)`; or fails with
   * `e` when the application `f(t)` throws `e`.
   *
   * @example {{{
   *   def toInt(s: String): Stitch[Int] = ... // i.e. an RPC service
   *   val s: Stitch[String] = ...
   *   val i: Stitch[Int] = s.transform {
   *     case Return(s) => toInt(s)
   *     case Throw(e) => 0
   *   }
   * }}}
   */
  def transform[U](f: Try[T] => Stitch[U]): Stitch[U] = Arrow.FlatmapTry(f).apply(this)

  /**
   * Convert this Stitch[T] to a Stitch[Unit] by discarding the result
   */
  def unit: Stitch[Unit] = Arrow.unit[T](this)

  /** Returns a Stitch which succeeds with `Return(v)` when the
   * underlying Stitch succeeds with v; or succeeds with `Throw(e)`
   * when the underlying Stitch fails with `e`.
   */
  def liftToTry: Stitch[Try[T]] = Arrow.liftToTry[T](this)

  /** Returns a Stitch which succeeds with `v` when the underlying
   * Stitch succeeds with Return(v); or fails with e when the underlying
   * Stitch succeeds with Throw(e), or if the the underlying Stitch fails
   * in the normal way.
   */
  def lowerFromTry[U](implicit ev: this.type <:< Stitch[Try[U]]): Stitch[U] = {
    val _ = ev //suppress warning
    Arrow.lowerFromTry[U](this.asInstanceOf[Stitch[Try[U]]])
  }

  /** Returns a Stitch which succeeds with `Some(v)` when the underlying
   * Stitch succeeds with v; succeeds with `None` when the underlying
   * Stitch fails with `e` and `f(e)` is defined and true; or fails
   * with `e` when the underlying Stitch fails with `e` and `f(e)` is
   * undefined or false.
   */
  def liftToOption(
    f: PartialFunction[Throwable, Boolean] = Stitch.liftToOptionDefaultFn
  ): Stitch[Option[T]] =
    Arrow.liftToOption[T](f).apply(this)

  /** Returns a Stitch which succeeds with `Some(v)` when the underlying
   * Stitch succeeds with v; succeeds with `None` when the underlying
   * Stitch fails with `NotFound`; or fails
   * with `e` when the underlying Stitch fails with `e` != `NotFound`.
   */
  def liftNotFoundToOption: Stitch[Option[T]] =
    liftToOption(Stitch.liftNotFoundToOptionFn)

  /** Returns a Stitch which succeeds with `v` when the underlying
   * Stitch contains a `Some(v)`; fails with a caller-specified exception
   * when the underlying Stitch contains a `None`, and fails with `e` when
   * the underlying Stitch fails with an `e`.
   */
  def lowerFromOption[U](
    e: Throwable = com.twitter.stitch.NotFound
  )(
    implicit ev: this.type <:< Stitch[Option[U]]
  ): Stitch[U] = {
    val _ = ev //suppress warning
    Arrow.lowerFromOption[U](e)(this.asInstanceOf[Stitch[Option[U]]])
  }

  /** Returns a Stitch which succeeds with `v` when the underlying
   * Stitch succeeds with `v`; fails with a [com.twitter.util.TimeoutException]
   * when the underlying Stitch takes longer than `timeout` to complete,
   * and fails with `e` when the underlying Stitch fails with `e`.
   *
   * Gets its Timer from the implicit scope, which is sometimes more convenient.
   *
   * @note The deadline is calculated when Stitch.run is first called on
   *       the returned Stitch and *not* when this method is invoked.
   */
  def within(timeout: Duration)(implicit timer: Timer): Stitch[T] =
    within(timer, timeout, new TimeoutException(s"Operation timed out after $timeout"))

  def within(timer: Timer, timeout: Duration, exception: => Exception): Stitch[T] =
    Stitch.Within(this, timeout, timer)(exception)

  private[stitch] def apply[K, V](k: K)(implicit ev: T <:< (K => Try[V])): Stitch[V] = {
    val _ = ev //suppress warning
    Arrow.Apply[K, V](k)(this.asInstanceOf[Stitch[K => Try[V]]])
  }

  /** Simplify a query as far as possible, collecting pending calls along the way.
   *
   * If no simplification is possible and no sub-queries are simplified,
   * should return this rather than allocating a new object.
   */
  private[stitch] def simplify(pending: Pending): Stitch[T]
}

object Stitch {
  val Unit: Stitch[Unit] = const(Return.Unit)
  val Done: Stitch[Unit] = Unit
  val NotFound: Stitch[Nothing] = exception(com.twitter.stitch.NotFound)
  val Timeout: Stitch[Nothing] = exception(com.twitter.stitch.Timeout)
  val True: Stitch[Boolean] = const(Return.True)
  val False: Stitch[Boolean] = const(Return.False)
  val Nil: Stitch[Seq[Nothing]] = const(Return.Nil)
  val None: Stitch[Option[Nothing]] = const(Return.None)
  val Never: Stitch[Nothing] = Incomplete()

  private[stitch] val liftToOptionDefaultFn: PartialFunction[Throwable, Boolean] = {
    case _: Throwable => true
  }

  private[stitch] val liftNotFoundToOptionFn: PartialFunction[Throwable, Boolean] = {
    case com.twitter.stitch.NotFound => true
  }

  private[Stitch] val emptyMapInst: Stitch[Map[Any, Any]] = Stitch.value(Map.empty[Any, Any])
  private def emptyMap[A, B]: Stitch[Map[A, B]] = emptyMapInst.asInstanceOf[Stitch[Map[A, B]]]

  /** Lift a computation into a Stitch. Apply catches nonfatal
   * exceptions and encapsulates them in a Stitch.exception.
   */
  def apply[T](t: => T): Stitch[T] =
    try {
      const(Try(t))
    } catch {
      case nlrc: NonLocalReturnControl[_] =>
        Stitch.exception(new StitchNonLocalReturnControl(nlrc))
    }

  /** Returns a Stitch which succeeds with `(v1, v2)` when its arguments
   * succeed with `v1` and `v2`; or fails with `e` if any argument
   * fails with `e`.
   */
  def join[A, B](a: Stitch[A], b: Stitch[B]): Stitch[(A, B)] = {
    val buf = new ArrayBuffer[Stitch[Any]](2)
    buf += a
    buf += b
    collectNoCopy(buf).map { ss => (ss(0).asInstanceOf[A], ss(1).asInstanceOf[B]) }
  }

  def joinMap[A, B, R](a: Stitch[A], b: Stitch[B])(k: (A, B) => R): Stitch[R] = {
    val buf = new ArrayBuffer[Stitch[Any]](2)
    buf += a
    buf += b
    collectNoCopy(buf).map { ss => k(ss(0).asInstanceOf[A], ss(1).asInstanceOf[B]) }
  }

  /** This works like the existing joinMap, except it is called as values become available.
   * This makes it possible to implement "short-circuit" type of logic which is
   * not efficient using `joinMap`.
   *
   * @note unlike join or joinMap, partialJoinMap might not fail even if some arguments fail.
   *       This is because when k has enough information to return Some[R], the result of the
   *       remaining arguments won't matter.
   *
   * @param a a Stitch computation of type A
   * @param b a Stitch computation of type B
   * @param k the function to apply when either a or b completes
   * @tparam A the type of argument a
   * @tparam B the type of argument b
   * @tparam R the return type of the call
   * @return a Stitch[R] based on the result of function k (or failure)
   */
  def partialJoinMap[A, B, R](
    a: Stitch[A],
    b: Stitch[B]
  )(
    k: (Option[A], Option[B]) => Option[R]
  ): Stitch[R] = {
    val buf = new ArrayBuffer[Stitch[Any]](2)
    buf += a
    buf += b
    PartialCollect(buf) { s => k(s(0).asInstanceOf[Option[A]], s(1).asInstanceOf[Option[B]]) }
  }

  private val tuple3 = Tuple3[Any, Any, Any] _

  /** Returns a Stitch which succeeds with `(v1, v2, v3)` when its
   * arguments succeed with `v1`, `v2`, and `v3`; or fails with `e`
   * if any argument fails with `e`.
   */
  def join[A, B, C](a: Stitch[A], b: Stitch[B], c: Stitch[C]): Stitch[(A, B, C)] =
    joinMap(a, b, c)(tuple3).asInstanceOf[Stitch[(A, B, C)]]

  def joinMap[A, B, C, R](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C]
  )(
    k: (A, B, C) => R
  ): Stitch[R] = {
    val buf = new ArrayBuffer[Stitch[Any]](3)
    buf += a
    buf += b
    buf += c
    collectNoCopy(buf).map { ss =>
      k(ss(0).asInstanceOf[A], ss(1).asInstanceOf[B], ss(2).asInstanceOf[C])
    }
  }

  def partialJoinMap[A, B, C, R](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C]
  )(
    k: (Option[A], Option[B], Option[C]) => Option[R]
  ): Stitch[R] = {
    val buf = new ArrayBuffer[Stitch[Any]](3)
    buf += a
    buf += b
    buf += c
    PartialCollect(buf) { s =>
      k(s(0).asInstanceOf[Option[A]], s(1).asInstanceOf[Option[B]], s(2).asInstanceOf[Option[C]])
    }
  }

  private val tuple4 = Tuple4[Any, Any, Any, Any] _

  def join[A, B, C, D](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D]
  ): Stitch[(A, B, C, D)] =
    joinMap(a, b, c, d)(tuple4).asInstanceOf[Stitch[(A, B, C, D)]]

  def joinMap[A, B, C, D, R](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D]
  )(
    k: (A, B, C, D) => R
  ): Stitch[R] = {
    val buf = new ArrayBuffer[Stitch[Any]](4)
    buf += a
    buf += b
    buf += c
    buf += d
    collectNoCopy(buf).map { ss =>
      k(ss(0).asInstanceOf[A], ss(1).asInstanceOf[B], ss(2).asInstanceOf[C], ss(3).asInstanceOf[D])
    }
  }

  def partialJoinMap[A, B, C, D, R](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D]
  )(
    k: (Option[A], Option[B], Option[C], Option[D]) => Option[R]
  ): Stitch[R] = {
    val buf = new ArrayBuffer[Stitch[Any]](4)
    buf += a
    buf += b
    buf += c
    buf += d
    PartialCollect(buf) { s =>
      k(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]]
      )
    }
  }

  private val tuple5 = Tuple5[Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E]
  ): Stitch[(A, B, C, D, E)] =
    joinMap(a, b, c, d, e)(tuple5).asInstanceOf[Stitch[(A, B, C, D, E)]]

  def joinMap[A, B, C, D, E, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E]
  )(
    z: (A, B, C, D, E) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](5)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](5)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]]
      )
    }
  }

  private val tuple6 = Tuple6[Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F]
  ): Stitch[(A, B, C, D, E, F)] =
    joinMap(a, b, c, d, e, f)(tuple6).asInstanceOf[Stitch[(A, B, C, D, E, F)]]

  def joinMap[A, B, C, D, E, F, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F]
  )(
    z: (A, B, C, D, E, F) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](6)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](6)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]]
      )
    }
  }

  private val tuple7 = Tuple7[Any, Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F, G](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G]
  ): Stitch[(A, B, C, D, E, F, G)] =
    joinMap(a, b, c, d, e, f, g)(tuple7).asInstanceOf[Stitch[(A, B, C, D, E, F, G)]]

  def joinMap[A, B, C, D, E, F, G, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G]
  )(
    z: (A, B, C, D, E, F, G) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](7)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F],
        ss(6).asInstanceOf[G]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, G, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](7)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]],
        s(6).asInstanceOf[Option[G]]
      )
    }
  }

  private val tuple8 = Tuple8[Any, Any, Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F, G, H](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H]
  ): Stitch[(A, B, C, D, E, F, G, H)] =
    joinMap(a, b, c, d, e, f, g, h)(tuple8).asInstanceOf[Stitch[(A, B, C, D, E, F, G, H)]]

  def joinMap[A, B, C, D, E, F, G, H, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H]
  )(
    z: (A, B, C, D, E, F, G, H) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](8)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F],
        ss(6).asInstanceOf[G],
        ss(7).asInstanceOf[H]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, G, H, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G],
      Option[H]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](8)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]],
        s(6).asInstanceOf[Option[G]],
        s(7).asInstanceOf[Option[H]]
      )
    }
  }

  private val tuple9 = Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F, G, H, I](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I]
  ): Stitch[(A, B, C, D, E, F, G, H, I)] =
    joinMap(a, b, c, d, e, f, g, h, i)(tuple9).asInstanceOf[Stitch[(A, B, C, D, E, F, G, H, I)]]

  def joinMap[A, B, C, D, E, F, G, H, I, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I]
  )(
    z: (A, B, C, D, E, F, G, H, I) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](9)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F],
        ss(6).asInstanceOf[G],
        ss(7).asInstanceOf[H],
        ss(8).asInstanceOf[I]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, G, H, I, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H],
      Option[I]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](9)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]],
        s(6).asInstanceOf[Option[G]],
        s(7).asInstanceOf[Option[H]],
        s(8).asInstanceOf[Option[I]]
      )
    }
  }

  private val tuple10 = Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F, G, H, I, J](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J]
  ): Stitch[(A, B, C, D, E, F, G, H, I, J)] =
    joinMap(a, b, c, d, e, f, g, h, i, j)(tuple10)
      .asInstanceOf[Stitch[(A, B, C, D, E, F, G, H, I, J)]]

  def joinMap[A, B, C, D, E, F, G, H, I, J, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J]
  )(
    z: (A, B, C, D, E, F, G, H, I, J) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](10)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F],
        ss(6).asInstanceOf[G],
        ss(7).asInstanceOf[H],
        ss(8).asInstanceOf[I],
        ss(9).asInstanceOf[J]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, G, H, I, J, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H],
      Option[I], Option[J]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](10)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]],
        s(6).asInstanceOf[Option[G]],
        s(7).asInstanceOf[Option[H]],
        s(8).asInstanceOf[Option[I]],
        s(9).asInstanceOf[Option[J]]
      )
    }
  }

  private val tuple11 = Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F, G, H, I, J, K](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K]
  ): Stitch[(A, B, C, D, E, F, G, H, I, J, K)] =
    joinMap(a, b, c, d, e, f, g, h, i, j, k)(tuple11)
      .asInstanceOf[Stitch[(A, B, C, D, E, F, G, H, I, J, K)]]

  def joinMap[A, B, C, D, E, F, G, H, I, J, K, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K]
  )(
    z: (A, B, C, D, E, F, G, H, I, J, K) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](11)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    buf += k
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F],
        ss(6).asInstanceOf[G],
        ss(7).asInstanceOf[H],
        ss(8).asInstanceOf[I],
        ss(9).asInstanceOf[J],
        ss(10).asInstanceOf[K]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, G, H, I, J, K, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H],
      Option[I], Option[J], Option[K]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](11)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    buf += k
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]],
        s(6).asInstanceOf[Option[G]],
        s(7).asInstanceOf[Option[H]],
        s(8).asInstanceOf[Option[I]],
        s(9).asInstanceOf[Option[J]],
        s(10).asInstanceOf[Option[K]]
      )
    }
  }

  private val tuple12 = Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F, G, H, I, J, K, L](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L]
  ): Stitch[(A, B, C, D, E, F, G, H, I, J, K, L)] =
    joinMap(a, b, c, d, e, f, g, h, i, j, k, l)(tuple12)
      .asInstanceOf[Stitch[(A, B, C, D, E, F, G, H, I, J, K, L)]]

  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L]
  )(
    z: (A, B, C, D, E, F, G, H, I, J, K, L) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](12)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    buf += k
    buf += l
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F],
        ss(6).asInstanceOf[G],
        ss(7).asInstanceOf[H],
        ss(8).asInstanceOf[I],
        ss(9).asInstanceOf[J],
        ss(10).asInstanceOf[K],
        ss(11).asInstanceOf[L]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, G, H, I, J, K, L, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H],
      Option[I], Option[J], Option[K], Option[L]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](12)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    buf += k
    buf += l
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]],
        s(6).asInstanceOf[Option[G]],
        s(7).asInstanceOf[Option[H]],
        s(8).asInstanceOf[Option[I]],
        s(9).asInstanceOf[Option[J]],
        s(10).asInstanceOf[Option[K]],
        s(11).asInstanceOf[Option[L]]
      )
    }
  }

  private val tuple13 = Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F, G, H, I, J, K, L, M](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L],
    m: Stitch[M]
  ): Stitch[(A, B, C, D, E, F, G, H, I, J, K, L, M)] =
    joinMap(a, b, c, d, e, f, g, h, i, j, k, l, m)(tuple13)
      .asInstanceOf[Stitch[(A, B, C, D, E, F, G, H, I, J, K, L, M)]]

  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L],
    m: Stitch[M]
  )(
    z: (A, B, C, D, E, F, G, H, I, J, K, L, M) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](13)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    buf += k
    buf += l
    buf += m
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F],
        ss(6).asInstanceOf[G],
        ss(7).asInstanceOf[H],
        ss(8).asInstanceOf[I],
        ss(9).asInstanceOf[J],
        ss(10).asInstanceOf[K],
        ss(11).asInstanceOf[L],
        ss(12).asInstanceOf[M]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L],
    m: Stitch[M]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H],
      Option[I], Option[J], Option[K], Option[L], Option[M]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](13)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    buf += k
    buf += l
    buf += m
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]],
        s(6).asInstanceOf[Option[G]],
        s(7).asInstanceOf[Option[H]],
        s(8).asInstanceOf[Option[I]],
        s(9).asInstanceOf[Option[J]],
        s(10).asInstanceOf[Option[K]],
        s(11).asInstanceOf[Option[L]],
        s(12).asInstanceOf[Option[M]]
      )
    }
  }

  private val tuple14 =
    Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] _

  def join[A, B, C, D, E, F, G, H, I, J, K, L, M, N](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L],
    m: Stitch[M],
    n: Stitch[N]
  ): Stitch[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] =
    joinMap(a, b, c, d, e, f, g, h, i, j, k, l, m, n)(tuple14)
      .asInstanceOf[Stitch[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)]]

  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, N, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L],
    m: Stitch[M],
    n: Stitch[N]
  )(
    z: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => Z
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](14)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    buf += k
    buf += l
    buf += m
    buf += n
    collectNoCopy(buf).map { ss =>
      z(
        ss(0).asInstanceOf[A],
        ss(1).asInstanceOf[B],
        ss(2).asInstanceOf[C],
        ss(3).asInstanceOf[D],
        ss(4).asInstanceOf[E],
        ss(5).asInstanceOf[F],
        ss(6).asInstanceOf[G],
        ss(7).asInstanceOf[H],
        ss(8).asInstanceOf[I],
        ss(9).asInstanceOf[J],
        ss(10).asInstanceOf[K],
        ss(11).asInstanceOf[L],
        ss(12).asInstanceOf[M],
        ss(13).asInstanceOf[N]
      )
    }
  }

  def partialJoinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, N, Z](
    a: Stitch[A],
    b: Stitch[B],
    c: Stitch[C],
    d: Stitch[D],
    e: Stitch[E],
    f: Stitch[F],
    g: Stitch[G],
    h: Stitch[H],
    i: Stitch[I],
    j: Stitch[J],
    k: Stitch[K],
    l: Stitch[L],
    m: Stitch[M],
    n: Stitch[N]
  )(
    z: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F], Option[G], Option[H],
      Option[I], Option[J], Option[K], Option[L], Option[M], Option[N]) => Option[Z]
  ): Stitch[Z] = {
    val buf = new ArrayBuffer[Stitch[Any]](14)
    buf += a
    buf += b
    buf += c
    buf += d
    buf += e
    buf += f
    buf += g
    buf += h
    buf += i
    buf += j
    buf += k
    buf += l
    buf += m
    buf += n
    PartialCollect(buf) { s =>
      z(
        s(0).asInstanceOf[Option[A]],
        s(1).asInstanceOf[Option[B]],
        s(2).asInstanceOf[Option[C]],
        s(3).asInstanceOf[Option[D]],
        s(4).asInstanceOf[Option[E]],
        s(5).asInstanceOf[Option[F]],
        s(6).asInstanceOf[Option[G]],
        s(7).asInstanceOf[Option[H]],
        s(8).asInstanceOf[Option[I]],
        s(9).asInstanceOf[Option[J]],
        s(10).asInstanceOf[Option[K]],
        s(11).asInstanceOf[Option[L]],
        s(12).asInstanceOf[Option[M]],
        s(13).asInstanceOf[Option[N]]
      )
    }
  }

  /** Returns a Stitch which succeeds with `Seq(v0, v1, ...)` when each
   * Stitch in the argument succeeds with `v0`, `v1`, `...`; or fails
   * with `e` if any Stitch in the argument fails with `e`.
   */
  def collect[T](ss: Seq[Stitch[T]]): Stitch[Seq[T]] =
    if (ss.isEmpty) Nil
    else {
      // Copy seq to avoid external sharing
      val array = new ArrayBuffer[Stitch[T]](ss.length) ++= ss
      Collect(TransformSeq(array, null, Arrow.identity[T]))
    }

  /** Returns a Stitch which succeeds with `Map(k0 -> v0, k1 -> v1, ...)` when each
   * Stitch in the argument succeeds with `v0`, `v1`, `...`; or fails
   * with `e` if any Stitch in the argument fails with `e`.
   */
  def collect[A, B](ss: Map[A, Stitch[B]]): Stitch[Map[A, B]] =
    if (ss.isEmpty) emptyMap
    else {
      val (keys, values) = ss.toSeq.unzip
      Stitch.collect(values).map { seq => keys.zip(seq)(scala.collection.breakOut): Map[A, B] }
    }

  private[stitch] def collectNoCopy[T](buf: ArrayBuffer[Stitch[T]]): Stitch[Seq[T]] =
    if (buf.isEmpty) Nil
    else Collect(TransformSeq(buf, null, Arrow.identity[T]))

  /** Returns a Stitch which succeeds with `List(v0, v1, ...)` when each
   * Stitch in the argument succeds with `v0`, `v1`, `...`; or fails
   * with `e` if any Stitch in the argument fails with `e`.
   */
  def collect[T](ss: java.util.List[Stitch[T]]): Stitch[java.util.List[T]] =
    collect(ss.asScala) map { _.asJava }

  /** Returns a Stitch which succeeds with `Map(k0 -> v0, k1 -> v1, ...)` when each
   * Stitch in the argument succeeds with `v0`, `v1`, `...`; or fails
   * with `e` if any Stitch in the argument fails with `e`.
   */
  def collect[A, B](ss: java.util.Map[A, Stitch[B]]): Stitch[java.util.Map[A, B]] =
    collect(ss.asScala.toMap) map { _.asJava }

  /** Returns a Stitch which succeeds with `Some(v)` when the argument
   * is `Some(s)` and `s` succeeds with `v`; or fails with `e` when
   * `s` fails with `e`; or succeeds with `None` when the argument is
   * `None`.
   */
  def collect[T](so: Option[Stitch[T]]): Stitch[Option[T]] =
    so match {
      case scala.None => None
      case Some(s) => s.map(Some(_))
    }

  /** Returns a Stitch which succeeds with `Seq(Try(v0), Try(v1), ...)` with failures for each
   * stitch being encapsulated within their respective `Try`s
   */
  def collectToTry[T](ss: Seq[Stitch[T]]): Stitch[Seq[Try[T]]] =
    if (ss.isEmpty) Nil
    else {
      // Copy seq to avoid external sharing
      val array = new ArrayBuffer[Stitch[Try[T]]](ss.length)
      ss.foreach(array += _.liftToTry)
      Collect(TransformSeq(array, null, Arrow.identity[Try[T]]))
    }

  /** Returns a Stitch which succeeds with `List(Try(v0), Try(v1), ...)` with failures for each
   * stitch being encapsulated within their respective `Try`s
   */
  def collectToTry[T](ss: java.util.List[Stitch[T]]): Stitch[java.util.List[Try[T]]] =
    collectToTry(ss.asScala).map(_.asJava)

  /** Maps `f` over `ts` then `collect`s the result.
   */
  def traverse[T, U](ts: Seq[T])(f: T => Stitch[U]): Stitch[Seq[U]] =
    if (ts.isEmpty) Nil
    else {
      val iterator = ts.iterator
      val array = new ArrayBuffer[Stitch[U]](ts.length)
      while (iterator.hasNext) array += app(f, iterator.next())
      Collect(TransformSeq(array, null, Arrow.identity[U]))
    }

  /** Maps `f` over `ts` then `collect`s the result.
   */
  def traverse[T, U](ts: java.util.List[T])(f: T => Stitch[U]): Stitch[java.util.List[U]] =
    traverse(ts.asScala)(f).map(_.asJava)

  /** Maps `f` over `to` then `collect`s the result.
   */
  def traverse[T, U](to: Option[T])(f: T => Stitch[U]): Stitch[Option[U]] =
    collect(to.map(app(f, _)))

  /** Makes a Stitch with a constant result (value or exception)
   */
  def const[T](result: Try[T]): Stitch[T] = Const(result)

  /** Stack-safe recursion for Stitch evaluation.
   */
  def recursive[T](s: => Stitch[T]): Stitch[T] = Recursive(() => s)

  /** Makes a Stitch with a constant value
   */
  def value[T](t: T): Stitch[T] = const(Return(t))

  /** Makes a Stitch with a constant exceptional result
   */
  def exception[T](e: Throwable): Stitch[T] = const(Throw(e))

  /**
   * Converts a "normal" `Stitch` into one that will be executed asynchronously for its
   * side-effects rather than its result.  When the returned `Stitch` is evaluated, the embedded
   * computation will be started, but the returned `Stitch` will immediately be satisfied
   * with a return result of `Unit`.
   *
   * @note Result of [[async]] must be included into a computation passed to [[Stitch.run]] for the
   *       side-effect to apply.
   *
   * @note Asynchronous computations may continue after `Stitch.run`
   *       returns, but interrupting the returned Future interrupts
   *       asynchronous computations within that call.
   */
  def async[T](s: Stitch[T]): Stitch[Unit] = AddNewRoot(s)

  /** Makes a Stitch which represents a call to an underlying service;
   * typically this is called only from service adaptors, which wrap
   * it in a service-specific way.
   *
   * @tparam C type of the call argument (e.g. Long for a user ID)
   * @tparam T return type of the call (e.g. User for a user lookup)
   * @param call the call argument (e.g. the user ID)
   * @param group used for batching calls together and to run a batch, see [[com.twitter.stitch.Group]]
   * @return return value of the call (or failure)
   */
  def call[C, T](call: C, group: Group[C, T]): Stitch[T] = Call(call, group)

  /** Makes a Stitch which represents a call to an underlying service;
   * typically this is called only from service adaptors, which wrap
   * it in a service-specific way.
   *
   * @tparam C type of the call argument (e.g. Long for a user ID)
   * @tparam T return type of the call (e.g. User for a user lookup)
   * @param call the call argument (e.g. the user ID)
   * @param group used for batching calls together and to run a batch, see [[com.twitter.stitch.Group]]
   * @return return value of the call (or failure)
   */
  def callSeqTry[C, T](call: Seq[Try[C]], group: ArrowGroup[C, T]): Stitch[C => Try[T]] =
    CallSeqTry(call, group)

  /**
   * Call which calls a Future; useful for services without batch APIs.
   */
  def callFuture[T](f: => Future[T]): Stitch[T] = CallFuture(() => f)

  /** Wraps tracing around a Stitch: each time the expression is
   * simplified, its string representation is passed to the given
   * function (`Console.err.println` by default).
   */
  def trace[T](s: Stitch[T], f: String => Unit = Console.err.println): Stitch[T] = Trace(s, f)

  /**
   * Returns a Stitch which succeeds with `(t, d)`, where `t` is a
   * [[com.twitter.util.Try Try]] of the completion of the underlying
   * Stitch, and `d` is a [[com.twitter.util.Duration Duration]]
   * measuring the time taken to execute `s`
   */
  def time[T](s: => Stitch[T]): Stitch[(Try[T], Duration)] =
    Time(Stitch.Done.flatMap { _ => s })

  /**
   * Returns a Stitch which succeeds after the given `d` Duration
   * has elapsed. Behaves similarly to [[com.twitter.util.Future Future]].sleep,
   * only doesn't break batching.
   */
  def sleep(d: util.Duration)(implicit timer: Timer): Stitch[Unit] =
    if (d <= util.Duration.Zero)
      Stitch.Done
    else
      call((), SleepGroup(d, timer))

  /**
   * let-scope a [[Local]] around the contained [[Stitch]] `s`
   *
   * This is equivalent to `local.let(local)(a)` but correctly applies the `local` to the `s`'s execution.
   *
   * @note [[let]] applies the [[Local]] __only non-batched operations__,
   *       [[call]] and [[Group]]s do not have access to [[Local]]s set with [[let]].
   *       To access a [[Local]] value within a [[call]], or [[Group]], access the value
   *       before making your [[call]] and add it to your input.
   *       For instance,`s.map(v => (v, local())).call(...)`
   *
   * @param local the [[Local]] that should be let-scoped around `a`
   * @param value the value of the `local` within the execution of `s`
   * @param s the underlying [[Stitch]] which will have the `local` set during it's evaluation
   * @tparam L the type of the [[Local]] and it's value
   */
  def let[T, L](local: Local[L])(value: L)(s: => Stitch[T]): Stitch[T] =
    let(LocalLetter(local))(value)(s)

  /**
   * clear a [[Local]] around the contained [[Stitch]] `s`
   *
   * This is equivalent to `LetClearer.letClear(a)` but correctly clears the `local` for `s`'s execution.
   *
   * @param local the [[Local]] that should be let-scoped around `a`
   * @param s the underlying [[Stitch]] which will have the `local` set during it's evaluation
   */
  def letClear[T](local: Local[_])(s: => Stitch[T]): Stitch[T] =
    letClear(LocalLetClearer(local))(s)

  /**
   * [[Letter.let]] around the contained [[Stitch]] `s`
   *
   * This is equivalent to `Letter.let(local)(a)` but correctly applies the [[Letter.let]] to the `s`'s execution.
   *
   * This is used for let-scoping [[Local]]s which are private to a class and can only be accessed with their own
   * let method instead of giving access to the underlying [[Local]]
   *
   * @note [[let]] applies to __only non-batched operations__,
   *       [[call]] and [[Group]]s do not have access to let-scoped values.
   *       To access a let-scoped value value within a [[call]], or [[Group]], access the value
   *       before making your [[call]] and add it to your input.
   *       For instance,`s.map(v => (v, local())).call(...)`
   *
   * @param letter [[Letter]] that let-scopes around a block of code
   * @param value the value of the let-scoped [[Local]] within the execution of `s`
   * @param s the underlying [[Stitch]] which will have the `local` set during it's evaluation
   * @tparam L the type of the [[Local]] and it's value
   */
  def let[T, L](letter: Letter[L])(value: L)(s: => Stitch[T]): Stitch[T] =
    letter.let(value)(s) match { // let scope around evaluating `s` in case it's a `Const`
      case CompletedStitch(s) => s
      case s => Let(letter, value, s)
    }

  /**
   * [[LetClearer.letClear]] around the contained [[Stitch]] `s`
   *
   * This is equivalent to `LetClearer.letClear(a)` but correctly applies [[LetClearer.letClear]] for `s`'s execution.
   *
   * This is used for letClearing [[Local]]s which are private to a class and can only be accessed with their own
   * let method instead of giving access to the underlying [[Local]]
   *
   * @param letClearer [[LetClearer]] that clears around a block of code
   * @param s the underlying [[Stitch]] which will have the `local` set during it's evaluation
   */
  def letClear[T](letClearer: LetClearer)(s: => Stitch[T]): Stitch[T] =
    letClearer.letClear(s) match { // let scope around evaluating `s` in case it's a `Const`
      case CompletedStitch(s) => s
      case s => LetClear(letClearer, s)
    }

  /** Makes a Stitch with by-reference rather than by-name semantics; it
   * succeeds or fails the same as the underlying query but the
   * underlying query is executed at most once.
   *
   * In the first example below, the RPC call given in `s` is executed
   * twice; in the second it is executed only once.
   *
   * @example {{{
   *   val s = ... // some RPC call
   *   Stitch.run(s.flatMap { _ => s })
   * }}}
   * @example {{{
   *   val s = Stitch.ref(...) // some RPC call
   *   Stitch.run(s.flatMap { _ => s })
   * }}}
   *
   * NB: queries containing [[Stitch.ref]] are not safe to re-use
   * across calls to [[Stitch.run]] since results are stored by
   * mutating the query. See also the warning on [[Stitch.run]];
   * re-using a query across calls to [[Stitch.run]] is undefined in
   * any case.
   */
  def ref[T](s: Stitch[T]): Stitch[T] = Ref(s)

  /** Makes a thread-safe Stitch with by-reference rather than by-name semantics;
   * this is the same as using [[ref]] except that simplification is
   * synchronized.
   *
   * This makes it thread-safe to re-use across calls to [[Stitch.run]] calls
   */
  def synchronizedRef[T](s: Stitch[T]): Stitch[T] = new SynchronizedRef(s)

  /** helper for determining if a Stitch is completed to avoid doing extra work when we know its already done */
  private[stitch] object CompletedStitch {
    def unapply[T](s: Stitch[T]): Option[Const[T]] =
      s match {
        case const: Const[T] => Some(const)
        case Ref(const: Const[T]) => Some(const)
        case SynchronizedRef(const: Const[T]) => Some(const)
        case _ => scala.None
      }
  }

  /** Executes a Stitch computation, returning a Future which completes
   * when the Stitch completes.
   *
   * The behavior of calling [[run]] more than once on the same
   * Stitch is undefined.
   */
  def run[T](s: Stitch[T]): Future[T] = s match {
    case CompletedStitch(Const(t)) => Future.const(t)
    case _ => new Run(s).run()
  }

  /**
   * A version of [[Stitch.run]] that employs a local cache for RPCs. If you find yourself needing
   * the same key(s) more than once across [[Stitch.flatMap]]s, enabling the cache may result in
   * fewer RPCs. The local cache is not recommended for use with [[Stitch.callFuture]].
   */
  def runCached[T](s: Stitch[T]): Future[T] = s match {
    case CompletedStitch(Const(t)) => Future.const(t)
    case _ => new CachedRun(s).run()
  }

  /**
   * apply a synchronous function to return a Stitch instance,
   * wrapping the invocation with Stitch specific error handling
   *
   * Primarily used internally in [[Arrow]] when applying a function
   * that returns a [[Stitch]]
   */
  private[stitch] def app[T, U](f: T => Stitch[U], t: T): Stitch[U] =
    try f(t)
    catch {
      case e: NonLocalReturnControl[_] => exception(new StitchNonLocalReturnControl(e))
      case NonFatal(e) => exception(e)
    }

  /**
   * Returns a Stitch which completes when the argument Future
   * completes. (Note that this is different from callFuture, which
   * defers execution of its argument.)
   */
  private[stitch] def future[T](f: Future[T], label: Any = null): Stitch[T] = SFuture(f, label)

  private[stitch] def transformSeq[A, B](
    ss: ArrayBuffer[Stitch[A]],
    ls: ArrayBuffer[Arrow.Locals],
    a: Arrow[A, B]
  ): Stitch[ArrayBuffer[Try[B]]] =
    if (ss.isEmpty) emptyBufferStitch.asInstanceOf[Stitch[ArrayBuffer[Try[B]]]]
    else TransformSeq(ss, ls, a)

  private val emptyBuffer = new ArrayBuffer[Any](0)
  private val emptyBufferStitch = Stitch.value(emptyBuffer)

  private[stitch] case class Const[T](t: Try[T]) extends Stitch[T] {
    override def simplify(pending: Pending): Const[T] = this
  }

  private[stitch] case class Recursive[T](s: () => Stitch[T]) extends Stitch[T] {

    // null => not simplified yet
    // Evaluating => a run loop is evaluating the `s` function
    // Stitch => the recursion function (`s`) is already evaluated
    private[this] val state = new AtomicReference[Any]()

    // Needs to be synchronized so only one run loop
    // is able to evaluate the delayed recursion
    def eval(): Unit = {
      if (state.compareAndSet(null, Recursive.Evaluating)) {
        state.set(s())
      }
    }

    // Doesn't need to be synchronized since the
    // sync ref that might wrap the recursion already
    // guarantees that only one run loop can simplify
    // this Stitch at a time
    override def simplify(pending: Pending): Stitch[T] =
      state.get match {
        case null =>
          // let `pending` handle the recursion
          pending.recursive(this)
        case Recursive.Evaluating =>
          // another run loop is already evaluating
          // this recursion, let it finish
          this
        case s: Stitch[T] =>
          // recursion is already evaluated,
          // proceed with regular simplification
          s.simplify(pending)
      }
  }

  private[stitch] object Recursive {
    private final case object Evaluating
  }

  private[stitch] case class SFuture[T](f: Future[T], label: Any) extends Stitch[T] {
    override def simplify(pending: Pending): Stitch[T] =
      f.poll match {
        case scala.None => this
        case Some(r) => const(r)
      }
  }

  /**
   * [[Transform]] represents a chain of transformations on a query so that
   * the underlying query is accessible without traversing the
   * chain. This avoids needless traversal and copying during
   * simplification.
   */
  private[stitch] case class Transform[T, U](
    var input: Stitch[T],
    locals: Arrow.Locals,
    var a: Arrow[T, U])
      extends Stitch[U] {
    private var result: Stitch[U] = null

    override def simplify(pending: Pending): Stitch[U] = {
      if (result != null) return result

      input = input.simplify(pending)
      input match {
        case Const(t) =>
          result = a.run(t, locals).simplify(pending)
          result
        case Transform(s, l, b) if l == locals =>
          this.input = s.asInstanceOf[Stitch[T]]
          this.a = b
            .andThenNoReassoc(a)
            .asInstanceOf[Arrow[T, U]] // suppress ide warning
          this
        case _ =>
          this
      }
    }
  }

  /**
   * [[TransformSeq]] represents a chain of transformations on
   * an ArrayBuffer of queries so that the underlying queries are
   * accessible without traversing the chain. This avoids
   * needless traversal and copying during simplification.
   */
  private case class TransformSeq[T, U](
    var ss: ArrayBuffer[Stitch[T]],
    ls: ArrayBuffer[Arrow.Locals],
    a: Arrow[T, U])
      extends Stitch[ArrayBuffer[Try[U]]] {

    private[this] var forwarded: Stitch[ArrayBuffer[Try[U]]] = null

    private[this] def forward(s: Stitch[ArrayBuffer[Try[U]]]): Stitch[ArrayBuffer[Try[U]]] = {
      ss = null
      forwarded = s
      s
    }

    override def simplify(pending: Pending): Stitch[ArrayBuffer[Try[U]]] = {
      if (forwarded ne null) return forwarded

      var satisfied = true

      var i = 0
      while (i < ss.length) {
        ss(i) = ss(i).simplify(pending)
        ss(i) match {
          case Const(_) =>
          case _ => satisfied = false
        }
        i += 1
      }

      if (satisfied) {
        val result = ss.asInstanceOf[ArrayBuffer[Try[T]]]
        var i = 0
        while (i < ss.length) {
          ss(i) match {
            case Const(t) => result(i) = t
            case _ => scala.sys.error("stitch bug")
          }
          i += 1
        }
        forward(a.run(result, ls).simplify(pending))
      } else this
    }
  }

  private[stitch] case class Collect[T](var self: Stitch[Seq[Try[T]]]) extends Stitch[Seq[T]] {

    private[this] var forwarded: Stitch[Seq[T]] = null

    private[this] def forward(s: Stitch[Seq[T]]): Stitch[Seq[T]] = {
      self = null
      forwarded = s
      s
    }

    override def simplify(pending: Pending): Stitch[Seq[T]] = {
      if (forwarded ne null) return forwarded

      self match {
        case TransformSeq(ss, _, Arrow.Identity()) =>
          // when there is no continuation we can return an exception
          // as soon as we see it.
          // TODO(jdonham) we could also do this when there is a
          // continuation but it doesn't catch exceptions

          var satisfied = true

          var i = 0
          while (i < ss.length) {
            ss(i) = ss(i).simplify(pending)
            ss(i) match {
              case t @ Const(Throw(_)) =>
                return forward(t.asInstanceOf[Stitch[Seq[T]]])
              case Const(_) =>
              case _ => satisfied = false
            }
            i += 1
          }

          if (satisfied) {
            val result = ss.asInstanceOf[ArrayBuffer[T]]
            var i = 0
            while (i < ss.length) {
              ss(i) match {
                case Const(Return(t)) => result(i) = t.asInstanceOf[T]
                case _ => scala.sys.error("stitch bug")
              }
              i += 1
            }
            forward(Stitch.value(result))
          } else this

        case _ =>
          self.simplify(pending) match {
            case s @ Const(Throw(_)) =>
              forward(s.asInstanceOf[Stitch[Seq[T]]])

            case Const(Return(ts)) =>
              var i = 0
              while (i < ts.length) {
                ts(i) match {
                  case t @ Throw(_) =>
                    return forward(Const(t).asInstanceOf[Stitch[Seq[T]]])
                  case _ =>
                }
                i += 1
              }

              val result = new ArrayBuffer[T](ts.length)
              i = 0
              while (i < ts.length) {
                ts(i) match {
                  case Return(t) => result += t
                  case _ => scala.sys.error("stitch bug")
                }
                i += 1
              }
              forward(Stitch.value(result))

            case self =>
              this.self = self
              this
          }
      }
    }
  }

  private[stitch] case class PartialCollect[T, R](
    var ss: ArrayBuffer[Stitch[T]]
  )(
    k: (Seq[Option[T]]) => Option[R])
      extends Stitch[R] {
    private[this] var forwarded: Stitch[R] = null

    private[this] def forward(s: Stitch[R]): Stitch[R] = {
      forwarded = s
      s
    }

    private[this] val buf = ArrayBuffer.fill[Option[T]](ss.length)(scala.None)

    override def simplify(pending: Pending): Stitch[R] = {
      if (forwarded ne null) return forwarded

      var satisfied = true
      var i = 0
      while (i < ss.length) {
        ss(i) = ss(i).simplify(pending)
        ss(i) match {
          // this won't short-circuit immediately if any of the remaining stitches are Const(Throw(_))
          // but it will eventually return when it gets to the failed stitch and if we can't move forward
          // based on the current stitches.
          case t @ Const(Throw(_)) =>
            return forward(t.asInstanceOf[Stitch[R]])
          case Const(Return(v)) => buf(i) = Some(v)
          case _ => satisfied = false
        }

        // invoke k and return if we can move forward or run into exceptions
        try {
          k(buf) match {
            case Some(r) => return forward(Stitch.value(r))
            case scala.None =>
          }
        } catch {
          case NonFatal(e) => return forward(Stitch.exception[R](e))
        }
        i += 1
      }
      if (satisfied) {
        // if all stitches are satisfied and the function returns None, we return StitchInvalidState
        forward(Stitch.exception[R](new StitchInvalidState))
      } else {
        this
      }
    }
  }

  /**
   * A Call represents an external computation (e.g. an RPC call). It
   * has an associated group to control batching (see
   * [[com.twitter.stitch.Group]]), and call arguments which represent
   * the arguments not captured in the group (e.g. the key).
   */
  private case class Call[C, T](call: C, group: Group[C, T]) extends Stitch[T] {
    override def simplify(pending: Pending): Stitch[T] =
      pending.add(call, group)
  }

  private case class CallSeqTry[C, T](cs: Seq[Try[C]], group: ArrowGroup[C, T])
      extends Stitch[C => Try[T]] {
    override def simplify(pending: Pending): Stitch[C => util.Try[T]] =
      pending.addSeqTry(cs, group)
  }

  /** A Stitch that captures [[Local]]s around the evaluation of the [[Future]] [[f]] */
  private[stitch] case class CallFuture[T](f: () => Future[T]) extends Stitch[T] {

    override def simplify(pending: Pending): Stitch[T] = {

      /** Save the current locals in the Group instance */
      pending.add((), new CallFutureGroup(this, f, Local.save()))
    }
  }

  private[stitch] class CallFutureGroup[T](
    val parentCallFutureInstance: CallFuture[T],
    f: () => Future[T],
    context: Local.Context)
      extends Group[Unit, T] {

    def runner(): FutureRunner[Unit, T] = new FutureRunner[Unit, T] {
      private val p = Promise[T]()

      def add(c: Unit): Stitch[T] = Stitch.future(p)

      def run(): Promise[T] = {
        p.become(try { Local.let(context) { f() } }
        catch {
          case e: NonLocalReturnControl[_] =>
            Future.exception(new StitchNonLocalReturnControl(e))
          case e: InterruptedException => Future.exception(e)
          case NonFatal(e) => Future.exception(e)
        })
        p
      }
    }
  }

  /** wrap an arbitrary computation with a let-scoped [[Local]] */
  trait Letter[L] {

    /**
     * Let-scope's for the evaluation of by-name `s`
     * @note must be side-effect free
     */
    def let[S](value: L)(s: => S): S
  }

  /** wrap an arbitrary computation clearing a [[Local]] */
  trait LetClearer {

    /**
     * Clear a [[Local]] for the evaluation of by-name `s`
     * @note must be side-effect free
     */
    def letClear[S](s: => S): S
  }

  case class LocalLetter[L](local: Local[L]) extends Letter[L] {
    override def let[S](value: L)(s: => S): S = local.let(value)(s)
  }

  case class LocalLetClearer[L](local: Local[L]) extends LetClearer {
    override def letClear[S](s: => S): S = local.letClear(s)
  }

  private[this] case class Let[T, L](letter: Letter[L], value: L, var self: Stitch[T])
      extends Stitch[T] {

    override def simplify(pending: Pending): Stitch[T] = {
      self match {
        case const: Const[T] => const
        case _ =>
          // let scope around the simplify call to the underlying Stitch and
          // as a result any Stitches that Stitch contains
          // i.e. this includes any `Stitch.Transform`, `Arrow.run`, etc
          self = letter.let(value)(self.simplify(pending))
          self match {
            case const: Const[T] => const
            case _ => this
          }
      }
    }
  }

  private[this] case class LetClear[T](letter: LetClearer, var self: Stitch[T]) extends Stitch[T] {

    override def simplify(pending: Pending): Stitch[T] = {
      self match {
        case const: Const[T] => const
        case _ =>
          // letClear scope around the simplify call to the underlying Stitch and
          // as a result any Stitches that Stitch contains
          // i.e. this includes any `Stitch.Transform`, `Arrow.run`, etc
          self = letter.letClear(self.simplify(pending))
          self match {
            case const: Const[T] => const
            case _ => this
          }
      }
    }
  }

  private case class Trace[T](var self: Stitch[T], f: String => Unit) extends Stitch[T] {
    override def simplify(pending: Pending): Stitch[T] = {
      val s = self.simplify(pending)
      if (s ne self) f(s.toString)
      self = s
      self match {
        case s @ Const(_) => s
        case _ => this
      }
    }
  }

  private case class Time[T](
    var self: Stitch[T],
    var startTime: Option[util.Stopwatch.Elapsed] = scala.None)
      extends Stitch[(Try[T], Duration)] {
    override def simplify(pending: Pending): Stitch[(util.Try[T], util.Duration)] = {
      if (startTime.isEmpty) {
        startTime = Some(util.Stopwatch.start())
      }

      self = self.simplify(pending)
      self match {
        case Const(t) =>
          Const(Return((t, startTime.get.apply())))
        case _ => this
      }
    }
  }

  private case class Within[T](
    var self: Stitch[T],
    timeout: util.Duration,
    timer: Timer
  )(
    exc: => Exception)
      extends Stitch[T] {
    private[this] var sleepStitch: Stitch[Unit] = _

    override def simplify(pending: Pending): Stitch[T] = {
      if (sleepStitch == null)
        sleepStitch = pending.add((), SleepGroup(timeout, timer))

      assert(sleepStitch != null)
      sleepStitch = sleepStitch.simplify(pending)

      val expired =
        sleepStitch match {
          case Const(_) => true
          case _ => false
        }

      self match {
        case Const(_) => self

        case _ if expired => Stitch.exception(exc)

        case _ =>
          self = self.simplify(pending)
          self match {
            case Const(_) => self
            case _ => this
          }
      }
    }
  }

  private[stitch] case class Ref[T](var current: Stitch[T]) extends Stitch[T] {
    override def simplify(pending: Pending): Stitch[T] = {
      current = current.simplify(pending)
      current match {
        case Const(_) => current
        case _ => this
      }
    }
  }

  /** Stitch Ref that synchronizes simplification so it is safe to run concurrently,
   * even across multiple Stitch.Runs
   */
  private[stitch] class SynchronizedRef[T](initial: Stitch[T])
      extends Stitch[T]
      // reduce allocations by having each `SynchronizedRef` also be a `Group` and `FutureRunner` instead of containing those objects
      with Group[Unit, Unit]
      with FutureRunner[Unit, Unit] {

    override def toString: String = s"SynchronizedRef($currentSync)"

    override def equals(obj: Any): Boolean = obj match {
      case sr: SynchronizedRef[T] => sr.currentSync == currentSync
      case _ => false
    }

    @volatile private[stitch] var currentSync = initial

    private[this] val nwaiters = new AtomicInteger(0)
    private[this] val p = new Promise[Unit]()

    // used to make a `Pending` instance call `simplify` again when this Stitch is ready
    override def runner(): Runner[Unit, Unit] = this
    override def run(): Future[Unit] = p
    override def add(call: Unit): Stitch[Unit] = Stitch.future(p)

    override def simplify(pending: Pending): Stitch[T] = {
      // short circuit if we're already done
      if (currentSync.isInstanceOf[Const[T]]) {
        currentSync
      } else {
        // synchronize using an atomic counter since it should only be done by a single pending instance at a time
        // any thread can add to the counter but only the first one can decrement it, this avoids locks.
        if (nwaiters.getAndIncrement() == 0) {
          // short circuit if we're already done
          if (currentSync.isInstanceOf[Const[T]]) {
            nwaiters.set(0)
            return currentSync
          } else {
            do {
              currentSync = currentSync.simplify(pending)
              currentSync match {
                case _: Const[_] => // current is done, so clean everything up and return
                  p.setDone() // complete `p` so concurrent callers will `simplify` again
                  nwaiters.set(0) // reset `nwaiters` to 0 since theres no more work to be done
                  return currentSync // return the `currentSync`
                case _ => // current isn't done
              }
            } while (nwaiters.decrementAndGet() > 0)
            // simplified as much as possible but still not done
          }
        }
        // if we just simplified down to a `Const` we would have returned in the loop above
        // so instead we add a callback which calls `simplify` when `p` is completed
        pending.add((), this)
        // return `this` since it needs to be simplified again later
        this
      }
    }
  }

  private[stitch] object SynchronizedRef {
    def unapply[T](s: Stitch[T]): Option[Stitch[T]] = {
      s match {
        case sr: SynchronizedRef[T] => Some(sr.currentSync)
        case _ => scala.None
      }
    }
  }

  private[stitch] case class Incomplete[T]() extends Stitch[T] {
    override def simplify(pending: Pending): Incomplete[T] = this
  }

  private case class AddNewRoot(s: Stitch[_]) extends Stitch[Unit] {
    override def simplify(pending: Pending): Stitch[Unit] = {
      pending.addNewRoot(s)
      Stitch.Unit
    }
  }

  private object SleepGroup {
    private[this] val tunit: Try[Unit] = Return.Unit
    private[this] val innerCallback = { (_: Unit) => tunit }
    private val callback = { (_: Unit) => innerCallback }
  }

  private[stitch] case class SleepGroup(d: util.Duration, timer: Timer)
      extends MapGroup[Unit, Unit] {
    private[this] implicit val _t: util.Timer = timer
    protected def run(keys: Seq[Unit]): Future[(Unit) => Try[Unit]] =
      Future.sleep(d).map(SleepGroup.callback)
  }
}
