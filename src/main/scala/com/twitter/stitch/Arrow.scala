package com.twitter.stitch

import com.twitter.stitch.Stitch.LetClearer
import com.twitter.stitch.Stitch.Letter
import com.twitter.stitch.Stitch.LocalLetClearer
import com.twitter.stitch.Stitch.LocalLetter
import com.twitter.util.{Local => TwitterLocal}
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Timer
import com.twitter.util.Try

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.runtime.NonLocalReturnControl
import scala.util.control.NoStackTrace
import scala.util.control.NonFatal

/**
 * An `Arrow[T, U]` is semantically a `Try[T] => Stitch[U]` where the
 * internal structure of the function is exposed to the Stitch
 * evaluator; this permits arrows to be applied with less overhead,
 * particularly when applied uniformly across a sequence of values.
 *
 * Note on `flatMap`, `rescue`, `transform`, and `applyArrowToSeq`: these methods imply a
 * "join point" in the execution of an arrow; that is, in an arrow
 * `a.flatMap(f).andThen(b)`, all Stitches returned by `f` must
 * complete before `b` begins execution. This permits control over
 * batching: if `b` represents a service call, all calls are batched
 * together even if some applications of `f` take longer than others.
 *
 * Note on composition and error handling:
 *
 * Both successful values and exceptions from the arrow to the left of andThen are passed to the
 * arrow to the right. This can be counterintuitive, for example:
 *
 * {{{
 *   val handle = Arrow.handle[Int, Int] { case t => 0 }
 *
 *   // This returns `0`, not `e`, as `handle` handles `e`
 *   Arrow.exception(e).andThen {
 *     Arrow.map { x: Int => x + 1 }.andThen(handle)
 *   }
 * }}}
 *
 * Contrast this with flatMapArrow, which passes only successful values to the arrow on the right,
 * and propagates errors
 *
 * {{{
 *   val handle = Arrow.handle[Int, Int] { case t => 0 }
 *
 *   // propagates e, as the body is only returned
 *   // in the success case. In the failure case, e is returned.
 *   Arrow.exception(e).flatMapArrow {
 *     Arrow.map { x: Int => x + 1 }.andThen(handle)
 *   }
 *}}}
 */
sealed abstract class Arrow[-T, +U] {

  def apply(t: T): Stitch[U] = apply(Return(t))
  def apply(t: Try[T]): Stitch[U] = run(t, Arrow.Locals.empty)

  def apply(s: Stitch[T]): Stitch[U] = apply(s, Arrow.Locals.empty)

  def apply(t: Try[T], l: Arrow.Locals): Stitch[U] = run(t, l)

  def apply(s: Stitch[T], l: Arrow.Locals): Stitch[U] =
    s match {
      case Stitch.Transform(s, ll, a2) if ll == l =>
        // We can combine Transforms by combining the arrows as long as the locals are the same
        // NB: we use andThenNoReassoc here so that reassociating happens
        // when the arrow is consumed (in Arrow.headTail) rather than here
        // where it's created; we expect to use this arrow only once.
        a2.andThenNoReassoc(this).apply(s, ll)
      case _ =>
        Stitch.Transform(s, l, this)
    }

  /**
   * Maps `this` over `ts` then `collect`s the result.
   */
  def traverse(ts: Seq[T], l: Arrow.Locals): Stitch[Seq[U]] =
    if (ts.isEmpty) Stitch.Nil
    else Stitch.Collect(run(TryBuffer.values(ts), ArrayBuffer.fill(ts.length)(l)))

  def traverse(ts: Seq[T]): Stitch[Seq[U]] = traverse(ts, Arrow.Locals.empty)

  /**
   * Maps `this` over `to` then `collect`s the result.
   */
  def traverse(to: Option[T]): Stitch[Option[U]] =
    to match {
      case Some(t) => this.map(Some(_)).apply(t)
      case _ => Stitch.None
    }

  /**
   * `a.andThen(b)` is equivalent to `{ x => b(a(x)) }`
   *
   * Both successful values and errors are passed from `this` to `that`
   * See note on composition and error handling above.
   */
  def andThen[V](that: Arrow[U, V]): Arrow[T, V] = Arrow.andThen(this, that)

  private[stitch] def andThenNoReassoc[V](that: Arrow[U, V]): Arrow[T, V] =
    Arrow.AndThen.noReassoc(this, that)

  /**
   * `a.compose(b)` is equivalent to `{ x => a(b(x)) }`
   */
  def compose[V](that: Arrow[V, T]): Arrow[V, U] = Arrow.andThen(that, this)

  /**
   * `a.map(f)` is equivalent to `{ x => a(x).map(f) }`
   */
  def map[V](f: U => V): Arrow[T, V] = andThen(Arrow.map(f))

  /**
   * `a.flatMap(f)` is equivalent to `{ x => a(x).flatMap(f) }`, but
   * see note above.
   */
  def flatMap[V](f: U => Stitch[V]): Arrow[T, V] = andThen(Arrow.flatMap(f))

  /**
   * `a.flatMapArrow(f)` is equivalent to `{ x => a(x).flatMap(f) }`
   *
   * This is useful because it only passes successful values to `that`.
   * See note on composition and error handling above.
   */
  def flatMapArrow[V](that: Arrow[U, V]): Arrow[T, V] = this.andThen(Arrow.flatMapArrow(that))

  /**
   * `a.foldLeftArrow` is equivalent to `a.andThen(Arrow.foldLeftArrow(_))`
   */
  def foldLeftArrow[V, UU](a: Arrow[(V, UU), V])(implicit ev: U <:< (V, Seq[UU])): Arrow[T, V] =
    this.asInstanceOf[Arrow[T, (V, Seq[UU])]].andThen(Arrow.foldLeftArrow[UU, V](a))

  /**
   * `a.contramap(f)` is equivalent to `{ x => a(x.map(f)) }`
   */
  def contramap[S](f: S => T): Arrow[S, U] = Arrow.map(f).andThen(this)

  /**
   * `a.handle(f)` is equivalent to `{ x => a(x).handle(f) }`
   */
  def handle[V >: U](f: PartialFunction[Throwable, V]): Arrow[T, V] = andThen(Arrow.handle(f))

  /**
   * `a.rescue(f)` is equivalent to `{ x => a(x).rescue(f) }`, but see
   * note above.
   */
  def rescue[V >: U](f: PartialFunction[Throwable, Stitch[V]]): Arrow[T, V] =
    andThen(Arrow.rescue(f))

  /**
   * `a.mapFailure(pf)` is equivalent to `{ x => a(x).rescue(ex => Stitch.exception(pf.applyOrElse(ex, identity)) }`
   */
  def mapFailure(pf: PartialFunction[Throwable, Throwable]): Arrow[T, U] =
    andThen(Arrow.mapFailure(pf))

  /**
   * `a.ensure(f)` is equivalent to `{ x => a(x).ensure(f) }`
   */
  def ensure(f: => Unit): Arrow[T, U] = andThen(Arrow.ensure(f))

  /**
   * `a.respond(f)` is equivalent to `{ x => a(x).respond(f) }`
   */
  def respond(f: Try[U] => Unit): Arrow[T, U] = andThen(Arrow.respond(f))

  /**
   * `a.onSuccess(f)` is equivalent to `{ x => a(x).onSuccess(f) }`
   */
  def onSuccess(f: U => Unit): Arrow[T, U] = andThen(Arrow.onSuccess(f))

  /**
   * `a.onFailure(f)` is equivalent to `{ x => a(x).onFailure(f) }`
   */
  def onFailure(f: Throwable => Unit): Arrow[T, U] = andThen(Arrow.onFailure(f))

  /**
   * `a.transform(f)` is equivalent to `{ x => a(x).transform(f) }`,
   * but see note above.
   */
  def transform[V](f: Try[U] => Stitch[V]): Arrow[T, V] = andThen(Arrow.transform(f))

  /**
   * `a.transformTry(f)` is equivalent to `{ x => a(x).liftToTry.map(f).lowerFromTry }`
   */
  def transformTry[V](f: Try[U] => Try[V]): Arrow[T, V] = andThen(Arrow.transformTry(f))

  /**
   * `a.unit` is equivalent to `{ x => a(x).unit }`
   */
  def unit: Arrow[T, Unit] = andThen(Arrow.unit)

  /**
   * `a.liftToTry` is equivalent to `{ x => a(x).liftToTry }`
   */
  def liftToTry: Arrow[T, Try[U]] = andThen(Arrow.liftToTry)

  /**
   * `a.lowerFromTry` is equivalent to `{ x => a(x).lowerFromTry }`
   */
  def lowerFromTry[V](implicit ev: U <:< Try[V]): Arrow[T, V] = {
    val _ = ev //suppress warning
    andThen(Arrow.lowerFromTry.asInstanceOf[Arrow[U, V]])
  }

  /**
   * `a.liftToOption(f)` is equivalent to `{ x => a(x).liftToOption(f) }`
   */
  def liftToOption(
    f: PartialFunction[Throwable, Boolean] = Stitch.liftToOptionDefaultFn
  ): Arrow[T, Option[U]] =
    andThen(Arrow.liftToOption(f))

  /**
   * `a.liftNotFoundToOption` is equivalent to `{ x => a(x).liftNotFoundToOption }`
   */
  def liftNotFoundToOption: Arrow[T, Option[U]] =
    andThen(Arrow.liftNotFoundToOption)

  /**
   * `a.lowerFromOption(e)` is equivalent to `{ x => a(x).lowerFromOption(e) }`
   */
  def lowerFromOption[V](
    e: Throwable = com.twitter.stitch.NotFound
  )(
    implicit ev: U <:< Option[V]
  ): Arrow[T, V] = {
    val _ = ev // suppress warning
    andThen(Arrow.lowerFromOption(e).asInstanceOf[Arrow[U, V]])
  }

  /**
   * `a.applyArrow` is equivalent to `a.flatMap { case (a: Arrow, t) => a(t) }`
   * but propagates locals into `a`.
   */
  def applyArrow[V, W](implicit ev: U <:< (Arrow[V, W], V)): Arrow[T, W] = {
    val _ = ev // suppress warning
    andThen(Arrow.ApplyArrow.asInstanceOf[Arrow[U, W]])
  }

  /**
   * `a.applyArrowToSeq` is equivalent to `a.flatMap { case (a: Arrow, t: Seq) => a.traverse(t) }`
   * but propagates locals into `a`.
   */
  def applyArrowToSeq[V, W](implicit ev: U <:< (Arrow[V, W], Seq[V])): Arrow[T, Seq[W]] = {
    val _ = ev // suppress warning
    andThen[Seq[W]](Arrow.applyArrowToSeq[V, W].asInstanceOf[Arrow[U, Seq[W]]])
  }

  /**
   * `a.applyEffect(eff)` is equivalent to `{ x => for { y <- a(x); _ <- eff(y) } yield y }`
   */
  def applyEffect[O >: U](eff: Arrow.Effect[O]): Arrow[T, O] =
    andThen(Arrow.applyEffect(eff))

  /**
   * A convenience method, similar to `applyEffect`, but evaluates the effect asynchronously.
   */
  def applyEffectAsync[O >: U](eff: Arrow.Effect[O]): Arrow[T, Unit] =
    andThen(Arrow.async(Arrow.applyEffect(eff)))

  protected[stitch] def run(t: Try[T], l: Arrow.Locals): Stitch[U] = run(t, l, Arrow.identity)

  protected[stitch] def run[V](t: Try[T], l: Arrow.Locals, tail: Arrow[U, V]): Stitch[V] = ???

  protected[stitch] def run[T2 <: T, U2 >: U](
    ts: ArrayBuffer[Try[T2]],
    ls: ArrayBuffer[Arrow.Locals]
  ): Stitch[ArrayBuffer[Try[U2]]] =
    run(ts, ls, Arrow.identity[U2])

  protected[stitch] def run[T2 <: T, V](
    ts: ArrayBuffer[Try[T2]],
    ls: ArrayBuffer[Arrow.Locals],
    tail: Arrow[U, V]
  ): Stitch[ArrayBuffer[Try[V]]] =
    ???
}

object Arrow {

  /**
   * `Effect` is an arrow that applies a side effect (returns `Unit`)
   */
  type Effect[-T] = Arrow[T, Unit]

  object Effect {

    /**
     * An alias for `Arrow.apply` with the result type parameter fixed to `Unit`.
     */
    def apply[T](f: T => Stitch[Unit]): Effect[T] =
      Arrow.apply[T, Unit](f)

    /**
     * An alias for `Arrow.apply` with the result type parameter fixed to `Unit`
     * and the arrow calculation executed asynchronously.
     */
    def async[T](f: T => Stitch[Unit]): Effect[T] =
      Arrow.async(apply(f))

    /**
     * `Effect.andThen(a, b)` is equivalent to `{ x => for { _ <- a(x); _ <- b(x) } yield x }`
     */
    def andThen[T](a: Effect[T], b: Effect[T]): Effect[T] =
      applyEffect(a).andThen(b)

    /**
     * Performs all of the effects in order. If any effect fails, the
     * whole operation fails, and the subsequent effects are not
     * attempted.
     */
    def sequentially[T](effects: Effect[T]*): Effect[T] =
      effects.foldLeft[Effect[T]](unit[T])(andThen)

    /**
     * Performs all effects concurrently. If any effect fails, the
     * whole operation fails, but any of the effects may or may not have
     * taken place.
     */
    def inParallel[T](effects: Effect[T]*): Effect[T] =
      Arrow.collect(effects).unit

    /**
     * Executes the arrow only if `cond` is satisfied for the arrow input.
     */
    def onlyIf[T](cond: T => Boolean)(effect: Effect[T]): Effect[T] =
      Arrow.ifelse(cond, effect, Arrow.unit[T])

    /**
     * Skips the execution of the arrow if `cond` is false. This method
     * is similar to `onlyIf`, but the condition doesn't depend on the input.
     */
    def enabledBy[T](cond: => Boolean)(effect: Effect[T]): Effect[T] =
      EnabledBy(() => cond, effect, Arrow.unit)
  }

  /**
   * `Iso` is an arrow that is isomorphic (returns the same type as its input)
   */
  type Iso[T] = Arrow[T, T]

  object Iso {

    /**
     * Executes the arrow only if `cond` is satisfied for the arrow input.
     */
    def onlyIf[T](cond: T => Boolean)(iso: Iso[T]): Iso[T] =
      Arrow.ifelse(cond, iso, Arrow.identity)

    /**
     * Skips the execution of the arrow if `cond` is false. This method
     * is similar to `onlyIf`, but the condition doesn't depend on the input.
     */
    def enabledBy[T](cond: => Boolean)(iso: Iso[T]): Iso[T] =
      EnabledBy(() => cond, iso, Arrow.identity)
  }

  /**
   * Arrows that are synchronous (that is, they do not return Stitches
   * to evaluate). These Arrows are equivalent to `Try[T] => Try[U]`.
   */
  private[stitch] abstract class PureArrow[-T, +U] extends Arrow[T, U] {

    /**
     * Mark whether evaluation of this Arrow may have effects, such as
     * logging or accessing the system clock. We need to know this to
     * make sure that we don't optimize the effects away when performing
     * constant-folding.
     */
    def mayHaveEffect: Boolean = true

    /**
     * Mark whether evaluation of this Arrow may change inputs that are
     * `Throw`s. We need to know this because we cannot perform constant
     * folding for these Arrows.
     */
    def mayHandleExceptions: Boolean = true

    def applyPure(t: Try[T]): Try[U]

    private val applyPureF = applyPure _

    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      tail.run(applyPure(t), l)

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] =
      tail.run(TryBuffer.mapTry(ts)(applyPureF), ls)
  }

  private[stitch] case class TransformTry[-T, +U](
    f: Try[T] => Try[U],
    override val mayHaveEffect: Boolean,
    override val mayHandleExceptions: Boolean)
      extends PureArrow[T, U] {
    override def applyPure(t: Try[T]): Try[U] = t.transform(f)
  }

  private[stitch] case class Identity[T]() extends PureArrow[T, T] {
    override def mayHaveEffect = false
    override def mayHandleExceptions = false

    def applyPure(t: Try[T]): Try[T] = t

    override def apply(t: T): Stitch[T] = Stitch.value(t)
    override def apply(t: Try[T]): Stitch[T] = Stitch.const(t)
    override def apply(t: Stitch[T]): Stitch[T] = t

    override def run(t: Try[T], l: Locals): Stitch[T] = Stitch.const(t)
    override def run[U](t: Try[T], l: Locals, tail: Arrow[T, U]): Stitch[U] = tail.run(t, l)

    override def run[T2 <: T, U2 >: T](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals]
    ): Stitch[ArrayBuffer[Try[U2]]] =
      Stitch.const(Return(ts.asInstanceOf[ArrayBuffer[Try[U2]]]))

    override def run[T2 <: T, U](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[T, U]
    ): Stitch[ArrayBuffer[Try[U]]] =
      tail.run(ts, ls)
  }

  private[stitch] case class Call[T, U](g: ArrowGroup[T, U]) extends Arrow[T, U] {
    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      t match {
        case Return(t) => Call.run(t, g, l, tail)
        case _ => tail.run(t.asInstanceOf[Try[U]], l)
      }

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] =
      if (TryBuffer.containsReturn(ts)) {
        Call.run(ts, g, ls, tail)
      } else {
        tail.run(ts.asInstanceOf[ArrayBuffer[Try[U]]], ls)
      }
  }

  private object Call {
    def run[T, U, V](t: T, g: ArrowGroup[T, U], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      tail(Stitch.call(t, g), l)

    def run[T, T2 <: T, U, V](
      ts: ArrayBuffer[Try[T2]],
      g: ArrowGroup[T, U],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] =
      Stitch.callSeqTry(ts, g).transform {
        case Return(f) => tail.run(TryBuffer.flatMapReturn(ts)(f), ls)
        case t => tail.run(TryBuffer.flatMapReturn(ts)(_ => t.asInstanceOf[Try[U]]), ls)
      }
  }

  private[stitch] case class CallWithContext[C, T, U](gf: C => ArrowGroup[T, U])
      extends Arrow[(T, C), U] {
    override def run[V](t: Try[(T, C)], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      t match {
        case Return((t, c)) => Call.run(t, gf(c), l, tail)
        case _ => tail.run(t.asInstanceOf[Try[U]], l)
      }

    override def run[T2 <: (T, C), V](
      t2s: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      var hasReturn = false
      var hasCommonContext = true
      var commonContext = null.asInstanceOf[C]
      var i = 0

      while (hasCommonContext && i < t2s.length) {
        t2s(i) match {
          case Return((_, c: C @unchecked)) =>
            hasReturn = true
            if (commonContext == null) commonContext = c
            else if (c != commonContext) hasCommonContext = false
          case _ =>
        }
        i += 1
      }

      if (hasReturn) {
        if (hasCommonContext) {
          val ts = t2s.asInstanceOf[ArrayBuffer[Try[T]]]
          var i = 0
          while (i < ts.length) {
            t2s(i) match {
              case Return((t, _)) => ts(i) = Return(t)
              case _ =>
            }
            i += 1
          }
          Call.run(ts, gf(commonContext), ls, tail)

        } else { // !hasCommonContext
          val ss = t2s.asInstanceOf[ArrayBuffer[Stitch[U]]]
          var i = 0
          while (i < t2s.length) {
            ss(i) = t2s(i) match {
              case Return((t, c)) => Stitch.call(t, gf(c))
              case t => Stitch.const(t.asInstanceOf[Try[U]])
            }
            i += 1
          }
          Stitch.transformSeq(ss, ls, tail)
        }
      } else { // !hasReturn
        tail.run(t2s.asInstanceOf[ArrayBuffer[Try[U]]], ls)
      }
    }
  }

  private[stitch] case class AndThen[-T, U, +V](f: Arrow[T, U], g: Arrow[U, V])
      extends Arrow[T, V] {
    override def run[W](t: Try[T], l: Locals, tail: Arrow[V, W]): Stitch[W] =
      headTail(this) match {
        case AndThen(f, g) => f.run(t, l, g.andThen(tail))
      }

    override def run[T2 <: T, W](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[V, W]
    ): Stitch[ArrayBuffer[Try[W]]] =
      headTail(this) match {
        case AndThen(f, g) => f.run(ts, ls, g.andThen(tail))
      }

    @tailrec
    private[this] final def headTail[A, B](t: AndThen[A, _, B]): AndThen[A, _, B] =
      t match {
        case AndThen(AndThen(f, g), h) => headTail(AndThen(f, AndThen(g, h)))
        case t => t
      }
  }

  private[stitch] case class JoinMap2[T, U, V, R](a1: Arrow[T, U], a2: Arrow[T, V], k: (U, V) => R)
      extends Arrow[T, R] {
    override def run[S](t: Try[T], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(Stitch.joinMap(a1(t, l), a2(t, l))(k), l)

    override def run[T2 <: T, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2) => tail.run(TryBuffer.joinMap(ts1, ts2, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap3[T, U, V, W, R](
    a1: Arrow[T, U],
    a2: Arrow[T, V],
    a3: Arrow[T, W],
    k: (U, V, W) => R)
      extends Arrow[T, R] {
    override def run[S](t: Try[T], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(Stitch.joinMap(a1(t, l), a2(t, l), a3(t, l))(k), l)

    override def run[T2 <: T, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3) => tail.run(TryBuffer.joinMap(ts1, ts2, ts3, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap4[T, U, V, W, X, R](
    a1: Arrow[T, U],
    a2: Arrow[T, V],
    a3: Arrow[T, W],
    a4: Arrow[T, X],
    k: (U, V, W, X) => R)
      extends Arrow[T, R] {
    override def run[S](t: Try[T], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(Stitch.joinMap(a1(t, l), a2(t, l), a3(t, l), a4(t, l))(k), l)

    override def run[T2 <: T, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4) =>
            tail.run(TryBuffer.joinMap(ts1, ts2, ts3, ts4, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap5[A, B, C, D, E, F, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    k: (B, C, D, E, F) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(Stitch.joinMap(a1(t, l), a2(t, l), a3(t, l), a4(t, l), a5(t, l))(k), l)

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5) =>
            tail.run(TryBuffer.joinMap(ts1, ts2, ts3, ts4, ts5, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap6[A, B, C, D, E, F, G, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    k: (B, C, D, E, F, G) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(Stitch.joinMap(a1(t, l), a2(t, l), a3(t, l), a4(t, l), a5(t, l), a6(t, l))(k), l)

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6) =>
            tail.run(TryBuffer.joinMap(ts1, ts2, ts3, ts4, ts5, ts6, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap7[A, B, C, D, E, F, G, H, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    k: (B, C, D, E, F, G, H) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(
        Stitch.joinMap(a1(t, l), a2(t, l), a3(t, l), a4(t, l), a5(t, l), a6(t, l), a7(t, l))(k),
        l)

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(TryBuffer.copy(ts), ls),
          a7.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6, ts7) =>
            tail.run(TryBuffer.joinMap(ts1, ts2, ts3, ts4, ts5, ts6, ts7, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap8[A, B, C, D, E, F, G, H, I, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    k: (B, C, D, E, F, G, H, I) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(
        Stitch.joinMap(
          a1(t, l),
          a2(t, l),
          a3(t, l),
          a4(t, l),
          a5(t, l),
          a6(t, l),
          a7(t, l),
          a8(t, l)
        )(k),
        l
      )

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(TryBuffer.copy(ts), ls),
          a7.run(TryBuffer.copy(ts), ls),
          a8.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8) =>
            tail.run(TryBuffer.joinMap(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap9[A, B, C, D, E, F, G, H, I, J, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    k: (B, C, D, E, F, G, H, I, J) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(
        Stitch.joinMap(
          a1(t, l),
          a2(t, l),
          a3(t, l),
          a4(t, l),
          a5(t, l),
          a6(t, l),
          a7(t, l),
          a8(t, l),
          a9(t, l)
        )(k),
        l
      )

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(TryBuffer.copy(ts), ls),
          a7.run(TryBuffer.copy(ts), ls),
          a8.run(TryBuffer.copy(ts), ls),
          a9.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9) =>
            tail.run(TryBuffer.joinMap(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap10[A, B, C, D, E, F, G, H, I, J, K, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    k: (B, C, D, E, F, G, H, I, J, K) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(
        Stitch.joinMap(
          a1(t, l),
          a2(t, l),
          a3(t, l),
          a4(t, l),
          a5(t, l),
          a6(t, l),
          a7(t, l),
          a8(t, l),
          a9(t, l),
          a10(t, l)
        )(k),
        l
      )

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(TryBuffer.copy(ts), ls),
          a7.run(TryBuffer.copy(ts), ls),
          a8.run(TryBuffer.copy(ts), ls),
          a9.run(TryBuffer.copy(ts), ls),
          a10.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10) =>
            tail.run(TryBuffer.joinMap(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10, k), ls)
        }
    }
  }

  private[stitch] case class JoinMap11[A, B, C, D, E, F, G, H, I, J, K, L, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    k: (B, C, D, E, F, G, H, I, J, K, L) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(
        Stitch
          .joinMap(
            a1(t, l),
            a2(t, l),
            a3(t, l),
            a4(t, l),
            a5(t, l),
            a6(t, l),
            a7(t, l),
            a8(t, l),
            a9(t, l),
            a10(t, l),
            a11(t, l)
          )(k),
        l
      )

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(TryBuffer.copy(ts), ls),
          a7.run(TryBuffer.copy(ts), ls),
          a8.run(TryBuffer.copy(ts), ls),
          a9.run(TryBuffer.copy(ts), ls),
          a10.run(TryBuffer.copy(ts), ls),
          a11.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10, ts11) =>
            tail.run(
              TryBuffer.joinMap(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10, ts11, k),
              ls)
        }
    }
  }

  private[stitch] case class JoinMap12[A, B, C, D, E, F, G, H, I, J, K, L, M, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M],
    k: (B, C, D, E, F, G, H, I, J, K, L, M) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(
        Stitch.joinMap(
          a1(t, l),
          a2(t, l),
          a3(t, l),
          a4(t, l),
          a5(t, l),
          a6(t, l),
          a7(t, l),
          a8(t, l),
          a9(t, l),
          a10(t, l),
          a11(t, l),
          a12(t, l)
        )(k),
        l
      )

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(TryBuffer.copy(ts), ls),
          a7.run(TryBuffer.copy(ts), ls),
          a8.run(TryBuffer.copy(ts), ls),
          a9.run(TryBuffer.copy(ts), ls),
          a10.run(TryBuffer.copy(ts), ls),
          a11.run(TryBuffer.copy(ts), ls),
          a12.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10, ts11, ts12) =>
            tail.run(
              TryBuffer.joinMap(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10, ts11, ts12, k),
              ls
            )
        }
    }
  }

  private[stitch] case class JoinMap13[A, B, C, D, E, F, G, H, I, J, K, L, M, N, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M],
    a13: Arrow[A, N],
    k: (B, C, D, E, F, G, H, I, J, K, L, M, N) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(
        Stitch.joinMap(
          a1(t, l),
          a2(t, l),
          a3(t, l),
          a4(t, l),
          a5(t, l),
          a6(t, l),
          a7(t, l),
          a8(t, l),
          a9(t, l),
          a10(t, l),
          a11(t, l),
          a12(t, l),
          a13(t, l)
        )(k),
        l
      )

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(TryBuffer.copy(ts), ls),
          a7.run(TryBuffer.copy(ts), ls),
          a8.run(TryBuffer.copy(ts), ls),
          a9.run(TryBuffer.copy(ts), ls),
          a10.run(TryBuffer.copy(ts), ls),
          a11.run(TryBuffer.copy(ts), ls),
          a12.run(TryBuffer.copy(ts), ls),
          a13.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10, ts11, ts12, ts13) =>
            tail.run(
              TryBuffer
                .joinMap(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10, ts11, ts12, ts13, k),
              ls
            )
        }
    }
  }

  private[stitch] case class JoinMap14[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, R](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M],
    a13: Arrow[A, N],
    a14: Arrow[A, O],
    k: (B, C, D, E, F, G, H, I, J, K, L, M, N, O) => R)
      extends Arrow[A, R] {
    override def run[S](t: Try[A], l: Locals, tail: Arrow[R, S]): Stitch[S] =
      tail(
        Stitch.joinMap(
          a1(t, l),
          a2(t, l),
          a3(t, l),
          a4(t, l),
          a5(t, l),
          a6(t, l),
          a7(t, l),
          a8(t, l),
          a9(t, l),
          a10(t, l),
          a11(t, l),
          a12(t, l),
          a13(t, l),
          a14(t, l)
        )(k),
        l
      )

    override def run[T2 <: A, S](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[R, S]
    ): Stitch[ArrayBuffer[Try[S]]] = {
      Stitch
        .join(
          a1.run(TryBuffer.copy(ts), ls),
          a2.run(TryBuffer.copy(ts), ls),
          a3.run(TryBuffer.copy(ts), ls),
          a4.run(TryBuffer.copy(ts), ls),
          a5.run(TryBuffer.copy(ts), ls),
          a6.run(TryBuffer.copy(ts), ls),
          a7.run(TryBuffer.copy(ts), ls),
          a8.run(TryBuffer.copy(ts), ls),
          a9.run(TryBuffer.copy(ts), ls),
          a10.run(TryBuffer.copy(ts), ls),
          a11.run(TryBuffer.copy(ts), ls),
          a12.run(TryBuffer.copy(ts), ls),
          a13.run(TryBuffer.copy(ts), ls),
          a14.run(ts, ls)
        )
        .flatMap {
          case (ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10, ts11, ts12, ts13, ts14) =>
            tail.run(
              TryBuffer.joinMap(
                ts1,
                ts2,
                ts3,
                ts4,
                ts5,
                ts6,
                ts7,
                ts8,
                ts9,
                ts10,
                ts11,
                ts12,
                ts13,
                ts14,
                k
              ),
              ls
            )
        }
    }
  }

  private[stitch] case class Collect[T, U](as: Seq[Arrow[T, U]]) extends Arrow[T, Seq[U]] {
    assert(as.length > 0)

    private[this] val constants =
      if (as.exists { case Arrow.Const(Return(_)) => true; case _ => false }) {
        as.map {
          case Arrow.Const(Return(v)) => v
          case _ => null.asInstanceOf[U]
        }
      } else null

    private[this] def isConstant(i: Int): Boolean =
      constants != null && constants(i) != null

    override def run[V](t: Try[T], l: Locals, tail: Arrow[Seq[U], V]): Stitch[V] =
      tail(Stitch.traverse(as)(a => a.run(t, l)), l)

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[Seq[U], V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      Stitch
        .traverse(as) {
          case Arrow.Const(Return(_)) => Stitch.value(null)
          case a => a.run(TryBuffer.copy(ts), ls)
        }
        .flatMap { tss => // tss(j) is null if the jth arrow is constant

          // it's safe to reuse the first non-null tss(j) because it's uniquely owned
          val ts = tss.find(_ != null).get.asInstanceOf[ArrayBuffer[Try[Seq[U]]]]
          var i = 0

          while (i < ts.length) {
            // check for Throw
            var t: Try[U] = null
            var j = 0
            while (t == null && j < as.length) {
              if (!isConstant(j)) {
                tss(j)(i) match {
                  case Return(_) =>
                  case tt => t = tt
                }
              }
              j += 1
            }
            if (t != null) {
              ts(i) = t.asInstanceOf[Try[Seq[U]]]

              // otherwise collect the Returns
            } else {
              val us = new ArrayBuffer[U](as.length)
              j = 0
              while (j < as.length) {
                if (isConstant(j))
                  us += constants(j)
                else
                  tss(j)(i) match {
                    case Return(u) => us += u
                    case _ => scala.sys.error("bug")
                  }
                j += 1
              }
              ts(i) = Return(us)
            }
            i += 1
          }
          tail.run(ts, ls)
        }
    }
  }

  private[stitch] case class Sequence[T, U](a: Arrow[T, U]) extends Arrow[Seq[T], Seq[U]] {
    override def run[V](t: Try[Seq[T]], l: Locals, tail: Arrow[Seq[U], V]): Stitch[V] = {
      t match {
        case Return(ts) =>
          a.run(TryBuffer.values(ts), ArrayBuffer.fill(ts.length)(l))
            .flatMap { ts => tail.run(TryBuffer.collect(ts), l) }

        case _ =>
          tail.run(t.asInstanceOf[Try[Seq[U]]], l)
      }
    }

    override def run[T2 <: Seq[T], V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[Seq[U], V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      var totalCount = 0
      var i = 0

      while (i < ts.length) {
        ts(i) match {
          case Return(seq) => totalCount += seq.length
          case _ =>
        }
        i += 1
      }

      if (totalCount == 0) {
        tail.run(ts.asInstanceOf[ArrayBuffer[Try[Seq[U]]]], ls)
      } else {
        val flattenedInput = new ArrayBuffer[Try[T]](totalCount)
        val flattenedLocals = new ArrayBuffer[Locals](totalCount)
        var i = 0

        while (i < ts.length) {
          ts(i) match {
            case Return(seq) =>
              val iter = seq.iterator
              while (iter.hasNext) {
                flattenedInput += Return(iter.next)
                flattenedLocals += ls(i)
              }

            case _ =>
          }

          i += 1
        }

        a.run(flattenedInput, flattenedLocals).flatMap { flattenedResults =>
          var i = 0
          var j = 0
          val splitResults = ts.asInstanceOf[ArrayBuffer[Try[Seq[U]]]]

          while (i < ts.length) {
            ts(i) match {
              case Return(seq) =>
                var iRes: Try[Seq[U]] = null
                var k = j
                val kEnd = j + seq.length

                // first pass, look for any Throws in the range `j` to `j + seq.length`
                while (k < kEnd && iRes == null) {
                  flattenedResults(k) match {
                    case t @ Throw(_) => iRes = t.asInstanceOf[Try[Seq[U]]]
                    case _ =>
                  }
                  k += 1
                }

                // if no Throws, then all results are Returns, and can be converted to a
                // a Return[Seq[U]]
                if (iRes == null) {
                  k = j
                  val buf = new ArrayBuffer[U](seq.length)

                  while (k < kEnd) {
                    flattenedResults(k) match {
                      case Return(x) => buf += x
                      case _ => scala.sys.error("stitch bug")
                    }
                    k += 1
                  }

                  iRes = Return(buf)
                }

                splitResults(i) = iRes
                j += seq.length

              case _ =>
            }

            i += 1
          }

          tail.run(splitResults, ls)
        }
      }
    }
  }

  private[stitch] case class Choose[T, U](choices: Seq[(T => Boolean, Arrow[T, U])])
      extends Arrow[T, U] {
    private[this] val numChoices = choices.size

    private[this] val constants =
      if (choices.exists { case (_, Arrow.Const(_)) => true; case _ => false }) {
        choices.map {
          case (_, Arrow.Const(t)) => t
          case _ => null.asInstanceOf[Try[U]]
        }
      } else null

    private[this] def isConstant(i: Int): Boolean =
      constants != null && constants(i) != null

    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      t match {
        case Return(x) =>
          var i = 0

          while (i < numChoices) {
            val (cond, arrow) = choices(i)
            if (cond(x)) return arrow.run(t, l, tail)
            i += 1
          }

          throw new MatchError(s"choose $x")

        case Throw(_) =>
          tail.run(t.asInstanceOf[Try[U]], l)
      }

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val numInputs = ts.size
      // pairwise with `choices`, indicates how many inputs matched that choice
      val inputCounts = new Array[Int](numChoices)
      // how many different choices were matched across all input values
      var uniqueArrows = 0
      // pairwise with `ts`, indicates the index of the choice that the input matched against.
      // -1 indicates the input is a `Throw`.
      val which = new Array[Int](ts.size)
      var hasThrows = false
      // index of the last choice that was matched.  only used when there is a single
      // choice used for all inputs.
      var lastMatch = -1
      var i = 0

      // in the first pass over the inputs, find the matching choice, recording the match
      // in `which`.  we then increment the input count for that choice.  if no choice is matched,
      // then the input is converted to a `Throw` with a `MatchError`.
      while (i < numInputs) {
        ts(i) match {
          case Return(x) =>
            var found = false
            var j = 0

            while (j < numChoices && !found) {
              val (cond, _) = choices(j)
              if (cond(x)) {
                found = true
                if (inputCounts(j) == 0) uniqueArrows += 1
                inputCounts(j) += 1
                which(i) = j
                lastMatch = j
              }

              j += 1
            }

            if (!found) {
              ts(i) = Throw(new MatchError(s"choose $x"))
              which(i) = -1
            }

          case _ =>
            hasThrows = true
            which(i) = -1
        }

        i += 1
      }

      uniqueArrows match {
        case 0 =>
          // all failures, nothing to dispatch
          tail.run(ts.asInstanceOf[ArrayBuffer[Try[U]]], ls)

        case 1 if !hasThrows =>
          // with only a single choosen arrow and Throws to filter out, we can just forward the
          // entire input to `arrow`
          val (_, arrow) = choices(lastMatch)
          arrow.run(ts, ls, tail)

        case _ =>
          // multiple arrows were chosen, so we need to physically partition the inputs to pass
          // to each of the chosen arrows.
          val partitionedInputs = new ArrayBuffer[ArrayBuffer[Try[T2]]](numChoices)
          val partitionedLocals = new ArrayBuffer[ArrayBuffer[Locals]](numChoices)
          var i = 0

          // first, allocate correctly sized input buffers for each arrow.  if a choice has no
          // matched inputs, we reuse an empty buffer.
          while (i < numChoices) {
            inputCounts(i) match {
              case _ if isConstant(i) =>
                partitionedInputs += null
                partitionedLocals += null
              case 0 =>
                partitionedInputs += emptyBuffer.asInstanceOf[ArrayBuffer[Try[T2]]]
                partitionedLocals += emptyBuffer.asInstanceOf[ArrayBuffer[Locals]]
              case size =>
                partitionedInputs += new ArrayBuffer[Try[T2]](size)
                partitionedLocals += new ArrayBuffer[Locals](size)
            }

            i += 1
          }

          // next, iterate over the inputs and place in the correct partitioned input buffer
          i = 0
          while (i < numInputs) {
            which(i) match {
              case -1 =>
              case idx if isConstant(idx) =>
              case idx =>
                partitionedInputs(idx) += ts(i)
                partitionedLocals(idx) += ls(i)
            }
            i += 1
          }

          // next, iterate over the partitions, creating a `Stitch` for each sub-arrow
          // invocation.
          val stitches = new ArrayBuffer[Stitch[Any]](numChoices)
          i = 0
          while (i < numChoices) {
            stitches += (
              inputCounts(i) match {
                case _ if isConstant(i) => Stitch.value(null)
                case 0 => emptyBufferStitch
                case 1 =>
                  val (_, arrow) = choices(i)
                  arrow.run(partitionedInputs(i)(0), partitionedLocals(i)(0)).liftToTry
                case _ =>
                  val (_, arrow) = choices(i)
                  arrow.run(partitionedInputs(i), partitionedLocals(i))
              }
            )
            i += 1
          }

          // collect the stitches and merge the results back together into a single sequence
          Stitch.collectNoCopy(stitches).flatMap { resultsSeq =>
            // resultsSeq(j) is null if the jth arrow is constant
            val result = ts.asInstanceOf[ArrayBuffer[Try[U]]]
            val iters = stitches.asInstanceOf[ArrayBuffer[Iterator[Try[U]]]]
            var i = 0

            while (i < numChoices) {
              inputCounts(i) match {
                case _ if isConstant(i) => null
                case 0 | 1 => null
                case _ => iters(i) = resultsSeq(i).asInstanceOf[ArrayBuffer[Try[U]]].iterator
              }
              i += 1
            }

            i = 0
            while (i < numInputs) {
              which(i) match {
                case -1 =>
                case idx if isConstant(idx) => result(i) = constants(idx)
                case idx if inputCounts(idx) == 1 =>
                  result(i) = resultsSeq(idx).asInstanceOf[Try[U]]
                case idx => result(i) = iters(idx).next
              }
              i += 1
            }

            tail.run(result, ls)
          }
      }
    }
  }

  private[stitch] case class Apply[T, U](t: T) extends PureArrow[T => Try[U], U] {
    override def mayHandleExceptions = false
    def applyPure(f: Try[T => Try[U]]): Try[U] = f.flatMap(_(t))
  }

  private[stitch] case class Map[T, U](f: T => U) extends PureArrow[T, U] {
    override def mayHandleExceptions = false
    def applyPure(t: Try[T]): Try[U] = t.map(f)
  }

  private[stitch] case class MapFailure[T](pf: PartialFunction[Throwable, Throwable])
      extends PureArrow[T, T] {
    def applyPure(t: Try[T]): Try[T] =
      t.rescue {
        case ex if pf.isDefinedAt(ex) => Throw(pf(ex))
      }
  }

  private[stitch] case class FlatMap[T, U](f: T => Stitch[U]) extends Arrow[T, U] {
    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      t match {
        case Return(t) => tail(Stitch.app(f, t), l)
        case _ => tail.run(t.asInstanceOf[Try[U]], l)
      }

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val ss = ts.asInstanceOf[ArrayBuffer[Stitch[U]]]
      var i = 0
      while (i < ts.length) {
        ss(i) = ts(i) match {
          case Return(t) => Stitch.app(f, t)
          case t => Stitch.const(t.asInstanceOf[Try[U]])
        }
        i += 1
      }
      Stitch.transformSeq(ss, ls, tail)
    }
  }

  private object ApplyArrow extends Arrow[(Arrow[Any, Any], Any), Any] {
    override def run[V](t: Try[(Arrow[Any, Any], Any)], l: Locals, tail: Arrow[Any, V]): Stitch[V] =
      t match {
        case Return((a, tt)) => a.run(Return(tt), l, tail)
        case _ => tail.run(t.asInstanceOf[Try[Any]], l)
      }

    override def run[T2 <: (Arrow[Any, Any], Any), V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[Any, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val len = ts.length
      assert(ls.length == len)

      if (ts.isEmpty) return tail.run(ts.asInstanceOf[ArrayBuffer[Try[Any]]], ls)

      val tts = ts.asInstanceOf[ArrayBuffer[Try[(Arrow[Any, Any], Any)]]]

      /** contains the arrow of the first input if head is a Return, and null if head is a Throw */
      val firstArrow: Arrow[Any, Any] = tts.head match {
        case Return((a, _)) => a
        case Throw(_) => null
      }

      /**
       * true if all inputs are Return((arrow, _)), where arrow is the same for all inputs,
       * or true if all inputs are Throws, otherwise false
       */
      val allSame = tts.forall {
        case Return((a, _)) => firstArrow == a
        case Throw(_) => firstArrow == null
      }

      if (allSame && firstArrow != null) { // all inputs have the same arrow and are Returns
        val args = TryBuffer.mapTry(tts) {
          case Return((_, arg)) => Return(arg)
          case _ =>
            throw new IllegalStateException(
              "This should not be possible since firstArrow should only be populated if all args are Returns"
            )
        }
        firstArrow.run(args, ls, tail)
      } else if (allSame) { // all inputs are Throws
        tail.run(tts, ls)
      } else {

        /**
         * Input contains arrows that are not all the same or is a mix of Returns and Throws.
         * Inputs are grouped so that all inputs for a given arrow are batched together
         * and all inputs that are throws are batched together. This strategy ensures optimal batching.
         *
         * This grouping process necessarily shuffles the ordering of the inputs,
         * so when the batches are run they need to be unshuffled so they are back in the original order.
         */
        val countByArrow = new java.util.HashMap[Arrow[Any, Any], Int]()

        /** count the number of inputs for each arrow */
        tts.foreach {
          case Return((a, _)) => countByArrow.merge(a, 1, _ + _)
          case Throw(_) => countByArrow.merge(identityInstance, 1, _ + _)
        }

        // Map[Arrow -> (Indices, Args, Locals)], reuse the same Hashmap
        val groupedByArrow = countByArrow.asInstanceOf[java.util.HashMap[
          Arrow[Any, Any],
          (ArrayBuffer[Int], ArrayBuffer[Try[Any]], ArrayBuffer[Locals])
        ]]

        // Map[Arrow -> [Int | (Indices, Args, Locals)]], reuse the same Hashmap,
        // but since theres no type unions just do a pattern match on an Any
        val unionOfCountAndGrouped =
          countByArrow.asInstanceOf[java.util.HashMap[Arrow[Any, Any], Any]]

        // groupBy, store result in groupedByArrow
        var i = 0
        while (i < len) {
          ts(i) match {
            case Return((arrow, arg)) =>
              unionOfCountAndGrouped.get(arrow) match {
                case size: Int =>
                  groupedByArrow.put(
                    arrow,
                    (
                      new ArrayBuffer[Int](size) += i,
                      new ArrayBuffer[Try[Any]](size) += Return(arg),
                      new ArrayBuffer[Locals](size) += ls(i)
                    )
                  )
                case (
                      indices: ArrayBuffer[Int @unchecked],
                      args: ArrayBuffer[Try[Any] @unchecked],
                      locals: ArrayBuffer[Locals @unchecked]) =>
                  indices += i
                  args += Return(arg)
                  locals += ls(i)
                case arg =>
                  throw new IllegalArgumentException(
                    s"Expected either Int or (ArrayBuffer, ArrayBuffer, ArrayBuffer) but got $arg")
              }

            case t: Try[_] =>
              unionOfCountAndGrouped.get(identityInstance) match {
                case size: Int =>
                  groupedByArrow.put(
                    identityInstance,
                    (
                      new ArrayBuffer[Int](size) += i,
                      new ArrayBuffer[Try[Any]](size) += t,
                      new ArrayBuffer[Locals](size) += ls(i)
                    )
                  )
                case (
                      indices: ArrayBuffer[Int @unchecked],
                      args: ArrayBuffer[Try[Any] @unchecked],
                      locals: ArrayBuffer[Locals @unchecked]) =>
                  indices += i
                  args += t
                  locals += ls(i)
                case arg =>
                  throw new IllegalArgumentException(
                    s"Expected either Int or (ArrayBuffer, ArrayBuffer, ArrayBuffer) but got $arg")
              }
          }
          i += 1
        }

        val ss = ts.asInstanceOf[ArrayBuffer[Stitch[Any]]]

        val groupedIterator = groupedByArrow.entrySet().iterator()
        while (groupedIterator.hasNext) {
          val n = groupedIterator.next()
          val arrow = n.getKey
          val (indices, args, locals) = n.getValue
          // run optimal batches
          // We return a ref here to ensure that the Stitch returned by `arrow`
          // is run only once.  That way, accessing the result at different indexes
          // in the loop below will not rerun the entire Stitch.
          // This prevents a very specific class of bugs when arrow is a long chain of arrows
          // which have not yet been resolved from being resolved early.
          // Because the grouped arrows are resolved at the same time it means the second iteration
          // of the loop - which is expecting an unresolved arrow - finds a resolved one instead
          // and you get a ClassCastException.  This happens in the while loop below.
          // We were not able to create a unit test in Stitch/Arrow that could reproduce but there is
          // a test case in Strato which does: STTR-6433 regression test in
          // strato/src/test/scala/com/twitter/strato/rpc/ServerTest.scala
          val stitches = Stitch.ref(arrow.run(args, locals))
          // reshuffle the results back into the original order
          var i = 0
          while (i < indices.length) {
            // save the current value of var, `i` because accessing the Stitch at the
            // index is async and using `i` could result in the wrong value when finally read
            val currIndex = i
            ss(indices(i)) = stitches.map(buf => buf(currIndex)).lowerFromTry
            i += 1
          }
        }

        Stitch.transformSeq(ss, ls, tail)
      }
    }
  }

  private[stitch] case class Handle[T, U >: T](f: PartialFunction[Throwable, U])
      extends PureArrow[T, U] {
    def applyPure(t: Try[T]): Try[U] = t.handle(f)
  }

  private[stitch] case class Rescue[T, U >: T](f: PartialFunction[Throwable, Stitch[U]])
      extends Arrow[T, U] {
    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      t match {
        case Throw(e) if f.isDefinedAt(e) => tail(Stitch.app(f, e), l)
        case _ => tail.run(t.asInstanceOf[Try[U]], l)
      }

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val ss = ts.asInstanceOf[ArrayBuffer[Stitch[U]]]
      var i = 0
      while (i < ts.length) {
        ss(i) = ts(i) match {
          case Throw(e) if f.isDefinedAt(e) => Stitch.app(f, e)
          case t => Stitch.const(t.asInstanceOf[Try[U]])
        }
        i += 1
      }
      Stitch.transformSeq(ss, ls, tail)
    }
  }

  private[stitch] case class FlatmapTry[T, U](f: Try[T] => Stitch[U]) extends Arrow[T, U] {
    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      tail(Stitch.app(f, t), l)

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val ss = ts.asInstanceOf[ArrayBuffer[Stitch[U]]]
      var i = 0
      while (i < ts.length) {
        ss(i) = Stitch.app(f, ts(i))
        i += 1
      }
      Stitch.transformSeq(ss, ls, tail)
    }
  }

  private[stitch] case class Respond[T](f: Try[T] => Unit) extends PureArrow[T, T] {
    def applyPure(t: Try[T]): Try[T] = {
      try {
        f(t)
      } catch {
        case e: NonLocalReturnControl[_] => throw new StitchNonLocalReturnControl(e)
        case NonFatal(_) =>
      }
      t
    }
  }

  private[stitch] case class Ensure[T](f: () => Unit) extends PureArrow[T, T] {
    def applyPure(t: Try[T]): Try[T] = {
      try {
        f()
      } catch {
        case e: NonLocalReturnControl[_] => throw new StitchNonLocalReturnControl(e)
        case NonFatal(_) =>
      }
      t
    }
  }

  private[stitch] case class OnSuccess[T](f: T => Unit) extends PureArrow[T, T] {
    override def mayHandleExceptions = false
    def applyPure(t: Try[T]): Try[T] = {
      t match {
        case Return(v) =>
          try {
            f(v)
          } catch {
            case e: NonLocalReturnControl[_] => throw new StitchNonLocalReturnControl(e)
            case NonFatal(_) =>
          }
        case _ =>
      }
      t
    }
  }

  private[stitch] case class OnFailure[T](f: Throwable => Unit) extends PureArrow[T, T] {
    def applyPure(t: Try[T]): Try[T] = {
      t match {
        case Throw(e) =>
          try {
            f(e)
          } catch {
            case e: NonLocalReturnControl[_] => throw new StitchNonLocalReturnControl(e)
            case NonFatal(_) =>
          }
        case _ =>
      }
      t
    }
  }

  private[stitch] case object ToUnit extends PureArrow[Any, Unit] {
    override def mayHandleExceptions = false
    override def mayHaveEffect = false
    def applyPure(t: Try[Any]): Try[Unit] =
      t match {
        case Return(_) => Return.Unit
        case _ => t.asInstanceOf[Try[Unit]]
      }
  }

  private[stitch] case class LiftToTry[T]() extends PureArrow[T, Try[T]] {
    override def mayHaveEffect = false
    def applyPure(t: Try[T]): Return[Try[T]] = Return(t)
  }

  private[stitch] case class LowerFromTry[T]() extends PureArrow[Try[T], T] {
    override def mayHaveEffect = false
    override def mayHandleExceptions = false
    def applyPure(tt: Try[Try[T]]): Try[T] = tt match {
      case Return(t) => t
      case _ => tt.asInstanceOf[Try[T]]
    }
  }

  private[stitch] case class LiftToOption[T](f: PartialFunction[Throwable, Boolean])
      extends PureArrow[T, Option[T]] {
    override def mayHaveEffect: Boolean =
      !f.eq(Stitch.liftToOptionDefaultFn) && !f.eq(Stitch.liftNotFoundToOptionFn)

    def applyPure(t: Try[T]): Try[Option[T]] = t match {
      case Return(t) => Return(Some(t))
      case Throw(e) if f.isDefinedAt(e) && f(e) => Return.None
      case _ => t.asInstanceOf[Try[Option[T]]]
    }
  }

  private[stitch] case class LowerFromOption[T](e: Throwable) extends PureArrow[Option[T], T] {
    override def mayHaveEffect = false
    override def mayHandleExceptions = false
    private[this] val eTry = Throw(e)
    def applyPure(to: Try[Option[T]]): Try[T] = to match {
      case Return(Some(t)) => Return(t)
      case Return(_) => eTry
      case _ => to.asInstanceOf[Try[T]]
    }
  }

  private[stitch] case class Const[T](t: Try[T]) extends PureArrow[Any, T] {
    override def mayHaveEffect = false
    override def mayHandleExceptions = false

    def applyPure(ta: Try[Any]): Try[T] =
      ta match {
        case Return(_) => t
        case _ => ta.asInstanceOf[Try[T]]
      }
  }

  private[stitch] case class EnabledBy[T, U](cond: () => Boolean, a: Arrow[T, U], skip: Arrow[T, U])
      extends Arrow[T, U] {
    private[this] def arrow =
      if (cond()) a else skip

    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      arrow.run(t, l, tail)

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] =
      arrow.run(ts, ls, tail)
  }

  private[stitch] case class Within[T, U](timeout: Duration, timer: Timer, a: Arrow[T, U])
      extends Arrow[T, U] {

    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] = {
      tail(a.run(t, l).within(timeout)(timer), l)
    }

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val ss = ts.asInstanceOf[ArrayBuffer[Stitch[U]]]
      var i = 0
      while (i < ts.length) {
        ss(i) = a.run(ts(i), ls(i)).within(timeout)(timer)
        i += 1
      }
      Stitch.transformSeq(ss, ls, tail)
    }

  }

  private[stitch] case class Time[T, U](a: Arrow[T, U]) extends Arrow[T, (Try[U], Duration)] {
    override def run[V](t: Try[T], l: Locals, tail: Arrow[(Try[U], Duration), V]): Stitch[V] =
      tail(Stitch.time(a.run(t, l)), l)

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[(Try[U], Duration), V]
    ): Stitch[ArrayBuffer[Try[V]]] =
      Stitch.time(a.run(ts, ls)).flatMap {
        case (Return(us), d) =>
          val uds = ts.asInstanceOf[ArrayBuffer[Try[(Try[U], Duration)]]]
          var i = 0
          while (i < ts.length) {
            uds(i) = Return((us(i), d))
            i += 1
          }
          tail.run(uds, ls)
        case (t, _) =>
          val uds = ts.asInstanceOf[ArrayBuffer[Try[(Try[U], Duration)]]]
          var i = 0
          while (i < ts.length) {
            uds(i) = t.asInstanceOf[Try[(Try[U], Duration)]]
            i += 1
          }
          tail.run(uds, ls)
      }
  }

  private[stitch] case class Recursive[T, U](f: Arrow[T, U] => Arrow[T, U]) extends Arrow[T, U] {
    private[this] val arrow = f(this)

    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      Stitch.recursive(arrow.run(t, l, tail))

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] =
      Stitch.recursive(arrow.run(ts, ls, tail))
  }

  private[stitch] case class Wrap[T, U](arrow: () => Arrow[T, U]) extends Arrow[T, U] {
    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      arrow().run(t, l, tail)

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] =
      arrow().run(ts, ls, tail)
  }

  private[stitch] case object AllLocals extends Arrow[Any, Locals] {
    override def run[V](t: Try[Any], l: Locals, tail: Arrow[Locals, V]): Stitch[V] =
      tail.run(Return(l), l)

    override def run[T2 <: Any, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[Locals, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val len = ts.length

      val rs = ts.asInstanceOf[ArrayBuffer[Try[Locals]]]
      var i = 0
      while (i < len) {
        rs(i) = Return(ls(i))
        i += 1
      }
      tail.run(rs, ls)
    }
  }

  /** For testing within Stitch only. This allows you to examine the batch that `run` gets */
  private[stitch] case object ExtractBatch extends Arrow[Any, Seq[Any]] {
    override def run[V](t: Try[Any], l: Locals, tail: Arrow[Seq[Any], V]): Stitch[V] =
      tail.run(Return(Seq(t)), l)

    override def run[T2 <: Any, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[Seq[Any], V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      tail.run(ts.map(_ => Return(ts).asInstanceOf[Try[Seq[Any]]]), ls)
    }
  }

  private[stitch] object TwitterLocals {

    /**
     * Wraps the entire Arrow batch in the same [[TwitterLocal]]s set with the provided [[Letter]]
     * incurs much less overhead than when each batch element has different [[TwitterLocal]]s.
     *
     * @note Scopes the [[letter]] regardless of the state of the input.
     *       However, if [[v]] throws, then `a` will be executed with a [[Throw]] of that exception without the
     *       [[letter]] scoping.
     *
     * @param letter a [[Letter]] which wraps a Stitch and `let-scopes` locals around it
     * @param a      underlying Arrow
     */
    private[stitch] case class Let[T, U, L](
      v: () => L,
      letter: Letter[L],
      a: Arrow[T, U])
        extends Arrow[T, U] {

      override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] = {
        try tail(Stitch.let(letter)(v())(a.run(t, l)), l)
        catch {
          case e: NonLocalReturnControl[_] => throw new StitchNonLocalReturnControl(e)
          case NonFatal(t) => tail(a.run(Throw(t), l), l)
        }
      }

      override def run[T2 <: T, V](
        ts: ArrayBuffer[Try[T2]],
        ls: ArrayBuffer[Locals],
        tail: Arrow[U, V]
      ): Stitch[ArrayBuffer[Try[V]]] = {

        /** apply the same value to whole batch at once instead of to each element individually */
        try Stitch.let(letter)(v())(a.run(ts, ls)).flatMap { rs => tail.run(rs, ls) } catch {
          case e: NonLocalReturnControl[_] => throw new StitchNonLocalReturnControl(e)
          case NonFatal(t) =>
            /** if scoping failed then pass along the exception for all elements of the batch */
            a.run(TryBuffer.fill(ts)(Throw(t)), ls, tail)
        }
      }
    }

    /**
     * same as [[Let]] but for clearing a [[TwitterLocal]] with a [[LetClearer]]
     * @param letClearer a [[LetClearer]] which wraps a Stitch and clears locals around it
     * @param a          underlying Arrow
     */
    private[stitch] case class LetClear[T, U](
      letClearer: LetClearer,
      a: Arrow[T, U])
        extends Arrow[T, U] {

      override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
        tail(Stitch.letClear(letClearer)(a.run(t, l)), l)

      override def run[T2 <: T, V](
        ts: ArrayBuffer[Try[T2]],
        ls: ArrayBuffer[Locals],
        tail: Arrow[U, V]
      ): Stitch[ArrayBuffer[Try[V]]] =
        Stitch.letClear(letClearer)(a.run(ts, ls)).flatMap { rs => tail.run(rs, ls) }
    }

    /**
     * Base class for scoping [[TwitterLocal]]s around an [[Arrow]]
     *
     * @note if the input to this [[Arrow]] is in a [[Throw]] state then the underlying [[letter]] will not scope around `a`
     *       but `a` will be executed with the underlying [[Throw]] passed in.
     *       Similarly, if [[f]] throws, then `a` will be executed with a [[Throw]] of that exception without the
     *       [[letter]] scoping.
     *
     * @param letter a [[Letter]] which wraps a Stitch and `let-scopes` locals around it
     * @param f      transformer function from the current input to the new Local value
     * @param a      underlying Arrow
     * @tparam L     type of the Local
     */
    private[stitch] case class LetWithArg[T, U, L](
      letter: Letter[L],
      f: T => L,
      a: Arrow[T, U])
        extends Arrow[T, U] {

      override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] = {
        t.map(f) match {
          case Return(v) =>
            /** Scope and run the underling */
            tail(Stitch.let(letter)(v)(a.run(t, l)), l)
          case t: Throw[T] =>
            /**
             * Cant scope because we're in a failure state already and [[Letter.let]]
             * can only work inputs that aren't in failure states. Pass the failure to the underlying
             */
            tail(a.run(t, l), l)
        }
      }

      override def run[T2 <: T, V](
        ts: ArrayBuffer[Try[T2]],
        ls: ArrayBuffer[Locals],
        tail: Arrow[U, V]
      ): Stitch[ArrayBuffer[Try[V]]] = {
        val len = ts.length

        if (len == 0)
          return a.run(ts, ls, tail)

        val twitterLocals = ts.map(_.map(f))
        val uniqueLocals = twitterLocals.toSet

        /** Optimal case of all being the same, avoid the excess work of making Arrow compositions and calling into [[ApplyArrow]] */
        if (uniqueLocals.size == 1) {
          uniqueLocals.head match {
            case Return(firstLocal) =>
              /** All the same so we can do this without splitting the batch */
              Stitch.let(letter)(firstLocal)(a.run(ts, ls)).flatMap { rs => tail.run(rs, ls) }
            case t: Throw[_] =>
              /**
               *  [[f]] failed with the same exception for all inputs so we continue execution in an error state without any scoping.
               *  If the input was already a [[Throw]], keep that it, otherwise it becomes the [[Throw]] from evaluating [[f]] to make the new [[TwitterLocal]] value
               */
              a.run(TryBuffer.fill(ts)(t.asInstanceOf[Try[T2]]), ls, tail)
          }
        } else {

          /**
           * Non-optimal case, multiple values to scope the local to for different inputs.
           * Build the [[Arrow]] that scopes each batch using [[Let]],
           * then tuple the [[Arrow]] with the input and defer to [[ApplyArrow]] to do the execution from there
           */

          /**
           * Mapping from new [[TwitterLocal]] value to an underlying [[Arrow]] to run.
           * Inputs are [[Try]]s because we want to run the underlying even if we fail here.
           * [[andThenNoReassoc]] is used because these [[Arrow]]s are thrown away after each use.
           */
          val twitterLocalsMap: scala.collection.immutable.Map[Try[L], Arrow[Try[T], U]] =
            uniqueLocals.toIterator
              .map[(Try[L], Arrow[Try[T], U])] {
                case twitterLocalReturn: Return[L] =>
                  /**
                   * [[lowerFromTry]] to get back to the unwrapped input
                   * [[Let]] that will have the already computed [[TwitterLocal]]
                   * value be the same for all inputs efficiently applying to to them all.
                   */
                  (
                    twitterLocalReturn,
                    Arrow.lowerFromTry.andThenNoReassoc(
                      Let[T, U, L](() => twitterLocalReturn.get(), letter, a))
                  )
                case t: Throw[L] =>
                  /**
                   * if the input was already a [[Throw]], keep it,
                   * otherwise it becomes the exception from evaluating [[f]] to make the new [[TwitterLocal]] value,
                   * then call the underlying [[a]]
                   */
                  (t.asInstanceOf[Try[L]], Arrow.const(t.asInstanceOf[Try[T]]).andThenNoReassoc(a))
              }.toMap

          val rs = ts.asInstanceOf[ArrayBuffer[Try[(Arrow[Try[T], U], Try[T])]]]

          var i = 0
          while (i < len) {
            // make (Arrow, Try[T]) tuples to send to applyArrow
            rs(i) = Return((twitterLocalsMap(twitterLocals(i)), ts(i)))
            i += 1
          }

          /**
           * We have already checked the simple case of all the inputs being the same and
           * defer to [[ApplyArrow]] to handle the complex case of different values for different inputs
           * since it will handle the complexity of executing it as efficiently as possible and remapping inputs
           */
          Arrow.applyArrow[Try[T], U]().run(rs, ls, tail)
        }
      }
    }
  }

  private[stitch] case class GetLocal[L](local: Local[L]) extends Arrow[Any, L] {

    override def run[V](t: Try[Any], l: Locals, tail: Arrow[L, V]): Stitch[V] =
      tail.run(local.extract(l), l)

    override def run[T2 <: Any, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[L, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val len = ts.length

      val rs = ts.asInstanceOf[ArrayBuffer[Try[L]]]

      var i = 0
      while (i < len) {
        rs(i) = local.extract(ls(i))
        i += 1
      }

      tail.run(rs, ls)
    }
  }

  private[stitch] case class ClearLocal[L, T, U](local: Local[L], a: Arrow[T, U])
      extends Arrow[T, U] {

    override def run[V](t: Try[T], l: Locals, tail: Arrow[U, V]): Stitch[V] =
      tail(a.run(t, l - local), l)

    override def run[T2 <: T, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[U, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      a.run(ts, ls.map(_ - local)).flatMap { rs => tail.run(rs, ls) }
    }
  }

  private[stitch] case class LetLocalIndependent[L, In, Out](
    local: Local[L],
    a: Arrow[In, Out],
    fn: In => L)
      extends Arrow[In, Out] {

    override def run[V](t: Try[In], l: Locals, tail: Arrow[Out, V]): Stitch[V] = {
      val innerCtx = t match {
        case Return(v) => l.updated(local, Try(fn(v)))
        case Throw(_) => l
      }
      tail(a.run(t, innerCtx), l)
    }

    override def run[T2 <: In, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[Out, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val len = ts.length

      val innerLocals = new ArrayBuffer[Locals](len)

      var i = 0
      while (i < len) {
        val innerCtx = ts(i) match {
          case Return(v) => ls(i).updated(local, Try(fn(v)))
          case Throw(_) => ls(i)
        }
        innerLocals += innerCtx
        i += 1
      }

      a.run(ts, innerLocals).flatMap { rs => tail.run(rs, ls) }
    }
  }

  private[stitch] case class LetLocalDependent[L, In, Out](
    local: Local[L],
    a: Arrow[In, Out],
    fn: (In, L) => L)
      extends Arrow[In, Out] {

    @inline private def newLocal(t: Try[In], l: Locals): Try[L] =
      (t, local.extract(l)) match {
        case (Return(in), Return(loc)) => Try(fn(in, loc))
        case (_, unchanged) => unchanged
      }

    override def run[V](t: Try[In], l: Locals, tail: Arrow[Out, V]): Stitch[V] = {
      val innerCtx: Locals = l.updated(local, newLocal(t, l))
      tail(a.run(t, innerCtx), l)
    }

    override def run[T2 <: In, V](
      ts: ArrayBuffer[Try[T2]],
      ls: ArrayBuffer[Locals],
      tail: Arrow[Out, V]
    ): Stitch[ArrayBuffer[Try[V]]] = {
      val len = ts.length

      val innerLocals = new ArrayBuffer[Locals](len)

      var i = 0
      while (i < len) {
        val innerCtx: Locals = ls(i).updated(local, newLocal(ts(i), ls(i)))
        innerLocals += innerCtx
        i += 1
      }

      a.run(ts, innerLocals).flatMap { rs => tail.run(rs, ls) }
    }
  }

  val NotFound: Arrow[Any, Nothing] = Arrow.exception(com.twitter.stitch.NotFound)
  private val identityInstance = Identity[Any]()
  private val liftToTryInstance = LiftToTry[Any]()
  private val lowerFromTryInstance = LowerFromTry[Any]()
  private val emptyBuffer = new ArrayBuffer[Any](0)
  private val emptyBufferStitch = Stitch.value(new ArrayBuffer[Any](0))

  /**
   * An `Arrow` that ignores the input and returns the current Arrow.Locals
   */
  def locals: Arrow[Any, Locals] = AllLocals

  /**
   * An `Arrow` that zips the current input value with the current Arrow.Locals
   */
  def zipWithLocals[T]: Arrow[T, (T, Arrow.Locals)] = zipWithArg[T, Locals](AllLocals)

  /**
   * An `Arrow` that takes a function and applies it to a fixed key.
   */
  def application[T, U](t: T): Arrow[T => Try[U], U] = Apply(t)

  /**
   * `Arrow.apply(f)` is equivalent to `Arrow.flatMap(f)`
   */
  def apply[T, U](f: T => Stitch[U]): Arrow[T, U] = FlatMap(f)

  /**
   * `Arrow.value(v)` is equivalent to `Arrow { _ => Stitch.value(v) }`
   */
  def value[T](v: T): Arrow[Any, T] = Const(Return(v))

  /**
   * `Arrow.exception(e)` is equivalent to `Arrow { _ => Stitch.exception(e) }`
   */
  def exception(e: Throwable): Arrow[Any, Nothing] = Const(Throw(e))

  /**
   * `Arrow.const(t)` is equivalent to `Arrow { _ => Stitch.const(t) }`
   */
  def const[T](t: Try[T]): Arrow[Any, T] = Const(t)

  /**
   * `Arrow.identity` is equivalent to `{ x => Stitch.const(x) }`
   */
  def identity[T]: Arrow.Iso[T] = identityInstance.asInstanceOf[Identity[T]]

  /**
   * `Arrow.map(f)` is equivalent to `Arrow.identity.map(f)`
   */
  def map[T, U](f: T => U): Arrow[T, U] = Map(f)

  /**
   * `Arrow.flatMap(f)` is equivalent to `Arrow.identity.flatMap(f)`
   */
  def flatMap[T, U](f: T => Stitch[U]): Arrow[T, U] = FlatMap(f)

  /**
   * `Arrow.flatMapArrow(a)` is equivalent to `Arrow.identity.flatMap(x => a(x))`
   *
   * This is useful because it only passes successful values to the right.
   * See note on composition and error handling above.
   */
  def flatMapArrow[T, U](a: Arrow[T, U]): Arrow[T, U] = {
    // This is actually a special case of Arrow.choose with a single choice.
    // Arrow.choose propagates failures and only returns one of the choices
    // when the computation has not failed.
    Arrow.choose(Choice.otherwise(a))
  }

  /**
   * `Arrow.foldLeftArrow` is a `Seq.foldLeft(init)(f)` where the `init`
   * is tupled with the input `Seq`, and the `f` is an Arrow.
   *
   * This is useful because unlike using `foldLeft` on a Seq, `foldLeftArrow` maintains
   * batching for operations that the Arrow `a` executes.
   *
   * {{{
   *   Seq(0, 1, 2).foldLeft(0){ case (init, curr) => init + curr }
   *   // equivalent to
   *   Arrow.foldLeftArrow(Arrow.map[Int, Int]{ case (init, curr) => init + curr }).traverse((0, Seq(0, 1, 2)))
   * }}}
   */
  def foldLeftArrow[T, U](a: Arrow[(U, T), U]): Arrow[(U, Seq[T]), U] = {
    val foldLeft = {
      Arrow
        .map[(U, Seq[T]), (U, Iterator[T])] {
          case (u, seq) => (u, seq.iterator)
        }.andThen {
          Arrow.recursive[(U, Iterator[T]), U] { self =>
            val callArrow =
              Arrow
                .map[(U, Iterator[T]), (U, T)] {
                  case (u, t) => (u, t.next())
                }.andThen(a)

            val recurse =
              Arrow
                .zipWithArg(callArrow).map {
                  case ((_, it), u) => (u, it)
                }.andThen(self)

            Arrow.choose(
              Choice.when(_._2.hasNext, recurse),
              Choice.otherwise(Arrow.map(_._1))
            )
          }
        }
    }

    val forwardFailureToArrow =
      Arrow.map[Try[(U, Seq[T])], Try[(U, T)]](_.asInstanceOf[Try[(U, T)]]).lowerFromTry.andThen(a)

    Arrow.liftToTry[(U, Seq[T])].andThen {
      Arrow.choose(
        Choice.when(_.isThrow, forwardFailureToArrow),
        Choice.otherwise(Arrow.lowerFromTry.andThen(foldLeft))
      )
    }
  }

  /**
   * `Arrow.handle(f)` is equivalent to `Arrow.identity.handle(f)`
   */
  def handle[T, U >: T](f: PartialFunction[Throwable, U]): Arrow[T, U] = Handle(f)

  /**
   * `Arrow.rescue(f)` is equivalent to `Arrow.identity.rescue(f)`
   */
  def rescue[T, U >: T](f: PartialFunction[Throwable, Stitch[U]]): Arrow[T, U] = Rescue(f)

  /**
   * `Arrow.mapFailure(pf)` is equivalent to `Arrow.identity.mapFailure(pf)`
   */
  def mapFailure[T](pf: PartialFunction[Throwable, Throwable]): Arrow.Iso[T] = MapFailure(pf)

  /**
   * `Arrow.ensure(f)` is equivalent to `Arrow.identity.ensure(f)`
   */
  def ensure[T](f: => Unit): Arrow.Iso[T] = Ensure(() => f)

  /**
   * `Arrow.respond(f)` is equivalent to `Arrow.identity.respond(f)`
   */
  def respond[T](f: Try[T] => Unit): Arrow.Iso[T] = Respond(f)

  /**
   * `Arrow.onSuccess(f)` is equivalent to `Arrow.identity.onSuccess(f)`
   */
  def onSuccess[T](f: T => Unit): Arrow.Iso[T] = OnSuccess(f)

  /**
   * `Arrow.onFailure(f)` is equivalent to `Arrow.identity.onFailure(f)`
   */
  def onFailure[T](f: Throwable => Unit): Arrow.Iso[T] = OnFailure(f)

  /**
   * `Arrow.transform(f)` is equivalent to `Arrow.identity.transform(f)`
   */
  def transform[T, U](f: Try[T] => Stitch[U]): Arrow[T, U] = FlatmapTry(f)

  /**
   * `Arrow.transformTry(f)` is equivalent to `Arrow.identity.liftToTry.map(f).lowerFromTry`
   */
  def transformTry[T, U](f: Try[T] => Try[U]): Arrow[T, U] =
    TransformTry(f, mayHaveEffect = true, mayHandleExceptions = true)

  /**
   * `Arrow.unit` is equivalent to `Arrow.identity.unit`
   */
  def unit[T]: Arrow[T, Unit] = ToUnit

  /**
   * `Arrow.liftToTry` is equivalent to `Arrow.identity.liftToTry`
   */
  def liftToTry[T]: Arrow[T, Try[T]] = liftToTryInstance.asInstanceOf[LiftToTry[T]]

  /**
   * `Arrow.lowerFromTry` is equivalent to `Arrow.identity.lowerFromTry`
   */
  def lowerFromTry[T]: Arrow[Try[T], T] = lowerFromTryInstance.asInstanceOf[LowerFromTry[T]]

  /**
   * `Arrow.liftToOption(f)` is equivalent to `Arrow.identity.liftToOption(f)`
   */
  def liftToOption[T](
    f: PartialFunction[Throwable, Boolean] = Stitch.liftToOptionDefaultFn
  ): Arrow[T, Option[T]] =
    LiftToOption(f)

  /**
   * `Arrow.liftNotFoundToOption` is equivalent to `Arrow.identity.liftNotFoundToOption`
   */
  def liftNotFoundToOption[T]: Arrow[T, Option[T]] =
    LiftToOption(Stitch.liftNotFoundToOptionFn)

  /**
   * `Arrow.lowerFromOption(e)` is equivalent to `Arrow.identity.lowerFromOption(e)`
   */
  def lowerFromOption[T](e: Throwable = com.twitter.stitch.NotFound): Arrow[Option[T], T] =
    LowerFromOption(e)

  /**
   * `Arrow.time(a)` is equivalent to `Arrow { x => Stitch.time(a(x)) }`
   */
  def time[T, U](a: Arrow[T, U]): Arrow[T, (Try[U], Duration)] = Time(a)

  /**
   * `Arrow.within(dur)(a)` is equivalent to `Arrow { x => a(x).within(dur) }`
   */
  def within[T, U](timeout: Duration)(a: Arrow[T, U])(implicit timer: Timer): Arrow[T, U] =
    Within(timeout, timer, a)

  private[stitch] object AndThen {

    /**
     * reassoc is used when building arrows which will be persistent and used multiple times
     * this will recursively optimize the form of the arrow so that it will run more efficiently at the expense
     * of more work during creation time
     */
    def reassoc[T, U, V](f: Arrow[T, U], g: Arrow[U, V]): Arrow[T, V] =
      f match {
        case Identity() => g.asInstanceOf[Arrow[T, V]]

        // arrows are built up by left-associated calls to andThen (e.g. in a.map(f).map(g)),
        // but we want them to be right-associated so that when we consume then // we don't
        // need to reassociate to find the head, so we reassociate here.
        case AndThen(d, e: Arrow[Any, U] @unchecked) => d.andThen(e.andThen(g))

        // We can execute some computations on Const arrows at composition
        // time (constant folding.)
        //
        // It's not safe to do this for arrows that call user-defined code,
        // since that code may have effects, and we want to make sure that
        // those effects are not optimized away. For instance, the code may
        // increment a counter, or access the system clock:
        //
        // >>> val count = Arrow.map[Any, Unit](_ => incrCounter())
        // >>> val currentTime = Arrow.map[Any, Time](_ => Time.now)
        //
        // We also cannot constant-fold for operations that return different
        // results for different `Throw` inputs, since the exception may be
        // different at evaluation time then at constant-folding time. For
        // instance, consider these two expressions:
        //
        // object E1 extends Exception
        // object E2 extends Exception
        // >>> val arr = Arrow.exception(E2).andThen(Arrow.liftToTry)
        // >>> val arr2 = Arrow.exception(E1).andThen(arr)
        //
        // `arr` for any input should yield Return(Throw(E2)), and `arr2`
        // should yield Return(Throw(E1)), but if `arr` was constant-folded,
        // `arr2` would instead yield Throw(E1) (that is, the liftToTry
        // would not happen.)
        case Const(t) =>
          g match {
            case g: PureArrow[U @unchecked, V @unchecked]
                if !g.mayHaveEffect && !g.mayHandleExceptions =>
              Const(g.applyPure(t))

            case AndThen(g: PureArrow[U @unchecked, Any @unchecked], h: Arrow[Any, V] @unchecked)
                if !g.mayHaveEffect && !g.mayHandleExceptions =>
              Const(g.applyPure(t)).andThen(h)

            // We don't need to handle AndThen(AndThen(a, b), c) because
            // that's eliminated by the reassociation above.
            case _ =>
              AndThen(f, g)
          }

        // with the batch interface for arrows we iterate over all input arguments for each arrow
        // this is unnecessary in the case for pure arrows that are andThen'ed together.
        // Here we take the case of pure arrows that are andThen'ed and apply the
        // pure transformation inline which reduces iterations over the input when
        // working in batches
        case f: PureArrow[T, U]
            if g.isInstanceOf[PureArrow[U, V]] && !g.isInstanceOf[Identity[_]] =>
          val gg = g.asInstanceOf[PureArrow[U, V]]
          TransformTry[T, V](
            (t: Try[T]) => gg.applyPure(f.applyPure(t)),
            f.mayHaveEffect || gg.mayHaveEffect,
            f.mayHandleExceptions || gg.mayHandleExceptions
          )

        case _ =>
          g match {
            case Identity() => f.asInstanceOf[Arrow[T, V]]
            case _ => AndThen(f, g)
          }
      }

    /**
     * noReassoc is used when building arrows which are not going to be reused,
     * where the cost of recursive build time optimization is not worth it for a single execution
     */
    def noReassoc[T, U, V](f: Arrow[T, U], g: Arrow[U, V]): Arrow[T, V] =
      f match {
        case Identity() => g.asInstanceOf[Arrow[T, V]]
        case _ =>
          g match {
            case Identity() => f.asInstanceOf[Arrow[T, V]]
            case _ => AndThen(f, g)
          }
      }
  }

  /**
   * `Arrow.andThen(a, b)` is equivalent to `{ x => b(a(x)) }`
   *
   * Both successful values and errors are passed from a to b.
   * See note on composition and error handling above.
   */
  def andThen[T, U, V](f: Arrow[T, U], g: Arrow[U, V]): Arrow[T, V] =
    AndThen.reassoc(f, g)

  /**
   * `Arrow.async(a)` is equivalent to `{ x => Stitch.async(a(x)) }`
   */
  def async[T, U](arr: Arrow[T, U]): Arrow[T, Unit] = {
    Arrow.zipWithLocals.flatMap[Unit] {
      case (t, locals) =>
        Stitch.async(arr.apply(Return(t), locals))
    }
  }

  /**
   * `Arrow.call(g)` is equivalent to `Arrow { x => Stitch.call(x, g) }`
   */
  def call[T, U](g: ArrowGroup[T, U]): Arrow[T, U] = Call(g)

  /**
   * `Arrow.call(gf)` is equivalent to `Arrow { case (x, c) => Stitch.call(x, gf(c)) }`
   */
  def call[C, T, U](gf: C => ArrowGroup[T, U]): Arrow[(T, C), U] = CallWithContext(gf)

  /**
   * `Arrow.callFuture(f)` is equivalent to `Arrow { x => Stitch.callFuture(f(x)) }`
   */
  def callFuture[T, U](f: T => Future[U]): Arrow[T, U] =
    Arrow.flatMap[T, U] { x => Stitch.callFuture(f(x)) }

  /** `Arrow.sleep(d)` is equivalent to `Arrow { x => Stitch.sleep(d); x }` */
  def sleep[T](d: Duration)(implicit timer: Timer): Iso[T] =
    zipWithArg[T, Unit](flatMap(_ => Stitch.sleep(d)(timer))).map { case (arg, _) => arg }

  /**
   * `Arrow.join(a1, a2)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x)) }`
   */
  def join[A, B, C](a1: Arrow[A, B], a2: Arrow[A, C]): Arrow[A, (B, C)] =
    joinMap(a1, a2)((_, _))

  /**
   * `Arrow.joinMap(a1, a2)(k)` is equivalent to
   * `Arrow.join(a1, a2).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, Z](a1: Arrow[A, B], a2: Arrow[A, C])(k: (B, C) => Z): Arrow[A, Z] =
    (a1, a2) match {
      case (Const(Throw(e)), _) => exception(e)
      case (_, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _) =>
        a2.map(v2 => k(v1, v2))
      case (_, Const(Return(v2))) =>
        a1.map(v1 => k(v1, v2))
      case _ =>
        JoinMap2(a1, a2, k)
    }

  /**
   * `Arrow.join(a1, a2, a3)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x)) }`
   */
  def join[A, B, C, D](a1: Arrow[A, B], a2: Arrow[A, C], a3: Arrow[A, D]): Arrow[A, (B, C, D)] =
    joinMap(a1, a2, a3)((_, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D]
  )(
    k: (B, C, D) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3) match {
      case (Const(Throw(e)), _, _) => exception(e)
      case (_, Const(Throw(e)), _) => exception(e)
      case (_, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _) =>
        joinMap(a2, a3) { (v2, v3) => k(v1, v2, v3) }
      case (_, Const(Return(v2)), _) =>
        joinMap(a1, a3) { (v1, v3) => k(v1, v2, v3) }
      case (_, _, Const(Return(v3))) =>
        joinMap(a1, a2) { (v1, v2) => k(v1, v2, v3) }
      case _ =>
        JoinMap3(a1, a2, a3, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x)) }`
   */
  def join[A, B, C, D, E](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E]
  ): Arrow[A, (B, C, D, E)] =
    joinMap(a1, a2, a3, a4)((_, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E]
  )(
    k: (B, C, D, E) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4) match {
      case (Const(Throw(e)), _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _) => exception(e)
      case (_, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _) =>
        joinMap(a2, a3, a4) { (v2, v3, v4) => k(v1, v2, v3, v4) }
      case (_, Const(Return(v2)), _, _) =>
        joinMap(a1, a3, a4) { (v1, v3, v4) => k(v1, v2, v3, v4) }
      case (_, _, Const(Return(v3)), _) =>
        joinMap(a1, a2, a4) { (v1, v2, v4) => k(v1, v2, v3, v4) }
      case (_, _, _, Const(Return(v4))) =>
        joinMap(a1, a2, a3) { (v1, v2, v3) => k(v1, v2, v3, v4) }
      case _ =>
        JoinMap4(a1, a2, a3, a4, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x)) }`
   */
  def join[A, B, C, D, E, F](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F]
  ): Arrow[A, (B, C, D, E, F)] =
    joinMap(a1, a2, a3, a4, a5)((_, _, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F]
  )(
    k: (B, C, D, E, F) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5) match {
      case (Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _) =>
        joinMap(a2, a3, a4, a5) { (v2, v3, v4, v5) => k(v1, v2, v3, v4, v5) }
      case (_, Const(Return(v2)), _, _, _) =>
        joinMap(a1, a3, a4, a5) { (v1, v3, v4, v5) => k(v1, v2, v3, v4, v5) }
      case (_, _, Const(Return(v3)), _, _) =>
        joinMap(a1, a2, a4, a5) { (v1, v2, v4, v5) => k(v1, v2, v3, v4, v5) }
      case (_, _, _, Const(Return(v4)), _) =>
        joinMap(a1, a2, a3, a5) { (v1, v2, v3, v5) => k(v1, v2, v3, v4, v5) }
      case (_, _, _, _, Const(Return(v5))) =>
        joinMap(a1, a2, a3, a4) { (v1, v2, v3, v4) => k(v1, v2, v3, v4, v5) }
      case _ =>
        JoinMap5(a1, a2, a3, a4, a5, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x)) }`
   */
  def join[A, B, C, D, E, F, G](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G]
  ): Arrow[A, (B, C, D, E, F, G)] =
    joinMap(a1, a2, a3, a4, a5, a6)((_, _, _, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G]
  )(
    k: (B, C, D, E, F, G) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6) match {
      case (Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6) { (v2, v3, v4, v5, v6) => k(v1, v2, v3, v4, v5, v6) }
      case (_, Const(Return(v2)), _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6) { (v1, v3, v4, v5, v6) => k(v1, v2, v3, v4, v5, v6) }
      case (_, _, Const(Return(v3)), _, _, _) =>
        joinMap(a1, a2, a4, a5, a6) { (v1, v2, v4, v5, v6) => k(v1, v2, v3, v4, v5, v6) }
      case (_, _, _, Const(Return(v4)), _, _) =>
        joinMap(a1, a2, a3, a5, a6) { (v1, v2, v3, v5, v6) => k(v1, v2, v3, v4, v5, v6) }
      case (_, _, _, _, Const(Return(v5)), _) =>
        joinMap(a1, a2, a3, a4, a6) { (v1, v2, v3, v4, v6) => k(v1, v2, v3, v4, v5, v6) }
      case (_, _, _, _, _, Const(Return(v6))) =>
        joinMap(a1, a2, a3, a4, a5) { (v1, v2, v3, v4, v5) => k(v1, v2, v3, v4, v5, v6) }
      case _ =>
        JoinMap6(a1, a2, a3, a4, a5, a6, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x), a7(x)) }`
   */
  def join[A, B, C, D, E, F, G, H](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H]
  ): Arrow[A, (B, C, D, E, F, G, H)] =
    joinMap(a1, a2, a3, a4, a5, a6, a7)((_, _, _, _, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6, a7)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, H, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H]
  )(
    k: (B, C, D, E, F, G, H) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6, a7) match {
      case (Const(Throw(e)), _, _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6, a7) { (v2, v3, v4, v5, v6, v7) =>
          k(v1, v2, v3, v4, v5, v6, v7)
        }
      case (_, Const(Return(v2)), _, _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6, a7) { (v1, v3, v4, v5, v6, v7) =>
          k(v1, v2, v3, v4, v5, v6, v7)
        }
      case (_, _, Const(Return(v3)), _, _, _, _) =>
        joinMap(a1, a2, a4, a5, a6, a7) { (v1, v2, v4, v5, v6, v7) =>
          k(v1, v2, v3, v4, v5, v6, v7)
        }
      case (_, _, _, Const(Return(v4)), _, _, _) =>
        joinMap(a1, a2, a3, a5, a6, a7) { (v1, v2, v3, v5, v6, v7) =>
          k(v1, v2, v3, v4, v5, v6, v7)
        }
      case (_, _, _, _, Const(Return(v5)), _, _) =>
        joinMap(a1, a2, a3, a4, a6, a7) { (v1, v2, v3, v4, v6, v7) =>
          k(v1, v2, v3, v4, v5, v6, v7)
        }
      case (_, _, _, _, _, Const(Return(v6)), _) =>
        joinMap(a1, a2, a3, a4, a5, a7) { (v1, v2, v3, v4, v5, v7) =>
          k(v1, v2, v3, v4, v5, v6, v7)
        }
      case (_, _, _, _, _, _, Const(Return(v7))) =>
        joinMap(a1, a2, a3, a4, a5, a6) { (v1, v2, v3, v4, v5, v6) =>
          k(v1, v2, v3, v4, v5, v6, v7)
        }
      case _ =>
        JoinMap7(a1, a2, a3, a4, a5, a6, a7, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x), a7(x), a8(x)) }`
   */
  def join[A, B, C, D, E, F, G, H, I](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I]
  ): Arrow[A, (B, C, D, E, F, G, H, I)] =
    joinMap(a1, a2, a3, a4, a5, a6, a7, a8)((_, _, _, _, _, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6, a7, a8)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I]
  )(
    k: (B, C, D, E, F, G, H, I) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6, a7, a8) match {
      case (Const(Throw(e)), _, _, _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6, a7, a8) { (v2, v3, v4, v5, v6, v7, v8) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8)
        }
      case (_, Const(Return(v2)), _, _, _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6, a7, a8) { (v1, v3, v4, v5, v6, v7, v8) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8)
        }
      case (_, _, Const(Return(v3)), _, _, _, _, _) =>
        joinMap(a1, a2, a4, a5, a6, a7, a8) { (v1, v2, v4, v5, v6, v7, v8) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8)
        }
      case (_, _, _, Const(Return(v4)), _, _, _, _) =>
        joinMap(a1, a2, a3, a5, a6, a7, a8) { (v1, v2, v3, v5, v6, v7, v8) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8)
        }
      case (_, _, _, _, Const(Return(v5)), _, _, _) =>
        joinMap(a1, a2, a3, a4, a6, a7, a8) { (v1, v2, v3, v4, v6, v7, v8) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8)
        }
      case (_, _, _, _, _, Const(Return(v6)), _, _) =>
        joinMap(a1, a2, a3, a4, a5, a7, a8) { (v1, v2, v3, v4, v5, v7, v8) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8)
        }
      case (_, _, _, _, _, _, Const(Return(v7)), _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a8) { (v1, v2, v3, v4, v5, v6, v8) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8)
        }
      case (_, _, _, _, _, _, _, Const(Return(v8))) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7) { (v1, v2, v3, v4, v5, v6, v7) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8)
        }
      case _ =>
        JoinMap8(a1, a2, a3, a4, a5, a6, a7, a8, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x), a7(x), a8(x), a9(x)) }`
   */
  def join[A, B, C, D, E, F, G, H, I, J](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J]
  ): Arrow[A, (B, C, D, E, F, G, H, I, J)] =
    joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9)((_, _, _, _, _, _, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J]
  )(
    k: (B, C, D, E, F, G, H, I, J) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6, a7, a8, a9) match {
      case (Const(Throw(e)), _, _, _, _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _, _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, _, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6, a7, a8, a9) { (v2, v3, v4, v5, v6, v7, v8, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case (_, Const(Return(v2)), _, _, _, _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6, a7, a8, a9) { (v1, v3, v4, v5, v6, v7, v8, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case (_, _, Const(Return(v3)), _, _, _, _, _, _) =>
        joinMap(a1, a2, a4, a5, a6, a7, a8, a9) { (v1, v2, v4, v5, v6, v7, v8, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case (_, _, _, Const(Return(v4)), _, _, _, _, _) =>
        joinMap(a1, a2, a3, a5, a6, a7, a8, a9) { (v1, v2, v3, v5, v6, v7, v8, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case (_, _, _, _, Const(Return(v5)), _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a6, a7, a8, a9) { (v1, v2, v3, v4, v6, v7, v8, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case (_, _, _, _, _, Const(Return(v6)), _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a7, a8, a9) { (v1, v2, v3, v4, v5, v7, v8, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case (_, _, _, _, _, _, Const(Return(v7)), _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a8, a9) { (v1, v2, v3, v4, v5, v6, v8, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case (_, _, _, _, _, _, _, Const(Return(v8)), _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a9) { (v1, v2, v3, v4, v5, v6, v7, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case (_, _, _, _, _, _, _, _, Const(Return(v9))) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8) { (v1, v2, v3, v4, v5, v6, v7, v8) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9)
        }
      case _ =>
        JoinMap9(a1, a2, a3, a4, a5, a6, a7, a8, a9, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x), a7(x), a8(x), a9(x), a10(x)) }`
   */
  def join[A, B, C, D, E, F, G, H, I, J, K](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K]
  ): Arrow[A, (B, C, D, E, F, G, H, I, J, K)] =
    joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)((_, _, _, _, _, _, _, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K]
  )(
    k: (B, C, D, E, F, G, H, I, J, K) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) match {
      case (Const(Throw(e)), _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6, a7, a8, a9, a10) { (v2, v3, v4, v5, v6, v7, v8, v9, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, Const(Return(v2)), _, _, _, _, _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6, a7, a8, a9, a10) { (v1, v3, v4, v5, v6, v7, v8, v9, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, _, Const(Return(v3)), _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a4, a5, a6, a7, a8, a9, a10) { (v1, v2, v4, v5, v6, v7, v8, v9, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, _, _, Const(Return(v4)), _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a5, a6, a7, a8, a9, a10) { (v1, v2, v3, v5, v6, v7, v8, v9, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, _, _, _, Const(Return(v5)), _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a6, a7, a8, a9, a10) { (v1, v2, v3, v4, v6, v7, v8, v9, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, _, _, _, _, Const(Return(v6)), _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a7, a8, a9, a10) { (v1, v2, v3, v4, v5, v7, v8, v9, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, _, _, _, _, _, Const(Return(v7)), _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a8, a9, a10) { (v1, v2, v3, v4, v5, v6, v8, v9, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, _, _, _, _, _, _, Const(Return(v8)), _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a9, a10) { (v1, v2, v3, v4, v5, v6, v7, v9, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, _, _, _, _, _, _, _, Const(Return(v9)), _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a10) { (v1, v2, v3, v4, v5, v6, v7, v8, v10) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case (_, _, _, _, _, _, _, _, _, Const(Return(v10))) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9) { (v1, v2, v3, v4, v5, v6, v7, v8, v9) =>
          k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10)
        }
      case _ =>
        JoinMap10(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x), a7(x), a8(x), a9(x), a10(x), a11(x)) }`
   */
  def join[A, B, C, D, E, F, G, H, I, J, K, L](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L]
  ): Arrow[A, (B, C, D, E, F, G, H, I, J, K, L)] =
    joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)((_, _, _, _, _, _, _, _, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L]
  )(
    k: (B, C, D, E, F, G, H, I, J, K, L) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) match {
      case (Const(Throw(e)), _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) {
          (v2, v3, v4, v5, v6, v7, v8, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, Const(Return(v2)), _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6, a7, a8, a9, a10, a11) {
          (v1, v3, v4, v5, v6, v7, v8, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, Const(Return(v3)), _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a4, a5, a6, a7, a8, a9, a10, a11) {
          (v1, v2, v4, v5, v6, v7, v8, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, _, Const(Return(v4)), _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a5, a6, a7, a8, a9, a10, a11) {
          (v1, v2, v3, v5, v6, v7, v8, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, _, _, Const(Return(v5)), _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a6, a7, a8, a9, a10, a11) {
          (v1, v2, v3, v4, v6, v7, v8, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, _, _, _, Const(Return(v6)), _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a7, a8, a9, a10, a11) {
          (v1, v2, v3, v4, v5, v7, v8, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, _, _, _, _, Const(Return(v7)), _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a8, a9, a10, a11) {
          (v1, v2, v3, v4, v5, v6, v8, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, _, _, _, _, _, Const(Return(v8)), _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a9, a10, a11) {
          (v1, v2, v3, v4, v5, v6, v7, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, _, _, _, _, _, _, Const(Return(v9)), _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a10, a11) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, _, _, _, _, _, _, _, Const(Return(v10)), _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a11) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case (_, _, _, _, _, _, _, _, _, _, Const(Return(v11))) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
        }
      case _ =>
        JoinMap11(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x), a7(x), a8(x), a9(x), a10(x), a11(x), a12(x)) }`
   */
  def join[A, B, C, D, E, F, G, H, I, J, K, L, M](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M]
  ): Arrow[A, (B, C, D, E, F, G, H, I, J, K, L, M)] =
    joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)((_, _, _, _, _, _, _, _, _, _, _, _))

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M]
  )(
    k: (B, C, D, E, F, G, H, I, J, K, L, M) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) match {
      case (Const(Throw(e)), _, _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e)), _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) {
          (v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, Const(Return(v2)), _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) {
          (v1, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, Const(Return(v3)), _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a4, a5, a6, a7, a8, a9, a10, a11, a12) {
          (v1, v2, v4, v5, v6, v7, v8, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, Const(Return(v4)), _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a5, a6, a7, a8, a9, a10, a11, a12) {
          (v1, v2, v3, v5, v6, v7, v8, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, _, Const(Return(v5)), _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a6, a7, a8, a9, a10, a11, a12) {
          (v1, v2, v3, v4, v6, v7, v8, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, _, _, Const(Return(v6)), _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a7, a8, a9, a10, a11, a12) {
          (v1, v2, v3, v4, v5, v7, v8, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, _, _, _, Const(Return(v7)), _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a8, a9, a10, a11, a12) {
          (v1, v2, v3, v4, v5, v6, v8, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, _, _, _, _, Const(Return(v8)), _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a9, a10, a11, a12) {
          (v1, v2, v3, v4, v5, v6, v7, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, _, _, _, _, _, Const(Return(v9)), _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a10, a11, a12) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, _, _, _, _, _, _, Const(Return(v10)), _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a11, a12) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, _, _, _, _, _, _, _, Const(Return(v11)), _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a12) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case (_, _, _, _, _, _, _, _, _, _, _, Const(Return(v12))) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12)
        }
      case _ =>
        JoinMap12(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x), a7(x), a8(x), a9(x), a10(x), a11(x), a12(x), a13(x)) }`
   */
  def join[A, B, C, D, E, F, G, H, I, J, K, L, M, N](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M],
    a13: Arrow[A, N]
  ): Arrow[A, (B, C, D, E, F, G, H, I, J, K, L, M, N)] =
    joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13)(
      (_, _, _, _, _, _, _, _, _, _, _, _, _)
    )

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, N, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M],
    a13: Arrow[A, N]
  )(
    k: (B, C, D, E, F, G, H, I, J, K, L, M, N) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) match {
      case (Const(Throw(e)), _, _, _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e)), _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, Const(Throw(e)), _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) {
          (v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, Const(Return(v2)), _, _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) {
          (v1, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, Const(Return(v3)), _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) {
          (v1, v2, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, Const(Return(v4)), _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a5, a6, a7, a8, a9, a10, a11, a12, a13) {
          (v1, v2, v3, v5, v6, v7, v8, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, Const(Return(v5)), _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a6, a7, a8, a9, a10, a11, a12, a13) {
          (v1, v2, v3, v4, v6, v7, v8, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, _, Const(Return(v6)), _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a7, a8, a9, a10, a11, a12, a13) {
          (v1, v2, v3, v4, v5, v7, v8, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, _, _, Const(Return(v7)), _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a8, a9, a10, a11, a12, a13) {
          (v1, v2, v3, v4, v5, v6, v8, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, _, _, _, Const(Return(v8)), _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a9, a10, a11, a12, a13) {
          (v1, v2, v3, v4, v5, v6, v7, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, _, _, _, _, Const(Return(v9)), _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a10, a11, a12, a13) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, _, _, _, _, _, Const(Return(v10)), _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a11, a12, a13) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, _, _, _, _, _, _, Const(Return(v11)), _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a12, a13) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, _, _, _, _, _, _, _, Const(Return(v12)), _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a13) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case (_, _, _, _, _, _, _, _, _, _, _, _, Const(Return(v13))) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13)
        }
      case _ =>
        JoinMap13(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, k)
    }

  /**
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14)` is equivalent to
   * `{ x => Stitch.join(a1(x), a2(x), a3(x), a4(x), a5(x), a6(x), a7(x), a8(x), a9(x), a10(x), a11(x), a12(x), a13(x), a14(x)) }`
   */
  def join[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M],
    a13: Arrow[A, N],
    a14: Arrow[A, O]
  ): Arrow[A, (B, C, D, E, F, G, H, I, J, K, L, M, N, O)] =
    joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14)(
      (_, _, _, _, _, _, _, _, _, _, _, _, _, _)
    )

  /**
   * `Arrow.joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14)(k)` is equivalent to
   * `Arrow.join(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14).map(k.tupled)`,
   * but avoids allocating an intermediate tuple.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, Z](
    a1: Arrow[A, B],
    a2: Arrow[A, C],
    a3: Arrow[A, D],
    a4: Arrow[A, E],
    a5: Arrow[A, F],
    a6: Arrow[A, G],
    a7: Arrow[A, H],
    a8: Arrow[A, I],
    a9: Arrow[A, J],
    a10: Arrow[A, K],
    a11: Arrow[A, L],
    a12: Arrow[A, M],
    a13: Arrow[A, N],
    a14: Arrow[A, O]
  )(
    k: (B, C, D, E, F, G, H, I, J, K, L, M, N, O) => Z
  ): Arrow[A, Z] =
    (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) match {
      case (Const(Throw(e)), _, _, _, _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, Const(Throw(e)), _, _, _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, Const(Throw(e)), _, _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, Const(Throw(e)), _, _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, Const(Throw(e)), _, _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, Const(Throw(e)), _, _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, Const(Throw(e)), _, _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, Const(Throw(e)), _, _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, Const(Throw(e)), _, _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, Const(Throw(e)), _, _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, Const(Throw(e)), _, _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, _, Const(Throw(e)), _, _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, _, _, Const(Throw(e)), _) => exception(e)
      case (_, _, _, _, _, _, _, _, _, _, _, _, _, Const(Throw(e))) => exception(e)
      case (Const(Return(v1)), _, _, _, _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) {
          (v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, Const(Return(v2)), _, _, _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) {
          (v1, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, Const(Return(v3)), _, _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) {
          (v1, v2, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, Const(Return(v4)), _, _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) {
          (v1, v2, v3, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, Const(Return(v5)), _, _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a6, a7, a8, a9, a10, a11, a12, a13, a14) {
          (v1, v2, v3, v4, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, Const(Return(v6)), _, _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a7, a8, a9, a10, a11, a12, a13, a14) {
          (v1, v2, v3, v4, v5, v7, v8, v9, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, _, Const(Return(v7)), _, _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a8, a9, a10, a11, a12, a13, a14) {
          (v1, v2, v3, v4, v5, v6, v8, v9, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, _, _, Const(Return(v8)), _, _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a9, a10, a11, a12, a13, a14) {
          (v1, v2, v3, v4, v5, v6, v7, v9, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, _, _, _, Const(Return(v9)), _, _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a10, a11, a12, a13, a14) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v10, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, _, _, _, _, Const(Return(v10)), _, _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a11, a12, a13, a14) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v11, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, _, _, _, _, _, Const(Return(v11)), _, _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a12, a13, a14) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v12, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, _, _, _, _, _, _, Const(Return(v12)), _, _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a13, a14) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v13, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, _, _, _, _, _, _, _, Const(Return(v13)), _) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a14) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v14) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case (_, _, _, _, _, _, _, _, _, _, _, _, _, Const(Return(v14))) =>
        joinMap(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) {
          (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) =>
            k(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14)
        }
      case _ =>
        JoinMap14(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, k)
    }

  /**
   * `Arrow.tuple(a1, a2)` is equivalent to
   * `{ case (x, y) => Stitch.join(a1(x), a2(y)) }`
   */
  def tuple[A, B, C, D](a1: Arrow[A, B], a2: Arrow[C, D]): Arrow[(A, C), (B, D)] =
    join(a1.contramap(_._1), a2.contramap[(A, C)](_._2))

  /**
   * `Arrow.zipWithArg` is equivalent to `{ x => a(x).map((x, _)) }`
   */
  def zipWithArg[T, U](f: Arrow[T, U]): Arrow[T, (T, U)] =
    Arrow.join(Arrow.identity[T], f)

  /**
   * let-scope a [[TwitterLocal]] around the contained [[Arrow]] `a` based on a by-name `local`
   * When running this [[Arrow]], `local` is evaluated once per call to [[Arrow.run]], meaning it's evaluated only once for each batch.
   *
   * This is equivalent to `local.let(local)(a)` but correctly applies the `local` to the `a`'s execution.
   *
   * @note [[let]] applies the [[Local]] __only non-batched operations__,
   *       [[call]] and [[Group]]s do not have access to [[Local]]s set with [[let]].
   *       To access a [[Local]] value within a [[call]], or [[Group]], access the value
   *       before making your [[call]] and add it to your input.
   *       For instance,`a.map(v => (v, local())).call(...)`
   *
   * @param local the [[TwitterLocal]] that should be let-scoped around `a`
   * @param value the value of the `local` within the execution of `a`
   * @param a the underlying [[Arrow]] which will have the `local` set during it's evaluation
   * @tparam L the type of the [[TwitterLocal]] and it's value
   */
  def let[T, U, L](local: TwitterLocal[L])(value: => L)(a: Arrow[T, U]): Arrow[T, U] =
    let(LocalLetter(local))(value)(a)

  /**
   * let-scope a [[TwitterLocal]] around the contained [[Arrow]] `a` based on the input
   *
   * This is equivalent to `local.let(local)(a)` but correctly applies the `local` to the `a`'s execution.
   *
   * If your `update` returns a constant value, [[let]] is a more efficient.
   *
   * @note [[letWithArg]] applies the [[Local]] __only non-batched operations__,
   *       [[call]] and [[Group]]s do not have access to [[Local]]s set with [[letWithArg]].
   *       To access a [[Local]] value within a [[call]], or [[Group]], access the value
   *       before making your [[call]] and add it to your input.
   *       For instance,`a.map(v => (v, local())).call(...)`
   *
   * @note if the input to this [[Arrow]] is in a [[Throw]] state then the underlying [[TwitterLocal]] will not be scoped around `a`
   *       but `a` will be executed with the underlying [[Throw]] passed in.
   *       Similarly, if `update` throws, then `a` will be executed with a [[Throw]] of that exception without the
   *       [[TwitterLocal]] being scoped.
   *
   * @param local the [[TwitterLocal]] that should be let-scoped around `a`
   * @param update a function which will be evaluated to determine the value of the `local` within the execution of `a`
   * @param a the underlying [[Arrow]] which will have the `local` set during it's evaluation
   * @tparam L the type of the [[TwitterLocal]] and it's value
   */
  def letWithArg[T, U, L](local: TwitterLocal[L])(update: T => L)(a: Arrow[T, U]): Arrow[T, U] =
    letWithArg(LocalLetter(local))(update)(a)

  /**
   * clear a [[TwitterLocal]] around the contained [[Arrow]] `a`
   *
   * This is equivalent to `local.letClear(a)` but correctly clears the `local` for `a`'s execution.
   *
   * @param local the [[TwitterLocal]] that should be let-scoped around `a`
   * @param a the underlying [[Arrow]] which will have the `local` set during it's evaluation
   */
  def letClear[T, U](local: TwitterLocal[_])(a: => Arrow[T, U]): Arrow[T, U] =
    letClear(LocalLetClearer(local))(a)

  /**
   * let-scope a [[TwitterLocal]] around the contained [[Arrow]] `a` based on a by-name `value`
   * When running this [[Arrow]], `local` is evaluated once per call to [[Arrow.run]], meaning it's evaluated only once for each batch.
   *
   * This is equivalent to `Letter.let(value)(a)` but correctly applies the [[Letter.let]] to the `a`'s execution.
   *
   * This is used for let-scoping [[TwitterLocal]]s which are private to a class and can only be accessed with their own
   * let method instead of giving access to the underlying [[TwitterLocal]]
   *
   * @note [[let]] applies the [[Local]] __only non-batched operations__,
   *       [[call]] and [[Group]]s do not have access to [[Local]]s set with [[let]].
   *       To access a [[Local]] value within a [[call]], or [[Group]], access the value
   *       before making your [[call]] and add it to your input.
   *       For instance,`a.map(v => (v, local())).call(...)`
   *
   * @param letter [[Letter]] that let-scopes around a block of code
   * @param a the underlying [[Arrow]] which will have the `local` set during it's evaluation
   * @tparam L the type of the [[TwitterLocal]] and it's value
   */
  def let[T, U, L](letter: Letter[L])(value: => L)(a: Arrow[T, U]): Arrow[T, U] =
    TwitterLocals.Let(() => value, letter, a)

  /**
   * let-scope a [[TwitterLocal]] around the contained [[Arrow]] `a` based on the input
   *
   * This is equivalent to `Letter.let(value)(a)` where `value` changes for each argument in the batch
   * but correctly applies the [[Letter.let]] to the `a`'s execution.
   *
   * If your `update` returns a constant value, [[let]] is a more efficient.
   *
   * This is used for let-scoping [[TwitterLocal]]s which are private to a class and can only be accessed with their own
   * let method instead of giving access to the underlying [[TwitterLocal]]
   *
   * @note [[letWithArg]] applies the [[Local]] __only non-batched operations__,
   *       [[call]] and [[Group]]s do not have access to [[Local]]s set with [[letWithArg]].
   *       To access a [[Local]] value within a [[call]], or [[Group]], access the value
   *       before making your [[call]] and add it to your input.
   *       For instance,`a.map(v => (v, local())).call(...)`
   *
   * @note if the input to this [[Arrow]] is in a [[Throw]] state then the underlying [[TwitterLocal]] will not be scoped around `a`
   *       but `a` will be executed with the underlying [[Throw]] passed in.
   *       Similarly, if `update` throws, then `a` will be executed with a [[Throw]] of that exception without the
   *       [[TwitterLocal]] being scoped.
   *
   * @param letter [[Letter]] that let-scopes around a block of code
   * @param update a function which will be evaluated to determine the value of the `local` within the execution of `a`
   * @param a the underlying [[Arrow]] which will have the `local` set during it's evaluation
   * @tparam L the type of the [[TwitterLocal]] and it's value
   */
  def letWithArg[T, U, L](letter: Letter[L])(update: T => L)(a: Arrow[T, U]): Arrow[T, U] =
    TwitterLocals.LetWithArg[T, U, L](letter, update, a)

  /**
   * clear a [[TwitterLocal]] around the contained [[Arrow]] `a`
   *
   * This is equivalent to `LetClearer.letClear(a)` but correctly clears the `local` for `a`'s execution.
   *
   * This is used for let-scoping [[TwitterLocal]]s which are private to a class and can only be accessed with their own
   * let method instead of giving access to the underlying [[TwitterLocal]]
   *
   * @param letClearer the [[TwitterLocal]] that should be let-scoped around `a`
   * @param a the underlying [[Arrow]] which will have the `local` set during it's evaluation
   */
  def letClear[T, U](letClearer: LetClearer)(a: Arrow[T, U]): Arrow[T, U] =
    TwitterLocals.LetClear(letClearer, a)

  /**
   * Creates a new `Arrow[T, U]` that dispatches input to one of two sub-arrows
   * based on the result of a predicate.  This is equivalent to
   * `{ x => if (cond(x)) a1(x) else a2(x) }`.
   */
  def ifelse[T, U](cond: T => Boolean, a1: Arrow[T, U], a2: Arrow[T, U]): Arrow[T, U] =
    choose[T, U](
      (cond, a1),
      (_ => true, a2)
    )

  /**
   * Creates a new `Arrow[T, U]` that dispatches input to the first
   * sub-arrow for which the corresponding predicate returns true.  If
   * no predicate returns true for the input, that will result in a
   * `MatchError` for that input.
   *
   * Behavior is undefined if a predicate throws an exception.
   */
  def choose[T, U](choices: Choice[T, U]*): Arrow[T, U] =
    Choose(choices)

  /**
   * Encapsulates a choice for the `choose` method.  If the given predicate returns true
   * for a given input, then the corresponding arrow is invoked for that input.
   */
  type Choice[-T, +U] = (T => Boolean, Arrow[T, U])

  object Choice {

    /**
     * Builds a `Choice` that dispatches to the given `Arrow` when the given predicate is
     * true for the input.
     */
    def when[T, U](cond: T => Boolean, arrow: Arrow[T, U]): Choice[T, U] =
      (cond, arrow)

    /**
     * Builds a `Choice` that dispatches to the given `Arrow` when the given `PartialFunction`
     * is defined at the input.
     */
    def ifDefinedAt[T, S, U](pf: PartialFunction[T, S], a: Arrow[S, U]): Choice[T, U] =
      // we make this transformation to expose constant arrows so
      // Choose can optimize them. Ordinarily it's not valid to
      // transform Arrow.map(f).andThen(Arrow.Const(t)) to
      // Arrow.Const(t), because f may throw an exception; but here
      // pf may not throw an exception.
      a match {
        case Const(_) => (pf.isDefinedAt, a.asInstanceOf[Arrow[T, U]])
        case _ => (pf.isDefinedAt, Arrow.map(pf).andThen(a))
      }

    /**
     * Buildes a `Choice` with a predicate that is always `true`.  This can be used
     * as the last `Choice` as a catch-all.
     */
    def otherwise[T, U](a: Arrow[T, U]): Choice[T, U] =
      (_ => true, a)
  }

  /**
   * `Arrow.sequence(a)` is equivalent to `{ xs => Stitch.traverse(xs) { x => a(x) } }`
   */
  def sequence[T, U](a: Arrow[T, U]): Arrow[Seq[T], Seq[U]] =
    Sequence(a)

  /**
   * `Arrow.option(a)` is equivalent to `{ xo: Option[T] => Stitch.traverse(xo) { x => a(x) } }`
   */
  def option[T, U](a: Arrow[T, U]): Arrow[Option[T], Option[U]] =
    Arrow.choose[Option[T], Option[U]](
      Choice.ifDefinedAt({ case Some(t) => t }, a.map(Some(_))),
      Choice.otherwise(Arrow.value(None))
    )

  /**
   * `Arrow.collect(arrows)` is equivalent to `{ x => Stitch.traverse(arrows) { a => a(x) } }`
   */
  def collect[T, U](arrows: Seq[Arrow[T, U]]): Arrow[T, Seq[U]] =
    arrows match {
      case Seq() => value(Seq.empty[U])
      case Seq(a) => a.map(Seq(_))
      case _ =>
        val throws: PartialFunction[Arrow[T, U], Throwable] = { case Const(Throw(e)) => e }
        val returns: PartialFunction[Arrow[T, U], U] = { case Const(Return(v)) => v }
        if (arrows.exists(throws.isDefinedAt))
          Arrow.exception(arrows.collectFirst(throws).get)
        else if (arrows.forall(returns.isDefinedAt))
          Arrow.value(arrows.collect(returns))
        else
          Collect(arrows)
    }

  /**
   * `Arrow.applyEffect(a)` is equivalent to `{ x => for { _ <- eff(x) } yield x }`
   */
  def applyEffect[T](eff: Effect[T]): Arrow.Iso[T] =
    Arrow.zipWithArg(eff).map(_._1)

  /**
   * A convenience method, similar to `applyEffect`, but evaluates the effect asynchronously.
   */
  def applyEffectAsync[T](eff: Effect[T]): Arrow.Iso[T] =
    Arrow.zipWithArg(Arrow.async(eff)).map(_._1)

  /**
   * Produces an Arrow that supports recursive calls.  Recursing from a `Stitch`
   * continuation is not generally stack-safe.
   */
  def recursive[T, U](f: Arrow[T, U] => Arrow[T, U]): Arrow[T, U] = Recursive(f)

  /**
   * Wraps the given call-by-name value with an `Arrow` that dynamically dispatches
   * to the call-by-name result.
   */
  def wrap[T, U](f: => Arrow[T, U]): Arrow[T, U] = Wrap(() => f)

  /**
   * `Arrow.applyArrow` is equivalent to `Arrow.flatMap { case (a: Arrow, t) => a(t) }`
   * but propagates locals into `a`.
   */
  def applyArrow[T, U](): Arrow[(Arrow[T, U], T), U] =
    ApplyArrow.asInstanceOf[Arrow[(Arrow[T, U], T), U]]

  /**
   * `Arrow.applyArrowToSeq` is equivalent to
   * `Arrow.flatMap { case (a: Arrow, t: Seq) => a.traverse(t) }`
   * but propagates locals into `a`.
   */
  def applyArrowToSeq[T, U]: Arrow[(Arrow[T, U], Seq[T]), Seq[U]] =
    Arrow.zipWithLocals[(Arrow[T, U], Seq[T])].flatMap[Seq[U]] {
      case ((a, t), l) => a.traverse(t, l)
    }

  /** Holds the locals that are in scope during the run of an Arrow */
  type Locals = scala.collection.Map[Local[_], Try[_]]

  object Locals {
    val empty: Locals = scala.collection.Map.empty[Local[_], Try[_]]
    def emptyBuffer(len: Int): ArrayBuffer[Locals] = ArrayBuffer.fill(len)(empty)
  }

  /** Marker exception indicating the value of a Local could not be retrieved */
  case class LocalUnavailable[L](l: Local[L]) extends Exception with NoStackTrace

  /**
   * Unlike c.t.u.Local, Arrow Locals are local to the execution of the Arrow, not the thread.
   *
   * That means that the local may take on different values for different elements in the same
   * batch. For instance if you traverse an Arrow with a sequence of inputs, when each input is
   * processed it will have its own instance of the Locals scoped for running just that specific
   * element.
   *
   * It also means that the locals are not available in the body of the functions implementing
   * an Arrow (eg. in `map`, `flatMap`, `transform`, `rescue`, etc.) unless you apply the local
   * first to make it part of the input type. This has potential to be confusing if you apply an
   * Arrow in the body of a `flatMap`, since the inner Arrow will not inherit the locals from
   * the Arrow that invoked it.
   */
  final class Local[L](default: L) {

    /**
     * Constructor for Locals with no default
     */
    def this() = this(null.asInstanceOf[L])

    /**
     * An arrow who's input is discarded and who's output is the current Local value
     */
    def apply(): Arrow[Any, L] = GetLocal(this)

    /**
     * Get current Local value as an arrow that returns the current value tupled with
     * the input argument
     *
     * @tparam In the type of the input argument
     * @return a tuple of the input argument and the current value of the local
     */
    def applyWithArg[In](): Arrow[In, (In, L)] = zipWithArg(GetLocal(this))

    /**
     * Creates an arrow wrapping the input arrow, `a`, with the Local value
     * defined for the scope of `a`, restoring the current state upon completion
     *
     * The local this will not be updated if the input is in a `Throw` state,
     * `a` will run with the existing local value.
     *
     * @param value the value that the local will be set to
     * @param a the arrow which will be executed with the Local set to `value`
     * @tparam In the input type for the returned arrow and for arrow `a`
     * @tparam Out the output type for the returned arrow and for arrow`a`
     *
     * @return an arrow wrapping `a` and with the local defined for arrow `a`
     */
    def let[In, Out](value: L)(a: Arrow[In, Out]): Arrow[In, Out] =
      LetLocalIndependent(this, a, _ => value)

    /**
     * Creates an arrow wrapping the input arrow, `a`, with the Local value
     * returned by the function `update` and defined for the scope of `a`,
     * restoring the current state upon completion
     *
     * The local this will not be updated if either the input or existing local
     * are in a `Throw` state, `a` will run with the existing local value.
     *
     * @param update a function which takes in an the current local value
     *               then returns a new local value which will be used
     *               in this scope.
     * @param a the arrow which will be executed with the Local updated by `update`
     * @tparam In the input type for the returned arrow and for arrow `a`
     * @tparam Out the output type for the returned arrow and for arrow `a`
     * @return an arrow wrapping `a` and with the local defined for arrow `a`
     */
    def let[In, Out](update: L => L)(a: Arrow[In, Out]): Arrow[In, Out] =
      LetLocalDependent[L, In, Out](this, a, (_: In, l: L) => update(l))

    /**
     * Creates an arrow wrapping the input arrow, `a`, with the Local value
     * returned by the function `update` and defined for the scope of `a`,
     * restoring the current state upon completion
     *
     * The local this will not be updated if the input is in a `Throw` state,
     * `a` will run with the existing local value.
     *
     * @param update a function which takes the current input value then returns
     *               a new local value which will be used in this scope.
     * @param a the arrow which will be executed with the Local updated by `update`
     * @tparam In the input type for the returned arrow and for arrow `a`
     * @tparam Out the output type for the returned arrow and for arrow `a`
     * @return an arrow wrapping `a` and with the local defined for arrow `a`
     */
    def letWithArg[In, Out](update: In => L)(a: Arrow[In, Out]): Arrow[In, Out] =
      LetLocalIndependent[L, In, Out](this, a, update)

    /**
     * Creates an arrow wrapping the input arrow, `a`, with the Local value
     * returned by the function `update` and defined for the scope of `a`,
     * restoring the current state upon completion
     *
     * The local this will not be updated if either the input or existing local
     * are in a `Throw` state, `a` will run with the existing local value.
     *
     * @param update a function which takes in the current input value and the
     *               current local value then returns a new local value which
     *               will be used in this scope.
     * @param a the arrow which will be executed with the Local updated by `update`
     * @tparam In the input type for the returned arrow and for arrow `a`
     * @tparam Out the output type for the returned arrow and for arrow `a`
     * @return an arrow wrapping `a` and with the local defined for arrow `a`
     */
    def letWithArg[In, Out](update: (In, L) => L)(a: Arrow[In, Out]): Arrow[In, Out] =
      LetLocalDependent[L, In, Out](this, a, update)

    /**
     * Creates an arrow wrapping the input arrow, `a`, with the Local value
     * cleared for the scope of `a`, restoring the current state upon completion
     *
     * @param a the arrow which will be executed with the Local cleared
     * @tparam In the input type for the returned arrow and for arrow `a`
     * @tparam Out the output type for the returned arrow and for arrow `a`
     * @return an arrow wrapping `a` and with the local cleared for arrow `a`
     */
    def letClear[In, Out](a: Arrow[In, Out]): Arrow[In, Out] =
      ClearLocal(this, a)

    /** Marker exception indicating the value of this Local could not be retrieved */
    val Unavailable: LocalUnavailable[L] = LocalUnavailable(this)

    private[this] val fallback =
      if (default == null) Throw(Unavailable) else Return(default)

    private[this] val fallbackFn = { (_: Local[_]) => fallback }

    private[Arrow] def extract(ctx: Locals): Try[L] =
      ctx.applyOrElse(this, fallbackFn).asInstanceOf[Try[L]]
  }
}
