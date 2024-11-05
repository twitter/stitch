package com.twitter.stitch.cache.caffeine

import com.github.benmanes.caffeine.cache.Caffeine
import com.twitter.stitch.Arrow
import com.twitter.stitch.Stitch
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Timer
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.ConcurrentMap
import scala.annotation.tailrec
import scala.ref.WeakReference

/**
 * A cache that periodically refreshes contents. It returns
 * an Arrow to get the latest value for the provided key.
 * The refreshing mechanism keeps a weak reference to the
 * returned Arrows and automatically stops updating once
 * they're not strongly reachable anymore. If the initial
 * fetching of a key fails, subsequent requests will
 * re-fetch. Failures are not cached, so once a fetch
 * succeeds it is not affected by refresh failures.
 *
 * The implementation uses `equals`/`==` of the `U` type
 * and, during the refresh, only updates the cache and
 * invokes the `transform` function if the equality
 * comparison returns false (i.e. `fetch` returns a new
 * value).
 *
 * The `interval` parameter is used to delay the execution
 * between refreshes. The implementation doesn't take into
 * consideration how long the refresh takes. For example,
 * if the `interval` is 1s and the refresh takes 0.5s, the
 * values will be updated every 1.5s
 */
object RefreshingWeakCache {

  /**
   * Creates a refreshing cache. The cache is a function from a key to an `Arrow` that retrieves the
   * latest value for that key. Callers must hold on to the returned `Arrow`s for keys they are
   * interested in - this signals to the cache that it must keep refreshing those keys.
   *
   * @param fetch used to fetch the latest value. The second
   *              input is the previously fetched value
   * @param interval interval between cache refreshes
   * @param timer timer used for the refresh
   * @tparam K key to be fetched
   * @tparam V final transformed value
   * @return function to get an Arrow that retrieves the latest value for a key at the time that the
   *         arrow is executed. Callers should keep a reference to the arrow as long as the cache
   *         entry is needed. When the arrow is no longer strongly reachable, the cache will stop
   *         updating the entry.
   */
  def apply[K, V](
    fetch: Arrow[(K, Option[V]), V],
    interval: Duration,
  )(
    implicit timer: Timer
  ): K => Arrow[Unit, V] = apply(fetch, (_: K, v: V) => v, interval, NullObserver)

  /**
   * Creates a refreshing cache. The cache is a function from a key to an `Arrow` that retrieves the
   * latest value for that key. Callers must hold on to the returned `Arrow`s for keys they are
   * interested in - this signals to the cache that it must keep refreshing those keys.
   *
   * @param fetch used to fetch the latest value. The second
   *              input is the previously fetched value
   * @param interval interval between cache refreshes
   * @param observer observer notified of cache events. Use [[NullObserver]] if cache statistics are not needed
   * @param timer timer used for the refresh
   * @tparam K key to be fetched
   * @tparam V final transformed value
   * @return function to get an Arrow that retrieves the latest value for a key at the time that the
   *         arrow is executed. Callers should keep a reference to the arrow as long as the cache
   *         entry is needed. When the arrow is no longer strongly reachable, the cache will stop
   *         updating the entry.
   */
  def apply[K, V](
    fetch: Arrow[(K, Option[V]), V],
    interval: Duration,
    observer: Observer[K],
  )(
    implicit timer: Timer
  ): K => Arrow[Unit, V] = apply(fetch, (_: K, v: V) => v, interval, observer)

  /**
   * Creates a refreshing cache. The cache is a function from a key to an `Arrow` that retrieves the
   * latest value for that key. Callers must hold on to the returned `Arrow`s for keys they are
   * interested in - this signals to the cache that it must keep refreshing those keys.
   *
   * When first populating the value for a given key, the `fetch` arrow is run as part of the returned
   * cached arrow (and therefore on the thread where the arrow is run). Subsequent refreshes are run
   * by the provided `timer`, on whatever threads it uses. All known keys are refreshed in the same Stitch
   * batch (using `Stitch.collect`), so updates are maximally batched. For example, if the `fetch` arrow
   * involves making a batchable RPC to another service, all keys may be refreshed in a single RPC.
   *
   * @param fetch used to fetch the latest value. The second
   *              input is the previously fetched value
   * @param transform used to transform the value returned by
   *                  `fetch`
   * @param interval interval between cache refreshes
   * @param observer observer notified of cache events. Use [[NullObserver]] if cache statistics are not needed
   * @param timer timer used for the refresh
   * @tparam K key to be fetched
   * @tparam U intermediate value to be transformed
   * @tparam V final transformed value
   * @return function to get an Arrow that retrieves the latest value for a key at the time that the
   *         arrow is executed. Callers should keep a reference to the arrow as long as the cache
   *         entry is needed. When the arrow is no longer strongly reachable, the cache will stop
   *         updating the entry.
   */
  def apply[K, U, V](
    fetch: Arrow[(K, Option[U]), U],
    transform: (K, U) => V,
    interval: Duration,
    observer: Observer[K] = NullObserver,
  )(
    implicit timer: Timer
  ): K => Arrow[Unit, V] = {
    sealed trait State
    object State {
      case class Loading(init: Stitch[Unit]) extends State
      case class Loaded(u: U, v: V) extends State
      case object Failed extends State
    }

    val cache: ConcurrentMap[K, AtomicReference[State]] =
      Caffeine
        .newBuilder().weakValues()
        .build().asMap()
        .asInstanceOf[ConcurrentMap[K, AtomicReference[State]]]

    // Unconditionally fetch, without checking for an existing in-progress fetch
    def forceLoad(k: K, u: Option[U], ref: AtomicReference[State]): Stitch[Unit] =
      Stitch.synchronizedRef {
        observer.willRefresh(k)
        fetch((k, u)).respond {
          case Return(nu) =>
            if (u.forall(_ != nu)) {
              observer.onUpdate(k)
              val nv = transform(k, nu)
              ref.set(State.Loaded(nu, nv))
            }
          case Throw(t) =>
            // If there hasn't been a successful load yet, set the state back to Failed. This ensures
            // that we keep re-attempting loads until one succeeds. We can do this because only
            // one load is in flight at a time, so this won't clobber a racing successful load.
            if (u.isEmpty) {
              ref.set(State.Failed)
              observer.onInitFailure(k, t)
            }
        }.unit
      }

    def load(k: K, ref: AtomicReference[State]): Stitch[Unit] = {
      (ref.get() match {
        case null => Right(None)
        case State.Failed => Right(None)
        case State.Loaded(u, _) => Right(Some(u))
        case State.Loading(s) => Left(s)
      }) match {
        case Left(s) => s
        case Right(u) => forceLoad(k, u, ref)

      }
    }

    val refresh = () => {
      import collection.JavaConverters._
      val tasks =
        cache.asScala.toSeq.map {
          case (k, ref) =>
            load(k, ref).handle {
              case ex =>
                observer.onRefreshFailure(k, ex)
            }
        }
      Stitch.run(Stitch.time(Stitch.collect(tasks)).map {
        case (_, latency) => observer.refreshLatency(latency)
      })
    }

    refreshLoop(interval, new WeakReference(refresh))

    { (k: K) =>
      // Get the state reference. Despite calling load(), this doesn't actually start loading until
      // the Stitch is run through the returned Arrow
      var hit = true
      val ref = cache.computeIfAbsent(
        k,
        _ => {
          hit = false
          val ref = new AtomicReference[State]
          ref.set(State.Loading(load(k, ref)))
          ref
        }
      )

      if (hit) observer.onCacheHit(k)
      else observer.onCacheMiss(k)

      // Get the value after a loading stitch completes
      def retrieve(): V = ref.get() match {
        case s: State.Loaded => s.v
        case _: State.Loading =>
          // This shouldn't be reachable because if loading fails, then the init Stitch should contain an exception
          throw new IllegalStateException("bug: state still loading after init is done")
        case State.Failed =>
          // This shouldn't be reachable for the same reason as State.Loading
          throw new IllegalStateException("bug: state still failed after init is done")
      }

      Arrow
        .flatMap { (_: Unit) =>
          // Keep a strong reference to the refresh function.
          // The refresh loop will stop when this arrow and its
          // refresh function are GCed. In addition, the arrow
          // keeps a strong reference to the per-key ref, so
          // when it's GCed, the Caffeine cache entry will be
          // removed.
          val refreshRef = refresh

          @tailrec
          def loop(): Stitch[V] = ref.get() match {
            case s: State.Loading =>
              s.init.map { _ => retrieve() }
            case s: State.Loaded => Stitch.value(s.v)
            case State.Failed =>
              // Atomically set our load() stitch as the current loading stitch. This ensures that only
              // one thread retries if there has not yet been a successful load. If the current thread
              // loses the race, rerun the loop - this accounts for a race condition where another thread
              // wins and its stitch completes (Loaded or Failed) before we retrieve it,
              // so we can't rely on the state being Loading if we lose the race.
              // This uses a flatMap trick from Stitch.time to ensure that fetch is only called if
              // this thread's stitch is used, in case fetch has side effects. We use forceLoad because
              // load checks the current ref value before proceeding, which can lead to it blocking
              // on itself.
              val ourLoad = Stitch.synchronizedRef(Stitch.Done.flatMap { _ =>
                forceLoad(k, None, ref)
              })
              val won = ref.compareAndSet(State.Failed, State.Loading(ourLoad))
              if (won) ourLoad.map { _ => retrieve() }
              else loop()
          }

          loop()
        }
    }
  }

  private[this] def refreshLoop(
    interval: Duration,
    refreshRef: WeakReference[() => Future[Unit]]
  )(
    implicit timer: Timer
  ): Future[Unit] = {
    Future.Unit.delayed(interval).flatMap { _ =>
      refreshRef.get match {
        case None =>
          // cached arrow and its refresh function got
          // GCed, stop refresh loop
          Future.Unit
        case Some(f) =>
          f().flatMap(_ => refreshLoop(interval, refreshRef))
      }
    }
  }

  /**
   * Observer for a [[RefreshingWeakCache]] arrow
   */
  trait Observer[-K] {

    /**
     * Called whenever an already-cached key is requested
     */
    def onCacheHit(key: K): Unit

    /**
     * Called whenever a key is requested that is not yet cached
     */
    def onCacheMiss(key: K): Unit

    /**
     * Called when fetching a key fails and there is not yet a cached value. This can happen if
     * the first attempt to initialize fails or if subsequent initializations also fail.
     */
    def onInitFailure(key: K, ex: Throwable): Unit

    /**
     * Called when refreshing a key fails
     */
    def onRefreshFailure(key: K, ex: Throwable): Unit

    /**
     * Called when the cache is about to refresh a key
     */
    def willRefresh(key: K): Unit

    /**
     * Called when the value for a key changes
     */
    def onUpdate(key: K): Unit

    /**
     * Called when refreshing all keys completes
     */
    def refreshLatency(latency: Duration): Unit
  }

  /**
   * Observer with default no-op implementations
   */
  class BaseObserver extends Observer[Any] {
    override def onCacheHit(key: Any): Unit = {}
    override def onCacheMiss(key: Any): Unit = {}
    override def onInitFailure(key: Any, ex: Throwable): Unit = {}
    override def onRefreshFailure(key: Any, ex: Throwable): Unit = {}
    override def willRefresh(key: Any): Unit = {}
    override def onUpdate(key: Any): Unit = {}
    override def refreshLatency(latency: Duration): Unit = {}
  }

  object NullObserver extends BaseObserver
}
