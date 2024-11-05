package com.twitter.stitch

import com.twitter.util.Try
import com.twitter.util.Return
import com.twitter.util.Throw
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Provides some utility methods for working with `ArrayBuffer[Try[_]]`.
 */
object TryBuffer {

  /**
   * Makes a copy of the buffer.
   */
  def copy[A](ts: ArrayBuffer[Try[A]]): ArrayBuffer[Try[A]] =
    new ArrayBuffer[Try[A]](ts.length) ++= ts

  /**
   * Builds a new `ArrayBuffer[Try[A]]` by wrapping each value in the given
   * sequence with `Return`.
   */
  def values[A](s: Seq[A]): ArrayBuffer[Try[A]] = {
    val iter = s.iterator
    val buf = new ArrayBuffer[Try[A]](s.length)
    while (iter.hasNext) buf += Return(iter.next())
    buf
  }

  /**
   * Returns true if the given buffer contains a `Return`.
   */
  def containsReturn[A](ts: ArrayBuffer[Try[A]]): Boolean = {
    var i = 0
    val len = ts.length

    while (i < len) {
      if (ts(i).isReturn) return true
      i += 1
    }

    false
  }

  /**
   * Applies the given function to each `Try` element in the buffer, updating the elements
   * in place (does not create a new buffer.)
   */
  def mapTry[A, B](ts: ArrayBuffer[Try[A]])(f: Try[A] => Try[B]): ArrayBuffer[Try[B]] = {
    val res = ts.asInstanceOf[ArrayBuffer[Try[B]]]
    var i = 0
    val len = ts.length

    while (i < len) {
      res(i) = f(ts(i))
      i += 1
    }

    res
  }

  /**
   * Applies the given function to each `Return`ed value in the buffer, updating the elements
   * in place (does not create a new buffer).
   */
  def flatMapReturn[A, B](ts: ArrayBuffer[Try[A]])(f: A => Try[B]): ArrayBuffer[Try[B]] = {
    val res = ts.asInstanceOf[ArrayBuffer[Try[B]]]
    var i = 0
    val len = ts.length

    while (i < len) {
      ts(i) match {
        case Return(v) => res(i) = f(v)
        case _ =>
      }
      i += 1
    }

    res
  }

  /**
   * Will return a `Throw` if the buffer contains any `Throws`.  Otherwise will return a `Return`
   * of the same buffer with each value unwrapped and updated in place (does not create a
   * new buffer).
   */
  def collect[A](ts: ArrayBuffer[Try[A]]): Try[ArrayBuffer[A]] = {
    val as = ts.asInstanceOf[ArrayBuffer[A]]
    val len = ts.length
    var i = 0

    while (i < len) {
      ts(i) match {
        case Return(t) => as(i) = t
        case t => return t.asInstanceOf[Try[ArrayBuffer[A]]]
      }
      i += 1
    }

    Return(as)
  }

  /**
   * Fills the buffer with the provided `v`
   */
  def fill[A, B](ts: ArrayBuffer[Try[A]])(v: Try[B]): ArrayBuffer[Try[B]] = {
    val res = ts.asInstanceOf[ArrayBuffer[Try[B]]]
    var i = 0
    val len = ts.length

    while (i < len) {
      res(i) = v
      i += 1
    }

    res
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    k: (A, B) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)

    while (i < as.length) {
      rs(i) =
        try Return(k(as(i)(), bs(i)()))
        catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    k: (A, B, C) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)

    while (i < as.length) {
      rs(i) =
        try Return(k(as(i)(), bs(i)(), cs(i)()))
        catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    k: (A, B, C, D) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)

    while (i < as.length) {
      rs(i) =
        try Return(k(as(i)(), bs(i)(), cs(i)(), ds(i)()))
        catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    k: (A, B, C, D, E) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)

    while (i < as.length) {
      rs(i) =
        try Return(k(as(i)(), bs(i)(), cs(i)(), ds(i)(), es(i)()))
        catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    k: (A, B, C, D, E, F) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)

    while (i < as.length) {
      rs(i) =
        try Return(k(as(i)(), bs(i)(), cs(i)(), ds(i)(), es(i)(), fs(i)()))
        catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, G, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    gs: ArrayBuffer[Try[G]],
    k: (A, B, C, D, E, F, G) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)
    assert(gs.length == len)

    while (i < as.length) {
      rs(i) =
        try Return(k(as(i)(), bs(i)(), cs(i)(), ds(i)(), es(i)(), fs(i)(), gs(i)()))
        catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, G, H, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    gs: ArrayBuffer[Try[G]],
    hs: ArrayBuffer[Try[H]],
    k: (A, B, C, D, E, F, G, H) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)
    assert(gs.length == len)
    assert(hs.length == len)

    while (i < as.length) {
      rs(i) =
        try Return(k(as(i)(), bs(i)(), cs(i)(), ds(i)(), es(i)(), fs(i)(), gs(i)(), hs(i)()))
        catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    gs: ArrayBuffer[Try[G]],
    hs: ArrayBuffer[Try[H]],
    is: ArrayBuffer[Try[I]],
    k: (A, B, C, D, E, F, G, H, I) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)
    assert(gs.length == len)
    assert(hs.length == len)
    assert(is.length == len)

    while (i < as.length) {
      rs(i) =
        try {
          Return(k(as(i)(), bs(i)(), cs(i)(), ds(i)(), es(i)(), fs(i)(), gs(i)(), hs(i)(), is(i)()))
        } catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    gs: ArrayBuffer[Try[G]],
    hs: ArrayBuffer[Try[H]],
    is: ArrayBuffer[Try[I]],
    js: ArrayBuffer[Try[J]],
    k: (A, B, C, D, E, F, G, H, I, J) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)
    assert(gs.length == len)
    assert(hs.length == len)
    assert(is.length == len)
    assert(js.length == len)

    while (i < as.length) {
      rs(i) =
        try {
          Return(
            k(
              as(i)(),
              bs(i)(),
              cs(i)(),
              ds(i)(),
              es(i)(),
              fs(i)(),
              gs(i)(),
              hs(i)(),
              is(i)(),
              js(i)()
            )
          )
        } catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    gs: ArrayBuffer[Try[G]],
    hs: ArrayBuffer[Try[H]],
    is: ArrayBuffer[Try[I]],
    js: ArrayBuffer[Try[J]],
    ks: ArrayBuffer[Try[K]],
    k: (A, B, C, D, E, F, G, H, I, J, K) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)
    assert(gs.length == len)
    assert(hs.length == len)
    assert(is.length == len)
    assert(js.length == len)
    assert(ks.length == len)

    while (i < as.length) {
      rs(i) =
        try {
          Return(
            k(
              as(i)(),
              bs(i)(),
              cs(i)(),
              ds(i)(),
              es(i)(),
              fs(i)(),
              gs(i)(),
              hs(i)(),
              is(i)(),
              js(i)(),
              ks(i)()
            )
          )
        } catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    gs: ArrayBuffer[Try[G]],
    hs: ArrayBuffer[Try[H]],
    is: ArrayBuffer[Try[I]],
    js: ArrayBuffer[Try[J]],
    ks: ArrayBuffer[Try[K]],
    ls: ArrayBuffer[Try[L]],
    k: (A, B, C, D, E, F, G, H, I, J, K, L) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)
    assert(gs.length == len)
    assert(hs.length == len)
    assert(is.length == len)
    assert(js.length == len)
    assert(ks.length == len)
    assert(ls.length == len)

    while (i < as.length) {
      rs(i) =
        try {
          Return(
            k(
              as(i)(),
              bs(i)(),
              cs(i)(),
              ds(i)(),
              es(i)(),
              fs(i)(),
              gs(i)(),
              hs(i)(),
              is(i)(),
              js(i)(),
              ks(i)(),
              ls(i)()
            )
          )
        } catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    gs: ArrayBuffer[Try[G]],
    hs: ArrayBuffer[Try[H]],
    is: ArrayBuffer[Try[I]],
    js: ArrayBuffer[Try[J]],
    ks: ArrayBuffer[Try[K]],
    ls: ArrayBuffer[Try[L]],
    ms: ArrayBuffer[Try[M]],
    k: (A, B, C, D, E, F, G, H, I, J, K, L, M) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)
    assert(gs.length == len)
    assert(hs.length == len)
    assert(is.length == len)
    assert(js.length == len)
    assert(ks.length == len)
    assert(ls.length == len)
    assert(ms.length == len)

    while (i < as.length) {
      rs(i) =
        try {
          Return(
            k(
              as(i)(),
              bs(i)(),
              cs(i)(),
              ds(i)(),
              es(i)(),
              fs(i)(),
              gs(i)(),
              hs(i)(),
              is(i)(),
              js(i)(),
              ks(i)(),
              ls(i)(),
              ms(i)()
            )
          )
        } catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }

  /**
   * Will reuse the first buffer for the result.
   */
  def joinMap[A, B, C, D, E, F, G, H, I, J, K, L, M, N, Z](
    as: ArrayBuffer[Try[A]],
    bs: ArrayBuffer[Try[B]],
    cs: ArrayBuffer[Try[C]],
    ds: ArrayBuffer[Try[D]],
    es: ArrayBuffer[Try[E]],
    fs: ArrayBuffer[Try[F]],
    gs: ArrayBuffer[Try[G]],
    hs: ArrayBuffer[Try[H]],
    is: ArrayBuffer[Try[I]],
    js: ArrayBuffer[Try[J]],
    ks: ArrayBuffer[Try[K]],
    ls: ArrayBuffer[Try[L]],
    ms: ArrayBuffer[Try[M]],
    ns: ArrayBuffer[Try[N]],
    k: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => Z
  ): ArrayBuffer[Try[Z]] = {
    val len = as.length
    val rs = as.asInstanceOf[ArrayBuffer[Try[Z]]]
    var i = 0

    assert(bs.length == len)
    assert(cs.length == len)
    assert(ds.length == len)
    assert(es.length == len)
    assert(fs.length == len)
    assert(gs.length == len)
    assert(hs.length == len)
    assert(is.length == len)
    assert(js.length == len)
    assert(ks.length == len)
    assert(ls.length == len)
    assert(ms.length == len)
    assert(ns.length == len)

    while (i < as.length) {
      rs(i) =
        try {
          Return(
            k(
              as(i)(),
              bs(i)(),
              cs(i)(),
              ds(i)(),
              es(i)(),
              fs(i)(),
              gs(i)(),
              hs(i)(),
              is(i)(),
              js(i)(),
              ks(i)(),
              ls(i)(),
              ms(i)(),
              ns(i)()
            )
          )
        } catch {
          case NonFatal(t) => Throw(t)
        }
      i += 1
    }

    rs
  }
}
