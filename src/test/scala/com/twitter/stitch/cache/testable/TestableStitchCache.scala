package com.twitter.stitch.cache.testable

import com.twitter.stitch.Stitch
import com.twitter.stitch.cache.StitchCache
import scala.collection.mutable

/**
 * A [[StitchCache]] that exposes internal information about the state, for testing only
 *
 * Unlike a proper [[StitchCache]], this stores the provided Stitches as-is without
 * changing their form to be thread-safe. This is only supposed to be used to test
 * [[StitchCache]] wrappers which defer changing the Stitch's form to an underlying
 * implementation.
 */
class TestableStitchCache[K, V] extends StitchCache[K, V] {

  /** the underlying map containing all the keys and Stitches */
  val map: mutable.Map[K, Stitch[V]] = mutable.Map[K, Stitch[V]]()

  /** the number of times [[get]] was called */
  def getCalls: Int = getCallsPvt
  private var getCallsPvt = 0

  /** the number of times [[getOrElseUpdate]] was called */
  def getOrElseUpdateCalls: Int = getOrElseUpdateCallsPvt
  private var getOrElseUpdateCallsPvt = 0

  /** the last result of `compute` if it was run when [[getOrElseUpdate]] was called */
  def getOrElseUpdateComputeResult: Option[Stitch[V]] = getOrElseUpdateComputeResultPvt
  private var getOrElseUpdateComputeResultPvt: Option[Stitch[V]] = None

  /** the number of times [[set]] was called */
  def setCalls: Int = setCallsPvt
  private var setCallsPvt = 0

  /** the number of times [[evict]] was called */
  def evictCalls: Int = evictCallsPvt
  private var evictCallsPvt = 0

  /** the number of times [[size]] was called */
  def sizeCalls: Int = sizeCallsPvt
  private var sizeCallsPvt = 0

  override def get(key: K): Option[Stitch[V]] = synchronized {
    getCallsPvt += 1
    map.get(key)
  }

  override def getOrElseUpdate(key: K)(compute: => Stitch[V]): Stitch[V] = synchronized {
    getOrElseUpdateCallsPvt += 1
    map.get(key) match {
      case Some(s) => s
      case None =>
        val s = compute
        getOrElseUpdateComputeResultPvt = Some(s)
        map.update(key, s)
        s
    }
  }

  override def set(key: K, value: Stitch[V]): Stitch[V] = synchronized {
    setCallsPvt += 1
    map.update(key, value)
    value
  }

  override def evict(key: K, value: Stitch[V]): Boolean = synchronized {
    evictCallsPvt += 1
    map.get(key) match {
      case Some(s) if s == value => map.remove(key).nonEmpty
      case _ => false
    }
  }

  override def size: Int = synchronized {
    sizeCallsPvt += 1
    map.size
  }
}
