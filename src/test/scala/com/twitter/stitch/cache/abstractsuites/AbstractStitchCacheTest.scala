package com.twitter.stitch.cache.abstractsuites

import com.twitter.stitch.Stitch
import com.twitter.stitch.Stitch.{Const, SynchronizedRef}
import com.twitter.stitch.cache.StitchCache
import com.twitter.stitch.helpers.AwaitHelpers
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractStitchCacheTest extends AnyFunSuite with AwaitHelpers {

  /** isWrapper indicates whether the cache that is being tested is a wrapper
   * around another cache. If it is then checking if the Stitches are in the
   * correct form is skipped since that is delegated to the underlying cache
   */
  def isWrapper: Boolean = false

  /** the name of the cache type, used for making test names nice*/
  def name: String

  /** test fixture that contains a cache and Stitch that will be stored in the cache */
  def fixture(): Fixture

  val keyString: String = "key"
  val valueString: String = "value"

  trait Fixture {
    val value: Stitch[String] = Stitch.value(valueString)
    val cache: StitchCache[String, String]
  }

  test(s"$name should get nothing when there's nothing") {
    val ctx = fixture()
    import ctx._

    assert(cache.get(keyString).isEmpty)
    assert(cache.size == 0)
  }

  test(s"$name should get something when something's set") {
    val ctx = fixture()
    import ctx._

    assert(cache.get(keyString).isEmpty)
    cache.set(keyString, value)
    assert(cache.get(keyString).exists(s => await(s) == valueString))
    assert(cache.size == 1)
  }

  test(s"$name should evict when something's set") {
    val ctx = fixture()
    import ctx._

    assert(cache.size == 0)
    assert(cache.get(keyString).isEmpty)
    cache.set(keyString, value)
    assert(cache.size == 1)
    assert(cache.get(keyString).exists(s => await(s) == valueString))
    cache.evict(keyString, value)
    assert(cache.size == 0)
    assert(cache.get(keyString).isEmpty)
  }

  test(s"$name should refuse to evict incorrectly") {
    val ctx = fixture()
    import ctx._

    assert(cache.get(keyString).isEmpty)
    cache.set(keyString, value)
    assert(cache.get(keyString).exists(s => await(s) == valueString))
    cache.evict(keyString, Stitch.value("mu"))
    assert(cache.get(keyString).exists(s => await(s) == valueString))
  }

  test(s"$name should not update if gettable") {
    val ctx = fixture()
    import ctx._

    cache.set(keyString, value)

    var updated = false
    val result = cache.getOrElseUpdate(keyString) {
      updated = true
      Stitch.value("other")
    }
    assert(await(result) == valueString)
    assert(!updated)
  }

  test(s"$name should update if ungettable") {
    val ctx = fixture()
    import ctx._

    val s = Stitch.value("other")
    var mod = false
    val result = cache.getOrElseUpdate(keyString) {
      mod = true
      s
    }
    assert(await(result) == "other")
    assert(mod)
  }

  test(s"$name should report correct size") {
    val ctx = fixture()
    import ctx._

    cache.set(keyString, value)
    cache.set("key2", value)
    cache.set("key3", value)
    assert(cache.size == 3)
  }

  // wrapper implementations can delegate to the underlying for thread safety
  if (!isWrapper) {
    test(s"$name should only cache stitches in thread-safe forms") {
      // get constants
      val ctx = fixture()
      import ctx._

      val safeStitches = Seq[Stitch[String]](
        value, // Const(Return(_))
        Stitch.exception(new Exception), // Const(Throw(_))
        Stitch.synchronizedRef(value) // SynchronizedRef(_)
      )

      val someUnsafeStitches = Seq[Stitch[String]](
        Stitch.ref(value),
        Stitch.value("other").flatMap(_ => value),
        Stitch.traverse(Some(valueString))(Stitch.value).lowerFromOption(),
        Stitch.callFuture(Future.value(valueString))
      )

      (safeStitches ++ someUnsafeStitches).foreach { s =>
        val ctx = fixture()
        import ctx._

        cache.set(keyString, s) match {
          case Const(_) =>
          case SynchronizedRef(_) =>
          case s => fail(s"$s is not a thread-safe Stitch form")
        }

        cache.get(keyString).exists {
          case Const(_) => true
          case SynchronizedRef(_) => true
          case s => fail(s"$s is not a thread-safe Stitch form")
        }

      }
    }
  }
}
