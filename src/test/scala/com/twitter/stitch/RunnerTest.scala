package com.twitter.stitch

import com.twitter.stitch.helpers.StitchTestSuite
import com.twitter.util.{Await, Future, Return, Throw}
import java.util
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.collection.JavaConverters._

class RunnerTest extends StitchTestSuite {
  test("SeqRunner") {
    val runner = new SeqRunner[Int, String]() {
      override def run(calls: Seq[Int]) = {
        calls.size must equal(3) // keys deduped
        Future.value(calls.map(call => Return(call.toString)))
      }
    }

    val stitches = Seq(1, 2, 3, 2).map(runner.add(_))
    Await.result(runner.run())
    val actual = stitches.map(s => Await.result(Stitch.run(s)))
    actual must equal(Seq("1", "2", "3", "2")) // results match keys
  }

  test("SeqRunner mismatched result") {
    val g = SeqGroup { calls: Seq[Int] => Future.value(Seq(Return("foo"), Return("bar"))) }
    val s = Stitch.collect(Seq(1, 2, 3).map { k => Stitch.call(k, g) })
    val e = intercept[RuntimeException] {
      Await.result(Stitch.run(s))
    }
    e.getMessage must equal("expected 3 items, got 2 items")
  }

  test("SeqRunner throws when >1 result is returned for a single key") {
    val g = SeqGroup { calls: Seq[Int] => Future.value(Seq(Return("foo"), Return("bar"))) }
    val s = Stitch.collect(Seq(1).map { k => Stitch.call(k, g) })
    val e = intercept[RuntimeException] {
      Await.result(Stitch.run(s))
    }
    e.getMessage must equal("expected 1 items, got 2 items")
  }

  test("SeqRunner returns a throw if no results are returned for a single key") {
    val g = SeqGroup { calls: Seq[Int] => Future.value(Seq()) }
    val s = Stitch.collect(Seq(1).map { k => Stitch.call(k, g) })
    val e = intercept[RuntimeException] {
      Await.result(Stitch.run(s))
    }
    e.getMessage must equal("expected 1 items, got 0 items")
  }

  test("MapRunner") {
    class TestRunner extends MapRunner[Int, String]() {
      def run(calls: Seq[Int]) = {
        // keys deduped, and represented in order of insertion
        calls must equal(Seq(1, 2, 3, 0))
        Future.value(key => Return(key.toString))
      }
    }
    val runner = new TestRunner

    val stitches = Seq(1, 2, 3, 2, 0).map(runner.add(_))
    Await.result(runner.run())
    val actual = stitches.map(s => Await.result(Stitch.run(s)))
    actual must equal(Seq("1", "2", "3", "2", "0")) // results match keys
  }

  test("BucketedMapRunner") {
    class TestRunner extends BucketedMapRunner[Int, String, Int]() {
      override def run(calls: Seq[Int]) = {
        calls.size must equal(3) // keys deduped
        Future.value(key => Return(key.toString))
      }
      override def bucket(key: Int): Int = 0
      override def packingStrategy: BatchPackingStrategy = NeverSplitBuckets
    }
    val runner = new TestRunner

    val stitches = Seq(1, 2, 3, 2).map(runner.add(_))
    Await.result(runner.run())
    val actual = stitches.map(s => Await.result(Stitch.run(s)))
    actual must equal(Seq("1", "2", "3", "2")) // results match keys
  }

  test("BucketedSeqRunner") {
    val runner = new BucketedSeqRunner[Int, String, Int]() {
      override def run(calls: Seq[Int]) = {
        calls.size must equal(3) // keys deduped
        Future.value(calls.map(call => Return(call.toString)))
      }
      override def bucket(key: Int): Int = 0
      override def packingStrategy: BatchPackingStrategy = NeverSplitBuckets
    }

    val stitches = Seq(1, 2, 3, 2).map(runner.add(_))
    Await.result(runner.run())
    val actual = stitches.map(s => Await.result(Stitch.run(s)))
    actual must equal(Seq("1", "2", "3", "2")) // results match keys
  }

  test("maxSize") {
    var count = 0

    class TestRunner extends MapRunner[Int, String]() {
      def run(calls: Seq[Int]) = {
        calls.size must equal(2) // keys deduped and chunked
        count += 1
        Future.value(key => Return(key.toString))
      }

      override def maxSize = 3
    }
    val runner = new TestRunner

    val stitches = Seq(1, 2, 3, 2, 4).map(runner.add(_))
    Await.result(runner.run())
    count must equal(2)
    val actual = stitches.map(s => Await.result(Stitch.run(s)))
    actual must equal(Seq("1", "2", "3", "2", "4")) // results match keys
  }

  test("StitchSeqGroup") {
    case object SG extends SeqGroup[Seq[Int], Seq[String]] {
      def run(calls: Seq[Seq[Int]]) = {
        calls.size must equal(2)
        Future.value(calls.map(call => Return(call.map(_.toString))))
      }
    }

    case class SSG(flag: Boolean) extends StitchSeqGroup[Int, String] {
      def run(calls: Seq[Int]) = {
        calls.size must equal(3) // keys deduped
        Stitch.call(calls, SG)
      }
    }

    def call(i: Int) = Stitch.call(i, SSG(i % 2 == 0))

    val actual = Await.result(Stitch.run(Stitch.collect(Seq(1, 2, 3, 4, 5, 6, 2, 3).map(call))))
    actual must equal(Seq("1", "2", "3", "4", "5", "6", "2", "3"))
  }

  test("BatchPacking.fillBatch - NeverSplitBuckets") {
    def buckets = Array[util.HashSet[Int]](
      new util.HashSet(Seq(20, 21, 22, 23, 24).asJava),
      new util.HashSet(Seq(10, 11, 12, 13).asJava),
      new util.HashSet(Seq(0, 1).asJava)
    )

    val packingStrategy = NeverSplitBuckets

    // not enough space
    val batch1 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 1, packingStrategy)
    batch1 mustBe empty

    // just enough for small bucket
    val batch2 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 2, packingStrategy)
    batch2 must contain theSameElementsAs Seq(0, 1)

    // enough for small bucket
    val batch3 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 3, packingStrategy)
    batch3 must contain theSameElementsAs Seq(0, 1)

    // just enough for medium bucket
    val batch4 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 4, packingStrategy)
    batch4 must contain theSameElementsAs Seq(10, 11, 12, 13)

    // just enough for large bucket
    val batch5 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 5, packingStrategy)
    batch5 must contain theSameElementsAs Seq(20, 21, 22, 23, 24)

    // enough for large bucket
    val batch6 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 6, packingStrategy)
    batch6 must contain allElementsOf Seq(20, 21, 22, 23, 24)

    // just enough for both large and small bucket
    val batch7 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 7, packingStrategy)
    batch7 must contain theSameElementsAs Seq(0, 1, 20, 21, 22, 23, 24)

    // enough for both large and small bucket
    val batch8 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 8, packingStrategy)
    batch8 must contain theSameElementsAs Seq(0, 1, 20, 21, 22, 23, 24)

    // enough for both large and small bucket
    val batch9 = BatchPacking.fillBatch(buckets, ArrayBuffer.empty, 9, packingStrategy)
    batch9 must contain theSameElementsAs Seq(10, 11, 12, 13, 20, 21, 22, 23, 24)
  }

  test("BatchPacking.fillBatch - SplitSmallBuckets") {
    def buckets = Array[util.HashSet[Int]](
      new util.HashSet(Seq(20, 21, 22, 23, 24).asJava),
      new util.HashSet(Seq(10, 11, 12, 13).asJava),
      new util.HashSet(Seq(0, 1).asJava)
    )

    val packingStrategy = SplitSmallBuckets

    // not enough space, break smallest batch to fill
    val batch1Buckets = buckets
    val batch1 = BatchPacking.fillBatch(batch1Buckets, ArrayBuffer.empty, 1, packingStrategy)
    batch1 must contain oneElementOf Seq(0, 1)
    batch1.size mustBe 1

    // just enough for small bucket
    val batch2Buckets = buckets
    val batch2 = BatchPacking.fillBatch(batch2Buckets, ArrayBuffer.empty, 2, packingStrategy)
    batch2 must contain theSameElementsAs Seq(0, 1)

    // enough for small bucket, medium bucket becomes the smallest and is broken to fill
    val batch3Buckets = buckets
    val batch3 = BatchPacking.fillBatch(batch3Buckets, ArrayBuffer.empty, 3, packingStrategy)
    batch3 must contain allElementsOf Seq(0, 1)
    batch3 must contain oneElementOf Seq(10, 11, 12, 13)
    batch3.size mustBe 3

    // just enough for medium bucket
    val batch4Buckets = buckets
    val batch4 = BatchPacking.fillBatch(batch4Buckets, ArrayBuffer.empty, 4, packingStrategy)
    batch4 must contain theSameElementsAs Seq(10, 11, 12, 13)

    // just enough for large bucket
    val batch5Buckets = buckets
    val batch5 = BatchPacking.fillBatch(batch5Buckets, ArrayBuffer.empty, 5, packingStrategy)
    batch5 must contain theSameElementsAs Seq(20, 21, 22, 23, 24)

    // enough for large bucket, smallest bucket is broken from to fill
    val batch6Buckets = buckets
    val batch6 = BatchPacking.fillBatch(batch6Buckets, ArrayBuffer.empty, 6, packingStrategy)
    batch6 must contain allElementsOf Seq(20, 21, 22, 23, 24)
    batch6 must contain oneElementOf Seq(0, 1)
    batch6.size mustBe 6

    // just enough for both large and small bucket
    val batch7Buckets = buckets
    val batch7 = BatchPacking.fillBatch(batch7Buckets, ArrayBuffer.empty, 7, packingStrategy)
    batch7 must contain theSameElementsAs Seq(0, 1, 20, 21, 22, 23, 24)

    // enough for both large and small bucket, doesnt break last batch
    val batch8Buckets = buckets
    val batch8 = BatchPacking.fillBatch(batch8Buckets, ArrayBuffer.empty, 8, packingStrategy)
    batch8 must contain theSameElementsAs Seq(0, 1, 20, 21, 22, 23, 24)

    // enough for both large and small bucket
    val batch9Buckets = buckets
    val batch9 = BatchPacking.fillBatch(batch9Buckets, ArrayBuffer.empty, 9, packingStrategy)
    batch9 must contain theSameElementsAs Seq(10, 11, 12, 13, 20, 21, 22, 23, 24)
  }

  test("BatchPacking.packBatches - works with batches and inputs of various sizes") {

    /** returns the number of elements from `expected` that were found in `result`, excluding duplicates */
    def batchContainsN[T](expected: Set[T], result: Seq[T]): (Set[T], Int) = {
      result.foldLeft((expected, 0)) {
        case ((expected, sum), found) if expected.contains(found) =>
          (expected - found, sum + 1)
        case (existingState, _) => existingState
      }
    }

    /** throws if at least 1 batch doesn't contain n elements from expected and that all batches contain 0 or n elements from expected.
     *  This basically checks to see if buckets are broken up
     */
    def batchesMaintainedBucketsOfN[T](expected: T*)(n: Int*)(batches: Seq[Seq[T]]): Unit = {
      val (_, counts) = batches.foldLeft((expected.toSet, Seq.empty[Int])) {
        case ((expected, totalCount), batch) =>
          val (remaining, count) = batchContainsN(expected, batch)
          Some(count) must contain oneElementOf n :+ 0 // either the expected number or none, no partial buckets
          (remaining, totalCount :+ count)
      }
      counts must contain atLeastOneElementOf n // at least 1 match
    }

    def buckets = mutable.HashMap(
      0 -> new util.HashSet(Seq(0, 1).asJava),
      1 -> new util.HashSet(Seq(10, 11, 12, 13).asJava),
      2 -> new util.HashSet(Seq(20, 21, 22, 23, 24).asJava)
    )

    val packingStrategy = NeverSplitBuckets

    val batches1 = BatchPacking.packBatches(10, buckets, 1, packingStrategy)
    batches1.flatten must contain theSameElementsAs buckets.values.flatMap(_.asScala)
    batches1.foreach(_.size mustBe 1)

    val batches2 = BatchPacking.packBatches(10, buckets, 2, packingStrategy)
    batches2.flatten must contain theSameElementsAs buckets.values.flatMap(_.asScala)
    batches2.groupBy(_.size).foreach {
      case (2, batches) if batches.length == 5 =>
        batchesMaintainedBucketsOfN(0, 1)(2)(batches)
        batchesMaintainedBucketsOfN(10, 11, 12, 13)(2)(batches)
        batchesMaintainedBucketsOfN(20, 21, 22, 23, 24)(2)(batches)
      case (1, batches) if batches.length == 1 =>
        batches.head must contain oneElementOf Seq(20, 21, 22, 23, 24)
      case _ => fail
    }

    val batches3 = BatchPacking.packBatches(10, buckets, 3, packingStrategy)
    batches3.flatten must contain theSameElementsAs buckets.values.flatMap(_.asScala)
    batches3.groupBy(_.size).foreach {
      case (3, batches) if batches.length == 3 =>
        batchesMaintainedBucketsOfN(10, 11, 12, 13)(3, 1)(batches)
        batchesMaintainedBucketsOfN(20, 21, 22, 23, 24)(3, 2)(batches)
      case (2, batches) if batches.length == 1 =>
        val (_, count0) = batchContainsN(Set(0, 1), batches.head)
        val (_, count1) = batchContainsN(Set(20, 21, 22, 23, 24), batches.head)
        2 must (equal(count0) or equal(count1))
      case _ => fail
    }

    val batches4 = BatchPacking.packBatches(10, buckets, 4, packingStrategy)
    batches4.flatten must contain theSameElementsAs buckets.values.flatMap(_.asScala)
    batches4.groupBy(_.size).foreach {
      case (4, batches) if batches.length == 2 =>
        batchesMaintainedBucketsOfN(10, 11, 12, 13)(4)(batches)
        batchesMaintainedBucketsOfN(20, 21, 22, 23, 24)(4)(batches)
      case (3, batches) if batches.length == 1 =>
        batchesMaintainedBucketsOfN(0, 1)(2)(batches)
        batchesMaintainedBucketsOfN(20, 21, 22, 23, 24)(1)(batches)
      case _ => fail
    }

    val batches5 = BatchPacking.packBatches(10, buckets, 5, packingStrategy)
    batches5.flatten must contain theSameElementsAs buckets.values.flatMap(_.asScala)
    batches5.groupBy(_.size).foreach {
      case (5, batches) if batches.length == 1 =>
        batches.head must contain theSameElementsAs Seq(20, 21, 22, 23, 24)
      case (4, batches) if batches.length == 1 =>
        batches.head must contain theSameElementsAs Seq(10, 11, 12, 13)
      case (2, batches) if batches.length == 1 =>
        batches.head must contain theSameElementsAs Seq(0, 1)
      case _ => fail
    }

    val batches11 = BatchPacking.packBatches(10, buckets, 11, packingStrategy)
    batches11.head must contain theSameElementsAs buckets.values.flatMap(_.asScala)

    val batches12 = BatchPacking.packBatches(10, buckets, 12, packingStrategy)
    batches12.head must contain theSameElementsAs buckets.values.flatMap(_.asScala)
  }

  test("BucketedMapRunner - mix of bucket sizes, batches partially filled ") {
    var batches = ArrayBuffer[Seq[Int]]()
    class TestRunner extends BucketedMapRunner[Int, Int, Int]() {
      override val packingStrategy = NeverSplitBuckets
      override val maxSize = 5

      override def bucket(key: Int): Int = key % 10

      def run(batch: Seq[Int]) = {
        batches += batch
        Future.value(key => Return(key))
      }
    }
    val runner = new TestRunner

    val inputs = Seq(
      Seq(8, 18, 28, 38, 48, 58, 68, 78, 88),
      Seq(7, 17, 27, 37, 47, 57, 67, 77, 87),
      Seq(6, 16, 26, 36, 46, 56, 66),
      Seq(5, 15, 25, 35, 45),
      Seq(4, 14, 24, 34),
      Seq(3, 13, 23, 33),
      Seq(2, 12),
      Seq(1, 11)
    )

    val input = inputs.flatten

    val stitches = input.map(runner.add)
    Await.result(runner.run())
    val actual = stitches.map(s => Await.result(Stitch.run(s)))
    actual must equal(input) // results match keys

    batches.size mustBe 10
    batches.groupBy(_.size).foreach {
      case (i, b) if i == 5 => b.size mustBe 4 // 4 batches with size 5 (optimal)
      case (i, b) if i == 4 => b.size mustBe 5 // 5 batches with size 4 (non-optimal)
      case (i, b) if i == 2 => b.size mustBe 1 // 1 batch with size 2
    }
    inputs.foreach { input =>
      // batches contain the entire bucket up to the maxSize
      batches.exists(batch => math.min(5, input.length) == input.count(batch.contains)) mustBe true
    }
    batches.flatten must contain theSameElementsAs input
  }

  test("BucketedMapRunner - mix of bucket sizes, optimized - last batch partially filled") {
    var batches = ArrayBuffer[Seq[Int]]()
    class TestRunner extends BucketedMapRunner[Int, Int, Int]() {
      override val packingStrategy = SplitSmallBuckets
      override val maxSize = 5

      override def bucket(key: Int): Int = key % 10

      def run(batch: Seq[Int]) = {
        batches += batch
        Future.value(key => Return(key))
      }
    }
    val runner = new TestRunner

    val inputs = Seq(
      Seq(8, 18, 28, 38, 48, 58, 68, 78, 88),
      Seq(7, 17, 27, 37, 47, 57, 67, 77, 87),
      Seq(6, 16, 26, 36, 46, 56, 66),
      Seq(5, 15, 25, 35, 45),
      Seq(4, 14, 24, 34),
      Seq(3, 13, 23, 33),
      Seq(2, 12),
      Seq(1, 11)
    )

    val input = inputs.flatten

    val stitches = input.map(runner.add)
    Await.result(runner.run())
    val actual = stitches.map(s => Await.result(Stitch.run(s)))
    actual must equal(input) // results match keys

    batches.size mustBe 9
    batches.groupBy(_.size).foreach {
      case (i, b) if i == 5 => b.size mustBe 8 // 8 batches with size 5 (optimal)
      case (i, b) if i == 2 => b.size mustBe 1 // 1 batch with size 2
    }
    inputs.foreach { input =>
      // batches contain the entire bucket up to the maxSize, the smallest buckets may be split
      if (input.length > 2)
        batches.exists(batch =>
          math.min(5, input.length) == input.count(batch.contains)) mustBe true
    }
    batches.flatten must contain theSameElementsAs input
  }

  test("BucketedMapRunner - if bucket throws the batch still processes") {
    { // individual keys
      object MockException extends Exception
      var batches = ArrayBuffer[Seq[Int]]()
      class TestRunner extends BucketedMapRunner[Int, Int, Int]() {
        override val packingStrategy = SplitSmallBuckets
        override val maxSize = 3

        override def bucket(key: Int): Int = if (key == 2) throw MockException else 0

        def run(batch: Seq[Int]) = {
          batches += batch
          Future.value(key => Return(key))
        }
      }
      val runner = new TestRunner

      val input = Seq(1, 2, 3)

      val stitches = input.map(runner.add)
      Await.result(runner.run())
      val actual = stitches.map(s => Await.result(Stitch.run(s).liftToTry))
      actual must equal(Seq(Return(1), Throw(MockException), Return(3))) // results match keys

      batches.size mustBe 1
      batches.head must contain theSameElementsAs Seq(1, 3)
    }
    { // bulk keys
      object MockException extends Exception
      var batches = ArrayBuffer[Seq[Int]]()
      class TestRunner extends BucketedMapRunner[Int, Int, Int]() {
        override val packingStrategy = SplitSmallBuckets
        override val maxSize = 3

        override def bucket(key: Int): Int = if (key == 2) throw MockException else 0

        def run(batch: Seq[Int]) = {
          batches += batch
          Future.value(key => Return(key))
        }
      }
      val runner = new TestRunner

      val input = Seq(1, 2, 3)

      runner.addSeqTry(input.map(Return(_)))
      Await.result(runner.run())

      batches.size mustBe 1
      batches.head must contain theSameElementsAs Seq(1, 3)
    }
  }
}
