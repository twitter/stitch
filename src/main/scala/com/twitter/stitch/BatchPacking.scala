package com.twitter.stitch

import java.util.Comparator
import scala.annotation.tailrec
import scala.collection.mutable
import scala.math.Ordering

/** The strategy to use when packing buckets into batches */
sealed trait BatchPackingStrategy

/** Packing strategy that can result in more RPCs but buckets will remain intact
 *  - The largest bucket is combined with the next largest bucket which is small enough to fit in the batch
 *  - The combining is done recursively until there are no buckets small enough to fit in the remaining space
 *
 * For instance with a maxSize of 3 the buckets `[[A,A],[B,B],[C,C]]`
 * are not combined and become the batches `[[A,A],[B,B],[C,C]]`, resulting in 3 RPCs
 */
object NeverSplitBuckets extends BatchPackingStrategy

/** Packing strategy that can result in fewer RPCs but some buckets may be broken up
 *  - The largest bucket is combined with the next largest bucket which is small enough to fit in the batch
 *  - The combining is done recursively until there are no buckets small enough to fit in the remaining space
 *  - With no buckets small enough to fill the remaining space the smallest bucket is broken up.
 *      The keys from the smallest bucket are then added to fill the batch
 *  - As an optimization, splitting the bucket to fill the remaining space is skipped
 *      if there is only 1 bucket left and that bucket is too large to fill the remaining space
 *      since we would have to make another batch anyway, we can keep the bucket intact
 *
 * For instance with a maxSize of 3 the buckets `[[A,A],[B,B],[C,C]]`
 * the batches `[[A,B,B],[A,C,C]]` are formed by splitting the `[A,A]`, resulting in 2 RPCs
 */
object SplitSmallBuckets extends BatchPackingStrategy

private[stitch] object BatchPacking {
  // used to sort an array by the size of the stored hashsets in descending order
  private val setComparator: Comparator[java.util.HashSet[_]] =
    Ordering.by[java.util.HashSet[_], Int](_.size()).reverse

  /** Fills `batch` with `remainingSpaceInBatch` keys using keys from `remainingSortedKeyBuckets`
   *
   *  Preconditions:
   *   - no key set in `remainingSortedKeyBuckets` can have more than `maxSize` elements
   *   - `remainingSortedKeyBuckets` must be sorted by hashset size in ascending order
   *
   *  - Batches which can be combined without splitting are combined into batches
   *  - The remaining sorted key buckets are combined according to the provided `BatchPackingStrategy`
   *
   *  For instance with a maxSize of 5 `[[A,A],[B,B,B]]` are combined
   *  into the batch `[B,B,B,A,A]` with both `BatchPackingStrategy`s
   *
   * @tparam K the key type
   * @param remainingSortedKeyBuckets list of hashsets of keys sorted in descending order by size
   *                                  where no hashset has a size > maxSize
   * @param batch the batch that is currently being built
   * @param remainingSpaceInBatch the amount of space left in the batch, this is the same as maxSize - batch.size
   * @param batchPackingStrategy specifies how to pack the remaining batches
   * @return an array buffer containing a single batch of keys
   */
  @inline
  @tailrec
  def fillBatch[K](
    remainingSortedKeyBuckets: Array[java.util.HashSet[K]],
    batch: mutable.ArrayBuffer[K],
    remainingSpaceInBatch: Int,
    batchPackingStrategy: BatchPackingStrategy
  ): mutable.ArrayBuffer[K] = {
    // find the largest bucket which is small enough to fit in the batch and remove it from the remainingSortedKeyBuckets
    // if no bucket is found, indicate whether there is > 1 bucket remaining
    val (keySetThatFits, moreThan1BucketRemaining) = {
      var i = 0
      var countOfBuckets = 0
      var found: java.util.HashSet[K] = null
      while (i < remainingSortedKeyBuckets.length) {
        val bucket = remainingSortedKeyBuckets(i)
        if (bucket != null) {
          countOfBuckets += 1
          if (bucket.size() <= remainingSpaceInBatch) {
            found = bucket
            remainingSortedKeyBuckets.update(i, null) // tombstone the bucket
            i = remainingSortedKeyBuckets.length // exit the loop
          }
        }
        i += 1
      }
      (found, countOfBuckets > 1)
    }

    keySetThatFits match {
      case null if remainingSpaceInBatch > 0 && moreThan1BucketRemaining =>
        // With no buckets small enough to fill the remaining space,
        // if the `batchPackingStrategy` is `SplitSmallBuckets` then the smallest bucket
        // is broken up and keys from the smallest bucket are used to fill the batch
        //
        // small optimization:
        // splitting the bucket to fill the remaining space is skipped
        // if there is only 1 bucket left and that bucket is too large to fill the remaining space
        // since we would have to make another batch anyway, we can keep the bucket intact
        batchPackingStrategy match {
          case SplitSmallBuckets =>
            // since used buckets are tombstoned with null, we need to find the last element which is non-null
            // since moreThan1BucketRemaining is true, this will always return a valid index
            val indexOfLastNonTombstoneBucket = remainingSortedKeyBuckets.lastIndexWhere(_ != null)
            val smallestKeySet = remainingSortedKeyBuckets(indexOfLastNonTombstoneBucket).iterator()
            // remove element from the smallest bucket and add it to the current batch
            var rsib = remainingSpaceInBatch
            while (rsib > 0) {
              batch += smallestKeySet.next()
              smallestKeySet.remove()
              rsib -= 1
            }
          // no need to update `remainingSortedKeyBuckets` since we were using the smallest element which is already in the right spot

          case _ =>
        }
        batch // return the created batch

      case null => batch // return the created batch

      case keySet =>
        // `keySet.size` <=  `remainingSpaceInBatch` so we dont need to check sizes here
        keySet.forEach(key => batch += key)
        // recursively fill the remaining
        val newRemaining = remainingSpaceInBatch - keySet.size
        if (newRemaining != 0) {
          // fillBatch is done recursively until there are no buckets small enough to fit in the remaining space
          fillBatch(remainingSortedKeyBuckets, batch, newRemaining, batchPackingStrategy)
        } else {
          batch // return the created batch
        }
    }
  }

  /** Adds all keys in all keyBuckets into batches then returns the batches
   *
   *  - Buckets whose size >= maxSize are broken into batches of maxSize and the remaining key buckets
   *  - Batches which can be combined without splitting are combined into batches
   *  - Remaining key buckets are batched according to `batchPackingStrategy`
   *
   *  With the following Map[Bucket, Key] as an input and a maxSize of 3: `[
   *  A -> [A,A]
   *  B -> [B,B]
   *  C -> [C,C,C,C,C]
   *  ]` we start with these buckets `[[A,A],[B,B],[C,C,C,C,C]]`
   *  An initial pass extracts all full batches `[[C,C,C]]` leaving `[[A,A],[B,B],[C,C]]`
   *  then the `batchPackingStrategy` determines how these get batched
   *
   * @tparam K the key type
   * @tparam B the bucket type
   * @param keyCount the total number of keys in all the keyBuckets
   * @param keyBuckets hashmap of bucket -> set of keys, `keyBuckets` is mutated during packing
   * @param maxSize the max number of calls for an RPC
   * @param batchPackingStrategy specifies how to pack the remaining batches
   *
   * @return a linked list of batches containing all the keys from keyBuckets
   */
  @inline
  def packBatches[K, B](
    keyCount: Int,
    keyBuckets: mutable.HashMap[B, java.util.HashSet[K]],
    maxSize: Int,
    batchPackingStrategy: BatchPackingStrategy
  ): mutable.ArrayBuffer[mutable.ArrayBuffer[K]] = {
    // batches contains full batches, each with keys from a single bucket
    val batches = new mutable.ArrayBuffer[mutable.ArrayBuffer[K]] {
      // set the initial size to be the max number of batches possible to avoid resizing
      batchPackingStrategy match {
        case NeverSplitBuckets => keyCount % maxSize + keyBuckets.size
        case SplitSmallBuckets => keyCount % maxSize + 1
      }
    }

    // using an array so we can do inplace sorting later
    // sized for the number of buckets, so we may have extra nulls at the end
    // indexOfFirstNull is the index of the first null
    val remainingKeyBuckets = new Array[java.util.HashSet[K]](keyBuckets.size)
    var indexOfFirstNull = 0

    // Buckets whose size >= maxSize are broken into batches of maxSize and the remaining key buckets
    keyBuckets.foreach {
      case (_, keySet) =>
        val iter = keySet.iterator
        var batch = new mutable.ArrayBuffer[K](maxSize)
        var remaining = maxSize
        while (keySet.size >= remaining) {
          batch += iter.next()
          iter.remove()
          remaining -= 1
          if (batch.size == maxSize) {
            // complete batches are removed from buckets
            batches += batch
            batch = new mutable.ArrayBuffer[K](maxSize)
            remaining = maxSize
          }
        }
        if (keySet.size() > 0) {
          // the remainder in the buckets gets added to batches later
          remainingKeyBuckets.update(indexOfFirstNull, keySet)
          indexOfFirstNull += 1
        }
    }

    // batches now contains batches from all buckets which were >= maxSize
    // remainingKeyBuckets now contains all the buckets and remainders which were < maxSize

    // sort the remainingKeyBuckets in place to avoid extra allocations
    // only sort the non-null elements [0 to indexOfFirstNull)
    java.util.Arrays.sort(
      remainingKeyBuckets,
      0, // inclusive index
      indexOfFirstNull, // exclusive index
      setComparator.asInstanceOf[Comparator[java.util.HashSet[K]]]
    )

    // add non-complete buckets to batches
    while (remainingKeyBuckets.exists(_ != null)) {
      batches += fillBatch(
        remainingKeyBuckets,
        new mutable.ArrayBuffer[K](maxSize),
        maxSize,
        batchPackingStrategy)
    }

    batches
  }
}
