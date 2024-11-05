package com.twitter.stitch

import com.twitter.util.{Future, Try}

/** A Group is used as a key to batch calls together; calls with the
 * same (i.e. equals()) group may be batched together. Typically a
 * Group is a case class to get structural equality.
 *
 * For batch RPC APIs following the typical pattern of a set of keys
 * and some context which applies to all keys, the group should
 * include the context. For APIs with no context, the group can be
 * any distinct object.
 *
 * A group provides a runner to run calls in the group, so it can
 * pass the context to the underlying RPC call where needed.
 */
trait Group[C, T] {
  def runner(): Runner[C, T]
}

trait ArrowGroup[C, T] extends Group[C, T] {
  def runner(): ArrowFutureRunner[C, T]
}

trait SeqGroup[K, V] extends ArrowGroup[K, V] {
  def runner(): SeqRunner[K, V] = new SeqRunner[K, V](this) {
    def run(keys: Seq[K]): Future[Seq[Try[V]]] = SeqGroup.this.run(keys)
    override def maxSize: Int = SeqGroup.this.maxSize
    override def maxConcurrency: Int = SeqGroup.this.maxConcurrency
  }
  protected def run(keys: Seq[K]): Future[Seq[Try[V]]]
  protected def maxSize: Int = Int.MaxValue
  protected def maxConcurrency: Int = Int.MaxValue
}

object SeqGroup {

  /**
   * Create a [[SeqGroup]] from the provided function.
   */
  def apply[K, V](f: Seq[K] => Future[Seq[Try[V]]]): SeqGroup[K, V] =
    new SeqGroup[K, V] {
      override def run(keys: Seq[K]): Future[Seq[Try[V]]] = f(keys)
    }

}

trait StitchSeqGroup[K, V] extends Group[K, V] {
  def runner(): StitchSeqRunner[K, V] = new StitchSeqRunner[K, V]() {
    def run(keys: Seq[K]): Stitch[Seq[V]] = StitchSeqGroup.this.run(keys)
  }
  protected def run(keys: Seq[K]): Stitch[Seq[V]]
}

trait MapGroup[K, V] extends ArrowGroup[K, V] {
  def runner(): MapRunner[K, V] = new MapRunner[K, V](this) {
    def run(keys: Seq[K]): Future[K => Try[V]] = MapGroup.this.run(keys)
    override def maxSize: Int = MapGroup.this.maxSize
    override def maxConcurrency: Int = MapGroup.this.maxConcurrency
  }
  protected def run(keys: Seq[K]): Future[K => Try[V]]
  protected def maxSize: Int = Int.MaxValue
  protected def maxConcurrency: Int = Int.MaxValue
}

object MapGroup {
  def apply[K, V](f: Seq[K] => Future[K => Try[V]]): MapGroup[K, V] =
    new MapGroup[K, V] {
      override def run(keys: Seq[K]): Future[K => Try[V]] = f(keys)
    }
}

trait BucketedSeqGroup[K, V, B] extends ArrowGroup[K, V] {
  def runner(): BucketedSeqRunner[K, V, B] = new BucketedSeqRunner[K, V, B](this) {
    def bucket(key: K): B = BucketedSeqGroup.this.bucket(key)
    def run(keys: Seq[K]): Future[Seq[Try[V]]] = BucketedSeqGroup.this.run(keys)
    override def maxSize: Int = BucketedSeqGroup.this.maxSize
    override def maxConcurrency: Int = BucketedSeqGroup.this.maxConcurrency
    protected def packingStrategy: BatchPackingStrategy = BucketedSeqGroup.this.packingStrategy
  }
  protected def bucket(key: K): B
  protected def run(keys: Seq[K]): Future[Seq[Try[V]]]
  protected def maxSize: Int = Int.MaxValue
  protected def maxConcurrency: Int = Int.MaxValue
  protected def packingStrategy: BatchPackingStrategy = SplitSmallBuckets
}

object BucketedSeqGroup {
  def apply[K, V, B](
    bucket: K => B
  )(
    f: Seq[K] => Future[Seq[Try[V]]]
  ): BucketedSeqGroup[K, V, B] =
    new BucketedSeqGroup[K, V, B] {
      override def run(keys: Seq[K]): Future[Seq[Try[V]]] = f(keys)
      override def bucket(key: K) = bucket(key)
    }
}

trait BucketedMapGroup[K, V, B] extends ArrowGroup[K, V] {
  def runner(): BucketedMapRunner[K, V, B] = new BucketedMapRunner[K, V, B](this) {
    def bucket(key: K): B = BucketedMapGroup.this.bucket(key)
    def run(keys: Seq[K]): Future[K => Try[V]] = BucketedMapGroup.this.run(keys)
    override def maxSize: Int = BucketedMapGroup.this.maxSize
    override def maxConcurrency: Int = BucketedMapGroup.this.maxConcurrency
    protected def packingStrategy: BatchPackingStrategy = BucketedMapGroup.this.packingStrategy
  }
  protected def bucket(key: K): B
  protected def run(keys: Seq[K]): Future[K => Try[V]]
  protected def maxSize: Int = Int.MaxValue
  protected def maxConcurrency: Int = Int.MaxValue
  protected def packingStrategy: BatchPackingStrategy = SplitSmallBuckets
}

object BucketedMapGroup {
  def apply[K, V, B](
    bucket: K => B
  )(
    f: Seq[K] => Future[K => Try[V]]
  ): BucketedMapGroup[K, V, B] =
    new BucketedMapGroup[K, V, B] {
      override def bucket(key: K) = bucket(key)
      override def run(keys: Seq[K]): Future[K => Try[V]] = f(keys)
    }
}
