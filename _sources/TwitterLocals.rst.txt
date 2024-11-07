.. _twitter-locals:

Twitter Locals
##############

`Locals <https://twitter.github.io/util/docs/com/twitter/util/Local.html>`_ and `Contexts <https://twitter.github.io/finagle/guide/Contexts.html>`_
are available to use in Stitch but there is a catch, the locals that are available in the Stitch are the ones that are
defined when Stitch.run is called or within a `Stitch.let` scope. This can have some unexpected results.

In these examples, the `local` is not defined because Stitches are :ref:`evaluated lazily <lazy>` so
even though the `local` may be available when the Stitch is created, the `local` is not available when it's
actually evaluated.

.. code-block:: scala

  val local = new com.twitter.util.Local[Int]()

  val s = local.let(1)(Stitch.Unit.map(_ =>
    local())
  )
  Await.result(Stitch.run(s)) // result: None

  val s = Stitch.Unit.flatMap(_ =>
    local.let(1)(Stitch.Unit.map(_ => local()))
  )
  Await.result(Stitch.run(s)) // result: None

In the below examples, the `local` is defined because it wraps the `Stitch.run` call,
making them available for the life of the Stitch.

.. code-block:: scala

  val local = new com.twitter.util.Local[Int]()

  val s = Stitch.Unit.map(_ => local())
  Await.result(local.let(1)(
    Stitch.run(s)
  )) // result: Some(1)

In the below examples, the `local` is defined because it wraps the
creation of an *eagerly* evaluated :ref:`const`.

.. code-block:: scala

  val local = new com.twitter.util.Local[Int]()

  val s = local.let(1)(Stitch.value(local()))
  Await.result(Stitch.run(s)) // result: Some(1)

  val s = Stitch.Unit.flatMap(_ =>
    local.let(1)(Stitch.value(local()))
  )
  Await.result(Stitch.run(s)) // result: Some(1)

In the below examples, the `local` is defined because `Stitch.let` wraps
the Stitch.

.. code-block:: scala

  val local = new com.twitter.util.Local[Int]()

  val s = Stitch.let(local)(1)(Stitch.Unit.map(_ =>
    local())
  )
  Await.result(Stitch.run(s)) // result: Some(1)

  val s = Stitch.Unit.flatMap(_ =>
    Stitch.let(local)(1)(Stitch.Unit.map(_ => local()))
  )
  Await.result(Stitch.run(s)) // result: Some(1)

  val s = Stitch.let(local)(2)(Stitch.Unit.map(_ => local()))
  Await.result(local.let(1)(
    Stitch.run(s)
  )) // result: Some(2)

In the above examples we use Locals within Stitch code, Locals which are available in your Stitch code will also
be available in `callFuture`\s (non-batched calls), however Locals set with `Stitch.let` will not be available in
`Group`\s (invoked with `Stitch.call`). This is because the batching that occurs with a `Group` doesn't really make
sense unless all the elements of the batch have the same Locals (`local.let` around the `Stitch.run` call will, however,
be defined in the Group), it's rarely correct to split batches based on all Locals. Instead if you do care about
specific Locals in your Group, you can wire it into the Group instance or into the input arguments for the Group.

.. code-block:: scala

  // Group with localValue in the constructor, will batched based on `localValue`
  case class GroupWithLocalInConstructor(localValue: Option[Int]) extends SeqGroup[Int, Option[Int]]{ ... }

  s.flatMap(i => Stitch.call(i, GroupWithLocalInConstructor(local.apply())))

  // OR

  // Group with localValue in the input, different `localValue`s will be in the same batch
  case class IntWithContext(i: Int, localValue: Option[Int])
  val groupWithLocalInInputArg = new SeqGroup[IntWithContext, Option[Int]] { ... }

  s.flatMap(i => Stitch.call(IntWithContext(i, local.apply()), groupWithLocalInInputArg))

Finagle Context and TwitterContext is passed around with `Local`\s. To let-scope these you need to make a `Letter`,
here's an example of a `Letter` for a Finagle local context:

.. code-block:: scala

  object MyContextKeyLetter extends Letter[Int] {
            override def let[S](value: Int)(s: => S): S = Contexts.local.let(MyContextKey, value)(s)
  }

  // OR generically

  case class ContextLetter[L](contextKey: Contexts.local.Key[L]) extends Letter[L] {
    override def let[S](value: L)(s: => S): S = Contexts.local.let(contextKey, value)(s)
  }

Locals behave the same way with :ref:`arrows`, the scoping of the Locals must be around
the `Stitch.run` method call or by using `Arrow.let` and not around the creation of the Stitch or Arrow itself,
except for :ref:`const` Stitches and Arrows which are evaluated eagerly.

Next :ref:`recursion`
