.. _running:

Running
#######

Stitches must be run to execute the computation and get the result.
Running a Stitch returns a Future which will complete with the final
value of the Stitch. Exceptions that occur during the running of a
Stitch are encapsulated in the Stitch and can be handled in the same
way that Futures handle exceptions.

:ref:`Twitter Locals are also available when running a Stitch <twitter-locals>`.

.. _thread-safety:

Thread-safety
*************

During execution of a Stitch query, multiple underlying RPC calls may
run in parallel. When an underlying call returns, query simplification
(including calling callbacks) runs in a single thread with a lock on
the execution state. Each phase of simplification may occur in a
different thread (e.g. because a different Finagle thread handled the
RPC response).

Because the execution state is synchronized, it's safe to modify local
state (e.g. to accumulate a result into a buffer) inside a query
without extra locking. Moreover the results of one phase of
simplification are visible to other phases even if they happen in
different threads.

This guarantee applies only within a single query execution (i.e. a
call to `Stitch.run`); it's not safe to operate (without extra
locking) on state which is shared between multiple executions of the
same query or different queries.

**Caveat**: you probably don't want to use stateful operations in
most cases.

.. _never-re-runnable:

Never Re-Runnable
#################

It's not safe to re-run a Stitch query (besides the below exceptions).
While in practice, there may be occasions where no issues are seen,
it is inherently unsafe to rerun Stitches. The behavior is undefined
and should not be relied upon. `Stitch.const` (`Stitch.value` or `Stitch.exception`)
and `Stitch.synchronizedRef` are exceptions to this rule and are safe to re-run
but other Stitches are not considered safe to re-run.

All of the following are examples of rerunning:

- Directly rerunning

  .. code-block:: scala

    val s0:Stitch[T]
    Stitch.run(s0)
    Stitch.run(s0)

- Rerunning a Stitch which contains an already run Stitch

  .. code-block:: scala

    val s0:Stitch[T]
    val s1 = s0.ensure(sideEffect)
    Stitch.run(s0)
    Stitch.run(s1) // unsafe since it will rerun s0

- FlatMapping on an already run Stitch

  .. code-block:: scala

    val s0:Stitch[T]
    val s1:Stitch[T]
    val s2 = s0.flatMap(s1) // or Stitch.join(s0, s1)
    Stitch.run(s1)
    Stitch.run(s2) // unsafe since it will rerun s1

This is not an exhaustive list but it gives you an idea.
The key thing to remember is that no part of a Stitch should ever be passed
into more than one call to `Stitch.run`. However, it is safe to reuse a Stitch
as long as it is within the same `Stitch.run` call. This is covered in more
detail in :ref:`the section on Stitch.ref <refs>`.

.. code-block:: scala

  val s0 =
    Stitch.Unit.onSuccess(_ => println("this safely runs twice"))
  val s1 = s0.before(s0)
  Await.result(Stitch.run(s1))

This is safe since `Stitch.run` can correctly handle coming
across the same Stitch multiple times in the execution graph.

Why is re-running a Stitch unsafe?
Stitches have internal mutable state, that state is changed during running
without any locking or synchronization. This means that if a Stitch is run
concurrently you can experience unexpected behavior like missing data, code
that never executes, or other unwanted behavior. The 2 exceptions to this rule
are `Stitch.const` (`Stitch.value` or `Stitch.exception`) which is safe to re-run
since it's a constant and doesn't have mutable state and `Stitch.synchronizedRefs`
which are are safe to re-run but have the additional cost of extra
synchronization overhead. Since the structure of a Stitch (such as whether
it's a `Const` or `SynchronizedRef`) is opaque to the user (due to being private
classes) it's not possible to pattern match or otherwise assume an arbitrary
`Stitch[T]` is in one of these forms unless it was explicitly put into one of
these forms.

Next :ref:`efficient-queries`
