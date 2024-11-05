.. _setup:

Setup
#####

Most of the examples contain runnable code, however to simplify the example code imports have been omitted.

For simplicity, you can run the following imports in a repl or add them to your scala file before running examples.

.. code-block:: scala

    import com.twitter.stitch.{Arrow, Group, MapGroup, SeqGroup, Stitch}
    import com.twitter.stitch.cache.StitchCache
    import com.twitter.util.{
      Await,
      Duration,
      Future,
      FuturePool,
      JavaTimer,
      Return,
      Throw,
      Timer,
      Try
    }
    import java.util.concurrent.atomic.AtomicInteger
    import java.util.concurrent.ConcurrentHashMap

Next :ref:`stitch-vs-future`
