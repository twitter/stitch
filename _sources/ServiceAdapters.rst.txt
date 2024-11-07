.. _service-adapters:

Service Adapters
################

.. note::

  Be sure you have followed the instructions in :ref:`setup` for importing dependencies before running examples.

A Stitch **Service Adapter** provides the detailed querying implementation that
represents a service in a Stitch query. Each service has an associated adaptor
which maps Stitch atomic queries onto the underlying service, and provides
convenience methods and batching.

.. _writing-service-adapter:

Writing a Service Adapter
*************************

Before writing a Service Adapter, you should be familiar with :ref:`calls`, :ref:`groups`, and :ref:`runners`.

For most services that use Stitch, the call to a backend is hidden inside of their
adapter’s code, so you never actually see it. Instead, adapters often have a method that,
when given arguments returns a Stitch of the results. In this example, `adapter` takes
in an argument and returns a Stitch of the results. For simplicity, let's say that the
service just adds 1 to the arg.

.. code-block:: scala

  // in the adapter
  private val g = new SeqGroup[Int, Int]{
  def run(keys: Seq[Int]): Future[Seq[Try[Int]]] =
    Future.value(keys.map(i => Return(i + 1)))
  }
  def adapter(arg: Int): Stitch[Int] = Stitch.call(arg, g)

  // adapter's user’s code
  val s = Stitch.value(0).flatMap(arg => adapter(arg))
  Await.result(Stitch.run(s)) // result: 1

.. image:: images/Adapter.png
  :alt: Graphs showing a Call and Group being encapsulated by an adapter

For some APIs, all Stitch calls can go into the same batch.
For others, the service-level call includes additional contextual information along with the key.
That context is included in the :ref:`Group <groups>`, so `calls` with the same contexts may be
batched together.

When writing a Service Adapter its important to encapsulate the `Group` and `.call`, see :ref:`groups`.

Next :ref:`stitch-concepts`
