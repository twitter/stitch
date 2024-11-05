.. _recursion:

Recursion
#########

Regular Stitch execution isn't stack safe. Recursive computations must use `Stitch.recursive`
or `Arrow.recursive` to avoid stack overflows. Examples:

.. code-block:: scala

  def fact(i: Int): Stitch[Int] =
    Stitch.recursive {
      if (i <= 1) Stitch.value(1)
      else fact(i - 1).map(_ * i)
    }

.. code-block:: scala

  val fact =
    Arrow.recursive[Int, Int] { self =>
      Arrow.choose[Int, Int](
        Arrow.Choice.when(_ <= 1, Arrow.value(1)),
        Arrow.Choice.otherwise {
          Arrow
            .zipWithArg(
              Arrow
                .map[Int, Int](_ - 1)
                .andThen(self)
            )
            .map {
              case (a, b) => a * b
            }
        }
      )
    }

The execution of recursive computations uses on-stack recursion in batches to avoid the overhead
of unfolding the thread stack on each recursion. The number of recursions allowed on the stack 
is limited by the system property `stitch.maxRecursionDepth`, which has a default of `200`. The 
execution unfolds the stack only after the number of recursions reach this limit.

Applications might need to change `stitch.maxRecursionDepth` to a lower value in case they see
stack overflows with recursive computations. Increasing the value might improve performance if
it can be done safely.

Next :ref:`advanced`
