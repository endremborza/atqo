Scheduler
=========

.. currentmodule:: atqo

.. autoclass:: Scheduler
   :show-inheritance:

   .. rubric:: Attributes Summary

   .. autosummary::

      ~Scheduler.is_empty
      ~Scheduler.is_idle
      ~Scheduler.queued_task_count

   .. rubric:: Methods Summary

   .. autosummary::

      ~Scheduler.cleanup
      ~Scheduler.iter_until_n_tasks_remain
      ~Scheduler.join
      ~Scheduler.process
      ~Scheduler.refill_task_queue

   .. rubric:: Attributes Documentation

   .. autoattribute:: is_empty
   .. autoattribute:: is_idle
   .. autoattribute:: queued_task_count

   .. rubric:: Methods Documentation

   .. automethod:: cleanup
   .. automethod:: iter_until_n_tasks_remain
   .. automethod:: join
   .. automethod:: process
   .. automethod:: refill_task_queue
