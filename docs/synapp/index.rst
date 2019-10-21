
============================
Kronos synthetic application
============================

Description
===========

The Kronos synthetic application is designed to emulate a workload based on
invariants that can be extracted from real profiling data. The workload consists
of a series of configurable kernels representing memory access, floating-point
operations, and I/O.

Running the synthetic application
=================================

The synthetic application can be run stand-alone::

   <parallel-run> kronos-synapp <input-file>

where ``<parallel-run>`` is a parallel launcher (e.g. ``mpirun``) with
appropriate arguments.

It is designed to be integrated in a Kronos workflow via a KSchedule file, that
can be run as follows::

   kronos-executor -c <config-file> <kschedule>

In that case, Kronos will take care of submitting the jobs inside the schedule
according to their dependencies.

Reference documentation
=======================

.. toctree::
   :maxdepth: 1

   kernels
   input
